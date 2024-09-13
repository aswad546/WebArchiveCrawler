import os
import re
import json
import time
import requests
import psycopg2
from tqdm import tqdm

# Constants
WAYBACK_API_URL = "http://archive.org/wayback/available"
LOG_FILE = 'secondlog.txt'
DB_PARAMS = {
    'dbname': 'postgres',  # The default PostgreSQL database
    'user': 'postgres',
    'password': 'postgres',
    'host': os.getenv('DB_HOST', 'localhost'),  # Use the DB_HOST environment variable or default to localhost
    'port': '5432'
}

TOP_URL_FILE = 'fingerprinting_domains.json'  # This is the file with the script_url to top_url mapping.

def log_message(message):
    """
    Writes a message to the log file.
    """
    with open(LOG_FILE, 'a') as log_file:
        log_file.write(message + '\n')

def create_database_if_not_exists(retries=5, delay=5):
    """
    Connect to the PostgreSQL database and create the target database (fp_inspector) if it doesn't exist.
    Retries the connection if PostgreSQL is not ready.
    """
    for attempt in range(retries):
        try:
            conn = psycopg2.connect(**DB_PARAMS)
            conn.autocommit = True  # Enable autocommit to allow database creation
            with conn.cursor() as cur:
                # Check if the fp_inspector database exists
                cur.execute(f"SELECT 1 FROM pg_database WHERE datname = 'fp_inspector';")
                exists = cur.fetchone()
                if not exists:
                    log_message(f"Database 'fp_inspector' does not exist. Creating...")
                    cur.execute("CREATE DATABASE fp_inspector;")
                else:
                    log_message(f"Database 'fp_inspector' already exists.")
            conn.close()
            return
        except psycopg2.OperationalError as e:
            log_message(f"Attempt {attempt + 1}: Could not connect to PostgreSQL, retrying in {delay} seconds... Error: {e}")
            time.sleep(delay)
    
    log_message("Failed to connect to PostgreSQL after multiple attempts.")
    raise psycopg2.OperationalError("Failed to connect to PostgreSQL.")

def create_database_and_table(conn):
    """
    Ensures the 'fp_scripts' table exists in the PostgreSQL database.
    """
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS fp_scripts (
                    id SERIAL PRIMARY KEY,
                    script_url TEXT UNIQUE,  -- Ensure unique script_url
                    top_url TEXT,            -- Add the top_url column
                    script_code TEXT,
                    archived TEXT,           -- Yes or No
                    wayback_url TEXT          -- URL from Wayback Machine or empty if live
                );
            """)
            conn.commit()
    except Exception as e:
        log_message(f"Error creating the table: {e}")

def insert_into_db(conn, script_url, top_url, script_code, archived, wayback_url):
    """
    Inserts the JavaScript data into the PostgreSQL database.
    """
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO fp_scripts (script_url, top_url, script_code, archived, wayback_url)
                VALUES (%s, %s, %s, %s, %s) ON CONFLICT (script_url) DO NOTHING;
            """, (script_url, top_url, script_code, archived, wayback_url))
            conn.commit()
    except Exception as e:
        log_message(f"Error inserting data into the database for {script_url}: {e}")

def query_wayback(script_url, year):
    """
    Queries the Wayback Machine API to get the closest snapshot of the URL for the specified year.
    """
    timestamp = f"{year}0101000000"  # Start of the year
    params = {
        "url": script_url,
        "timestamp": timestamp
    }

    try:
        response = requests.get(WAYBACK_API_URL, params=params, timeout=15)
        if response.status_code == 200:
            data = response.json()
            if 'archived_snapshots' in data and 'closest' in data['archived_snapshots']:
                snapshot = data['archived_snapshots']['closest']
                if snapshot['available']:
                    return snapshot['url'], None
        return None, f"No snapshot found for {year}."
    except requests.RequestException as e:
        return None, f"Error querying Wayback Machine: {e}"

def fetch_javascript(script_url):
    """
    Fetches the JavaScript content from the live URL.
    """
    try:
        response = requests.get(script_url, timeout=15)
        if response.status_code == 200:
            return response.text, "No", ""  # No archive, live fetch
        else:
            return None, None, None
    except requests.RequestException as e:
        return None, None, f"Error fetching live script: {e}"

def fetch_wayback_javascript(script_url, years=[2020, 2019]):
    """
    Tries to fetch JavaScript from the Wayback Machine for the specified years.
    If no snapshot is found, falls back to live fetching.
    """
    for year in years:
        wayback_url, error = query_wayback(script_url, year)
        if wayback_url:
            try:
                response = requests.get(wayback_url, timeout=15)
                if response.status_code == 200:
                    return response.text, "Yes", wayback_url  # Archived
            except requests.RequestException as e:
                log_message(f"Error fetching from Wayback Machine for {script_url}: {e}")
                continue
    return None, None, None

def extract_urls_from_log():
    """
    Extracts all URLs from the log file where fetching the script failed.
    """
    pattern = re.compile(r"Error fetching script from (https?://[^\s]+):")
    urls = []

    with open('log.txt', 'r') as log_file:
        for line in log_file:
            match = pattern.search(line)
            if match:
                urls.append(match.group(1))

    return urls

def load_top_url_mapping(file_path):
    """
    Loads the top_url mapping from a JSON file.
    The file should have a structure where script_url is mapped to top_url.
    """
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        log_message(f"Error loading top_url mapping file {file_path}: {e}")
        return {}

def find_top_url(script_url, top_url_mapping):
    """
    Finds the top_url corresponding to a script_url from the top_url_mapping.
    If there are multiple, it picks the first occurrence.
    """
    for scripts in top_url_mapping.values():
        for script in scripts:
            if script.get('script_url') == script_url:
                return script.get('top_url')
    return None

def process_urls(conn, urls, top_url_mapping):
    """
    Process the list of URLs: Try to fetch from the Wayback Machine or fall back to live fetching.
    Inserts any successfully fetched script into the database, along with the top_url.
    Logs an error for any script that could not be fetched.
    """
    progress_bar = tqdm(total=len(urls))

    for url in urls:
        # Find the corresponding top_url for the script_url
        top_url = find_top_url(url, top_url_mapping)

        # Try to fetch from Wayback Machine (2020, 2019) or live URL
        script_code, archived, wayback_url = fetch_wayback_javascript(url)

        if not script_code:
            # If not found in Wayback, fetch from live URL
            script_code, archived, wayback_url = fetch_javascript(url)

        if script_code:
            # Insert into the database
            insert_into_db(conn, url, top_url, script_code, archived, wayback_url)
        else:
            log_message(f"Script not found for URL: {url}")
        
        progress_bar.update(1)
        time.sleep(10)

    progress_bar.close()

def main():
    # Step 1: Connect to the PostgreSQL database and create it if necessary
    create_database_if_not_exists()

    # Step 2: Connect to the fp_inspector database
    conn = psycopg2.connect(dbname='fp_inspector', user='postgres', password='postgres', host=os.getenv('DB_HOST', 'localhost'), port='5432')

    # Step 3: Create the necessary table in the database
    create_database_and_table(conn)

    # Step 4: Load the top_url mapping from the JSON file
    top_url_mapping = load_top_url_mapping(TOP_URL_FILE)

    # Step 5: Extract URLs from the log file
    urls = extract_urls_from_log()

    # Step 6: Process the extracted URLs and insert the valid scripts into the database
    process_urls(conn, urls, top_url_mapping)

    # Step 7: Close the database connection
    conn.close()

if __name__ == "__main__":
    # Clear the log file at the start of each run
    with open(LOG_FILE, 'w') as log_file:
        log_file.write("Log of errors and issues encountered:\n")

    time.sleep(120)  # Giving time to ensure that the DB is up and running
    main()
