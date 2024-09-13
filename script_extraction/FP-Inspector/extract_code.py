import json
import sys
from time import sleep
import requests
import psycopg2
from tqdm import tqdm

# Database connection parameters
DB_PARAMS = {
    'dbname': 'fp_inspector',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'my_postgres',
    'port': 5432
}

LOG_FILE = 'log.txt'
WAYBACK_API_URL = "http://archive.org/wayback/available"

def create_database_if_not_exists():
    """
    Connect to the default postgres database and create the target database if it doesn't exist.
    """
    conn = psycopg2.connect(dbname='postgres', user='postgres', password='postgres', host='my_postgres', port=5432)
    conn.autocommit = True  # Enable autocommit to allow database creation
    with conn.cursor() as cur:
        # Check if the database exists
        cur.execute(f"SELECT 1 FROM pg_database WHERE datname = 'fp_inspector';")
        exists = cur.fetchone()
        if not exists:
            log_message(f"Database 'fp_inspector' does not exist. Creating...")
            cur.execute("CREATE DATABASE fp_inspector;")
        else:
            log_message(f"Database 'fp_inspector' already exists.")
    conn.close()

def create_database_and_table(conn):
    """
    Drops the 'fp_scripts' table if it exists and creates a new one in the PostgreSQL database.
    Ensures the script_url column is unique.
    """
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS fp_scripts;")
        cur.execute("""
            CREATE TABLE fp_scripts (
                id SERIAL PRIMARY KEY,
                script_url TEXT UNIQUE,  -- Ensure unique script_url
                top_url TEXT,
                script_code TEXT,
                archived TEXT,          -- Yes or No
                wayback_url TEXT         -- URL from Wayback Machine or empty
            );
        """)
        conn.commit()

def script_url_exists(conn, script_url):
    """
    Checks if a script_url already exists in the database.
    Returns True if it exists, otherwise False.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM fp_scripts WHERE script_url = %s;", (script_url,))
        return cur.fetchone() is not None

def insert_into_db(conn, script_url, top_url, script_code, archived, wayback_url):
    """
    Inserts the JavaScript data into the PostgreSQL database, including archive status and wayback URL.
    """
    # Remove NUL characters from script_code
    script_code = script_code.replace('\x00', '')
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO fp_scripts (script_url, top_url, script_code, archived, wayback_url)
            VALUES (%s, %s, %s, %s, %s);
        """, (script_url, top_url, script_code, archived, wayback_url))
        conn.commit()

def query_wayback_for_snapshot(script_url):
    """
    Queries the Wayback Machine API to get the closest snapshot of the URL for July 2021.
    """
    timestamp = "2021" # Year of commits to github
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
        return None, "No snapshot found for July 2021."
    except requests.RequestException as e:
        return None, f"Error querying Wayback Machine: {e}"

def fetch_javascript(script_url):
    """
    Fetches the JavaScript content from the Wayback Machine's archived version of the script_url in July 2021.
    If not available, attempts to fetch the script directly from the live URL.
    """
    wayback_url, error = query_wayback_for_snapshot(script_url)
    
    if wayback_url:
        try:
            response = requests.get(wayback_url, timeout=15)
            if response.status_code == 200:
                return response.text, "Yes", wayback_url, None  # Archived: Yes
            else:
                return None, None, None, f"Error {response.status_code}: {response.reason}"
        except requests.RequestException as e:
            return None, None, None, f"Request failed: {e}"

    # If Wayback Machine snapshot not found, try to fetch directly from the live URL
    try:
        response = requests.get(script_url, timeout=10)
        if response.status_code == 200:
            return response.text, "No", "", None  # Archived: No
        else:
            return None, None, None, f"Error {response.status_code}: {response.reason}"
        
    except requests.RequestException as e:
        return None, None, None, f"Request failed: {e}"

def log_message(message):
    """
    Writes a message to the log file (no console output).
    """
    with open(LOG_FILE, 'a') as log_file:
        log_file.write(message + '\n')

def check_for_duplicates(json_data):
    """
    Checks for duplicate script URLs in the provided JSON data.
    Returns a new dictionary without duplicates. No logging for duplicates.
    """
    seen_urls = set()
    cleaned_data = {}

    for key, scripts in json_data.items():
        cleaned_scripts = []
        for script in scripts:
            script_url = script['script_url']

            if script_url not in seen_urls:
                seen_urls.add(script_url)
                cleaned_scripts.append(script)

        cleaned_data[key] = cleaned_scripts

    return cleaned_data

def process_json_data(json_data, conn):
    """
    Process the JSON data to fetch JavaScript from the 'script_url' and save it to the database.
    Logs errors for failed URLs and provides summary statistics.
    Skips duplicate script URLs.
    """
    total_scripts = sum(len(v) for v in json_data.values())  # Total number of scripts
    successful_scripts = 0
    failed_scripts = 0
    skipped_duplicates = 0

    progress_bar = tqdm(total=total_scripts, file=sys.stdout)  # Initialize progress bar

    for key, scripts in json_data.items():
        for script in scripts:
            sleep(10)
            script_url = script['script_url']
            top_url = script['top_url']

            # Check if the script_url already exists in the database
            if script_url_exists(conn, script_url):
                skipped_duplicates += 1
                progress_bar.update(1)
                continue

            # Fetch JavaScript content (either from Wayback or live URL)
            script_code, archived, wayback_url, error = fetch_javascript(script_url)

            if script_code:
                # Save to the database
                insert_into_db(conn, script_url, top_url, script_code, archived, wayback_url)
                successful_scripts += 1
            else:
                # Log error silently (no console output)
                log_message(f"Error fetching script from {script_url}: {error}")
                failed_scripts += 1

            # Update the progress bar
            progress_bar.update(1)

    progress_bar.close()

    # Summary statistics (only logged, not printed)
    log_message(f"\nTotal scripts processed: {total_scripts}")
    log_message(f"Successfully fetched: {successful_scripts}")
    log_message(f"Failed to fetch: {failed_scripts}")
    log_message(f"Skipped duplicates in database: {skipped_duplicates}")

def main():
    # Step 1: Ensure the target database exists
    create_database_if_not_exists()

    # Step 2: Connect to the target database and set up the table
    conn = psycopg2.connect(**DB_PARAMS)
    create_database_and_table(conn)

    # Step 3: Load JSON data from file
    with open('fingerprinting_domains.json', 'r') as file:
        json_data = json.load(file)

    # Step 4: Check for duplicate script URLs in the JSON file (no logging for duplicates)
    json_data = check_for_duplicates(json_data)

    # Step 5: Process the cleaned JSON data and save the scripts to the database
    process_json_data(json_data, conn)

    # Step 6: Close the database connection
    conn.close()

if __name__ == "__main__":
    # Clear the log file at the start of each run
    with open(LOG_FILE, 'w') as log_file:
        log_file.write("Log of errors and issues encountered:\n")

    main()
