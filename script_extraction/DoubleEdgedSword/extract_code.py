import os
import time
import requests
import psycopg2
from tqdm import tqdm

# Constants
WAYBACK_API_URL = "http://archive.org/wayback/available"
LOG_FILE = 'wayback_errors.txt'
DB_PARAMS = {
    'dbname': 'doubleedgesword',
    'user': 'postgres',
    'password': 'postgres',
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': '5432'
}

def log_message(message):
    """
    Writes a message to the log file.
    """
    with open(LOG_FILE, 'a') as log_file:
        log_file.write(message + '\n')

def create_result_table_if_not_exists(conn):
    """
    Ensures the 'wayback_fp_data' table exists in the PostgreSQL database.
    """
    try:
        with conn.cursor() as cur:
            cur.execute("""
                DROP TABLE IF EXISTS wayback_fp_data
                CREATE TABLE IF NOT EXISTS wayback_fp_data (
                    id SERIAL PRIMARY KEY,
                    script_url TEXT UNIQUE,
                    wayback_url TEXT,
                    archived TEXT,
                    code TEXT
                );
            """)
            conn.commit()
    except Exception as e:
        log_message(f"Error creating wayback_fp_data table: {e}")

def insert_into_results(conn, script_url, wayback_url, archived, code):
    """
    Inserts the JavaScript data into the 'wayback_fp_data' table.
    """
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO wayback_fp_data (script_url, wayback_url, archived, code)
                VALUES (%s, %s, %s, %s) ON CONFLICT (script_url) DO NOTHING;
            """, (script_url, wayback_url, archived, code))
            conn.commit()
    except Exception as e:
        log_message(f"Error inserting data for {script_url}: {e}")

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
        response = requests.get(script_url, timeout=10)
        if response.status_code == 200:
            return response.text, None
        else:
            return None, f"Error {response.status_code}: {response.reason}"
    except requests.RequestException as e:
        return None, f"Regular error fetching live script: {e}"

def fetch_wayback_javascript(script_url, years=[2023, 2022]):
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
                log_message(f"Wayback error fetching {script_url} for {year}: {e}")
                continue
    return None, None, None

def already_fetched(conn, script_url):
    """
    Checks if the script_url has already been fetched and stored in wayback_fp_data.
    """
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM wayback_fp_data WHERE script_url = %s;", (script_url,))
            return cur.fetchone() is not None
    except Exception as e:
        log_message(f"Error checking existing data for {script_url}: {e}")
        return False

def process_scripts(conn):
    """
    Fetch and process each script URL from the 'fingerprinters' table.
    """
    progress_bar = None
    try:
        with conn.cursor() as cur:
            # Get all script URLs from the fingerprinters table
            cur.execute("SELECT script_url FROM fingerprinters;")
            script_urls = cur.fetchall()

            progress_bar = tqdm(total=len(script_urls))  # Progress bar
            for script_url_tuple in script_urls:
                script_url = script_url_tuple[0]

                # Skip if already fetched
                if already_fetched(conn, script_url):
                    progress_bar.update(1)
                    continue

                # Try Wayback first for 2023, then 2022
                script_code, archived, wayback_url = fetch_wayback_javascript(script_url)

                if not script_code:
                    # If not found on Wayback, try fetching from live URL
                    script_code, error = fetch_javascript(script_url)
                    archived = "No"
                    wayback_url = ""

                    if not script_code:
                        log_message(f"Script not found for URL: {script_url}. {error}")
                        progress_bar.update(1)
                        continue  # Skip to next

                # Insert fetched script into the result table
                insert_into_results(conn, script_url, wayback_url, archived, script_code)

                progress_bar.update(1)
    except Exception as e:
        log_message(f"Error processing scripts: {e}")
    finally:
        if progress_bar:
            progress_bar.close()

def main():
    # Step 1: Connect to the PostgreSQL database
    try:
        conn = psycopg2.connect(**DB_PARAMS)

        # Step 2: Create the wayback_fp_data table if it doesn't exist
        create_result_table_if_not_exists(conn)

        # Step 3: Process the scripts from the fingerprinters table
        process_scripts(conn)

    except Exception as e:
        log_message(f"Error connecting to the database: {e}")
    finally:
        # Step 4: Close the database connection
        if conn:
            conn.close()

if __name__ == "__main__":
    # Clear the log file at the start of each run
    with open(LOG_FILE, 'w') as log_file:
        log_file.write("Wayback fetch log:\n")

    main()
