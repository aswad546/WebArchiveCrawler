import json
import requests
import psycopg2
from tqdm import tqdm

# Database connection parameters
DB_PARAMS = {
    'dbname': 'fp_inspector',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': 5432
}

LOG_FILE = 'log.txt'

def create_database_if_not_exists():
    """
    Connect to the default postgres database and create the target database if it doesn't exist.
    """
    conn = psycopg2.connect(dbname='postgres', user='postgres', password='postgres', host='localhost', port=5432)
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
    Drops the 'js_scripts' table if it exists and creates a new one in the PostgreSQL database.
    Ensures the script_url column is unique.
    """
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS js_scripts;")
        cur.execute("""
            CREATE TABLE js_scripts (
                id SERIAL PRIMARY KEY,
                script_url TEXT UNIQUE,  -- Ensure unique script_url
                top_url TEXT,
                script_code TEXT
            );
        """)
        conn.commit()

def script_url_exists(conn, script_url):
    """
    Checks if a script_url already exists in the database.
    Returns True if it exists, otherwise False.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM js_scripts WHERE script_url = %s;", (script_url,))
        return cur.fetchone() is not None

def insert_into_db(conn, script_url, top_url, script_code):
    """
    Inserts the JavaScript data into the PostgreSQL database.
    """
    # Remove NUL characters from script_code
    script_code = script_code.replace('\x00', '')
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO js_scripts (script_url, top_url, script_code)
            VALUES (%s, %s, %s);
        """, (script_url, top_url, script_code))
        conn.commit()

def fetch_javascript(script_url):
    """
    Fetches the JavaScript content from the provided URL.
    """
    try:
        response = requests.get(script_url, timeout=10)
        if response.status_code == 200:
            return response.text, None
        else:
            return None, f"Error {response.status_code}: {response.reason}"
    except requests.RequestException as e:
        return None, f"Request failed: {e}"

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

    progress_bar = tqdm(total=total_scripts)  # Initialize progress bar

    for key, scripts in json_data.items():
        for script in scripts:
            script_url = script['script_url']
            top_url = script['top_url']

            # Check if the script_url already exists in the database
            if script_url_exists(conn, script_url):
                skipped_duplicates += 1
                progress_bar.update(1)
                continue

            # Fetch JavaScript content from the script URL
            script_code, error = fetch_javascript(script_url)

            if script_code:
                # Save to the database
                insert_into_db(conn, script_url, top_url, script_code)
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
    with open('/home/vagrant/WebArchiveCrawler/datasets/FP-Inspector/fingerprinting_domains.json', 'r') as file:
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
