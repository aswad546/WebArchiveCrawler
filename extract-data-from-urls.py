import os
import json
import requests
import psycopg2
from psycopg2 import sql
from tqdm import tqdm

# Database connection parameters for the default postgres database
DB_PARAMS_DEFAULT = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': 5432
}

# Target database name
TARGET_DB_NAME = 'fp_radar'

WAYBACK_API_URL = "http://archive.org/wayback/available"

def create_database_and_wayback_table(conn):
    """
    Drops the 'wayback_fp_data' table if it exists and creates a new one in the PostgreSQL database.
    """
    with conn.cursor() as cur:
        cur.execute("""
            DROP TABLE IF EXISTS wayback_fp_data;
            CREATE TABLE wayback_fp_data (
                id SERIAL PRIMARY KEY,
                rank INT,
                url TEXT,
                wayback_url TEXT,
                code TEXT,
                fp_type TEXT,
                archive TEXT,
                wayback_year INT  -- New column to store the year where the script was found
            );
        """)
        conn.commit()

def fetch_wayback_url(url, year):
    """
    Queries the Wayback API for a snapshot of the given URL in the specified year.
    Returns the closest available snapshot URL if found, otherwise returns None.
    """
    # Create a timestamp for the specific year (start of the year)
    timestamp = f"{year}0101000000"
    params = {
        'url': url,
        'timestamp': timestamp
    }

    try:
        response = requests.get(WAYBACK_API_URL, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data.get('archived_snapshots') and 'closest' in data['archived_snapshots']:
                snapshot = data['archived_snapshots']['closest']
                return snapshot['url'] if snapshot['available'] else None
        return None
    except requests.RequestException as e:
        print(f"Error fetching Wayback snapshot for {url}: {e}")
        return None

def extract_data_from_json(json_content):
    """
    Extracts relevant data from the JSON content.
    """
    extracted_data = []
    for rank, data in json_content.items():
        if "topDomain" not in data:
            continue  # Non-fingerprinting scripts do not have topDomains
        top_domain_list = data["topDomain"]
        years = data.get("year", [])
        if not top_domain_list:
            continue  # Skip if 'topDomain' list is empty
        for top_domain in top_domain_list:
            extracted_data.append((rank, top_domain, years))
    return extracted_data

    
def process_json_files(directory):
    """
    Processes all JSON files in the specified directory structure and extracts relevant data,
    including the parent directory name as 'api_type'.
    """
    all_extracted_data = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".json"):
                file_path = os.path.join(root, file)
                parent_directory = os.path.basename(os.path.dirname(file_path)).lower()
                with open(file_path, "r") as json_file:
                    try:
                        json_content = json.load(json_file)
                        extracted_data = extract_data_from_json(json_content)
                        for data in extracted_data:
                            all_extracted_data.append((*data, parent_directory))
                    except json.JSONDecodeError as e:
                        print(f"Error decoding JSON from file {file_path}: {e}")
    return all_extracted_data


def fetch_javascript(url):
    """
    Fetches the content from the provided URL.
    Returns the content if successful, else returns the error message.
    """
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            # Assume the content is JavaScript regardless of Content-Type
            return response.text, None  # Return the content, no error
        else:
            return None, f"Error {response.status_code}: {response.reason}"
    except requests.RequestException as e:
        return None, f"Request failed: {e}"

def insert_into_wayback_db(conn, rank, url, wayback_url, code, fp_type, archive, wayback_year=None):
    """
    Inserts the Wayback JavaScript data into the PostgreSQL database.
    """
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO wayback_fp_data (rank, url, wayback_url, code, fp_type, archive, wayback_year) 
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """, (rank, url, wayback_url, code, fp_type, archive, wayback_year))
        conn.commit()

def process_wayback_data(conn, extracted_data):
    """
    Processes the extracted data by querying the Wayback Machine for each URL's historical snapshot,
    fetching the JavaScript code from the snapshot, and saving it to the database.
    """
    failure_log = {}
    failure_log_filename = "wayback_failure_log.json"
    batch_size = 1  # Save to file after every 20 failures
    failure_log_batch = []

    # Initialize tqdm progress bar
    with tqdm(total=len(extracted_data)) as progress_bar:
        for data in extracted_data:
            rank, url, years, api_type = data

            # Process the years in descending order to get the closest snapshot
            wayback_url = None
            archive = "No"  # Default to "No" for live URLs
            wayback_year = None  # New variable to capture the year if the Wayback snapshot is used

            for year in sorted(years, reverse=True):
                wayback_url = fetch_wayback_url(url, year)
                if wayback_url:
                    archive = "Yes"  # Mark as "Yes" if we find a Wayback snapshot
                    wayback_year = year  # Save the year when the snapshot was found
                    break  # Stop when we find the first snapshot

            if wayback_url:
                # Fetch JavaScript from the Wayback snapshot
                code, error = fetch_javascript(wayback_url)
                error = "Wayback_fetch_error:" + error if error else None
            else:
                # No snapshot found, attempt to fetch directly from the live URL
                code, error = fetch_javascript(url)
                wayback_url = ''  # Mark the live URL as the URL fetched from
                error = "Regular_fetch_error:" + error if error else None

            if code:
                # Insert into the PostgreSQL database, including the year if available
                insert_into_wayback_db(conn, rank, url, wayback_url, code, api_type, archive, wayback_year)
            else:
                # Log failure if no JavaScript was fetched
                failure_log_batch.append({url: error})

            # Save failure log in batches
            if len(failure_log_batch) >= batch_size:
                failure_log.update({url: error for entry in failure_log_batch for url, error in entry.items()})
                save_failure_log(failure_log, failure_log_filename)
                failure_log_batch.clear()

            # Update the progress bar
            progress_bar.update(1)

    # Save any remaining failures
    if failure_log_batch:
        failure_log.update({url: error for entry in failure_log_batch for url, error in entry.items()})
        save_failure_log(failure_log, failure_log_filename)

def save_failure_log(failure_log, filename):
    """
    Saves the failure log to a file for later inspection.
    """
    with open(filename, 'w') as f:
        json.dump(failure_log, f, indent=4)

def main():
    # First, connect to the default postgres database to check for or create the target database
    conn_default = psycopg2.connect(**DB_PARAMS_DEFAULT)
    conn_default.autocommit = True
    with conn_default.cursor() as cur:
        cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{TARGET_DB_NAME}';")
        exists = cur.fetchone()
        if not exists:
            cur.execute(sql.SQL(f"CREATE DATABASE {TARGET_DB_NAME};"))
    conn_default.close()

    # Now connect to the 'fp_radar' database and create the wayback_fp_data table
    DB_PARAMS_TARGET = {
        'dbname': TARGET_DB_NAME,
        'user': 'postgres',
        'password': 'postgres',
        'host': 'localhost',
        'port': 5432
    }
    conn = psycopg2.connect(**DB_PARAMS_TARGET)
    create_database_and_wayback_table(conn)

    # Extract data (simulated here, you should load the actual dataset)
    extracted_data = process_json_files("datasets/FP-Radar")

    # Process wayback data and save it into the database
    process_wayback_data(conn, extracted_data)

    # Close the database connection
    conn.close()

if __name__ == "__main__":
    main()
