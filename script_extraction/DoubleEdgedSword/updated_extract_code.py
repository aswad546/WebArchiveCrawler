import os
import time
import requests
import pandas as pd
import gc  # For garbage collection
from tqdm import tqdm
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy import inspect
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# Constants
WAYBACK_API_URL = "http://archive.org/wayback/available"
LOG_FILE = 'wayback_errors.txt'
DB_NAME = 'doubleedgesword'
DB_PARAMS = {
    'dbname': DB_NAME,
    'user': 'postgres',
    'password': 'postgres',
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': '5432'
}

# Create a persistent session with retry logic for network requests
session = requests.Session()
retry = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry)
session.mount("http://", adapter)
session.mount("https://", adapter)

def log_message(message):
    with open(LOG_FILE, 'a') as log_file:
        log_file.write(message + '\n')

def create_database_if_not_exists():
    """
    Check if the database exists, and if not, create it.
    """
    # Connect to the 'postgres' default database to create the target database if necessary
    postgres_engine = create_engine(f'postgresql+psycopg2://{DB_PARAMS["user"]}:{DB_PARAMS["password"]}@{DB_PARAMS["host"]}:{DB_PARAMS["port"]}/postgres')

    with postgres_engine.connect() as conn:
        # Check if the target database exists
        result = conn.execute(text(f"SELECT 1 FROM pg_database WHERE datname = '{DB_NAME}'")).fetchone()
        if result is None:
            log_message(f"Database {DB_NAME} does not exist. Creating...")
            conn.execute(text(f"CREATE DATABASE {DB_NAME}"))
        else:
            log_message(f"Database {DB_NAME} already exists.")

    # Dispose of the 'postgres' engine once the database check/creation is done
    postgres_engine.dispose()

# PostgreSQL connection for SQLAlchemy (target database engine)
def get_engine():
    return create_engine(f'postgresql+psycopg2://{DB_PARAMS["user"]}:{DB_PARAMS["password"]}@{DB_PARAMS["host"]}:{DB_PARAMS["port"]}/{DB_PARAMS["dbname"]}')

def create_result_table_if_not_exists(conn):
    try:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS doubleedgesword (
                id SERIAL PRIMARY KEY,
                script_url TEXT UNIQUE,
                wayback_url TEXT,
                archived TEXT,
                code TEXT
            );
        """))
    except Exception as e:
        log_message(f"Error creating doubleedgesword table: {e}")

def insert_into_results(conn, script_url, wayback_url, archived, code):
    try:
        insert_stmt = text("""
            INSERT INTO doubleedgesword (script_url, wayback_url, archived, code)
            VALUES (:script_url, :wayback_url, :archived, :code)
            ON CONFLICT (script_url) DO NOTHING;
        """)
        conn.execute(insert_stmt, {
            'script_url': script_url,
            'wayback_url': wayback_url,
            'archived': archived,
            'code': code
        })
    except IntegrityError:
        log_message(f"Duplicate entry for {script_url} - skipping.")
    except Exception as e:
        log_message(f"Error inserting data for {script_url}: {e}")

def query_wayback(script_url, year):
    timestamp = f"{year}0101000000"
    params = {"url": script_url, "timestamp": timestamp}

    try:
        response = session.get(WAYBACK_API_URL, params=params, timeout=15)
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
    try:
        response = session.get(script_url, timeout=10)
        if response.status_code == 200:
            return response.text, None
        return None, f"Error {response.status_code}: {response.reason}"
    except requests.RequestException as e:
        return None, f"Error fetching live script: {e}"

def fetch_wayback_javascript(script_url, years=[2023, 2022]):
    for year in years:
        time.sleep(2)  # Sleep between requests to avoid hitting rate limits
        wayback_url, error = query_wayback(script_url, year)
        if wayback_url:
            try:
                response = session.get(wayback_url, timeout=15)
                if response.status_code == 200:
                    return response.text, "Yes", wayback_url
            except requests.RequestException as e:
                log_message(f"Wayback error fetching {script_url} for {year}: {e}")
                continue
    return None, None, None

def already_fetched(conn, script_url):
    try:
        result = conn.execute(text("SELECT 1 FROM doubleedgesword WHERE script_url = :script_url"), {'script_url': script_url}).fetchone()
        return result is not None
    except Exception as e:
        log_message(f"Error checking existing data for {script_url}: {e}")
        return False

def process_scripts(batch_size=1000):
    """
    Process URLs in chunks to avoid memory issues or connection timeouts.
    """
    engine = get_engine()
    with engine.connect() as connection:
        df = pd.read_sql_query("SELECT script_url FROM duplicate_fp_types;", connection)

    total_urls = len(df)
    progress_bar = tqdm(total=total_urls)
    
    # Process in batches to avoid long-open connections and memory issues
    for start in range(0, total_urls, batch_size):
        end = min(start + batch_size, total_urls)
        batch_df = df[start:end]

        with engine.connect() as conn:
            create_result_table_if_not_exists(conn)

            try:
                for _, row in batch_df.iterrows():
                    script_url = row['script_url']

                    if already_fetched(conn, script_url):
                        progress_bar.update(1)
                        continue

                    script_code, archived, wayback_url = fetch_wayback_javascript(script_url)

                    if not script_code:
                        script_code, error = fetch_javascript(script_url)
                        archived = "No"
                        wayback_url = ""

                        if not script_code:
                            log_message(f"Script not found for URL: {script_url}. {error}")
                            progress_bar.update(1)
                            continue

                    insert_into_results(conn, script_url, wayback_url, archived, script_code)
                    time.sleep(2)
                    progress_bar.update(1)

            except Exception as e:
                log_message(f"Error processing scripts: {e}")
            finally:
                # Perform garbage collection after each batch to prevent memory overload
                gc.collect()

    progress_bar.close()

def main():
    with open(LOG_FILE, 'w') as log_file:
        log_file.write("Wayback fetch log:\n")

    # Ensure the database is created before doing anything else
    create_database_if_not_exists()

    # Process the scripts
    process_scripts()

if __name__ == "__main__":
    main()
