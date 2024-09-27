import os
import time
import requests
import pandas as pd
import gc  # For garbage collection
from tqdm import tqdm
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import psycopg2

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

# Set up basic retry logic with built-in Retry object for minor errors
session = requests.Session()
retry = Retry(
    total=3,                # Lower retries here (to avoid too many retries)
    backoff_factor=1,        # Use a small backoff factor for minor retries
    status_forcelist=[500, 502, 503, 504],  # Retry on server errors
    raise_on_status=False    # Do not raise exceptions for retries
)
adapter = HTTPAdapter(max_retries=retry)
session.mount("http://", adapter)
session.mount("https://", adapter)


# Logging function
def log_message(message):
    with open(LOG_FILE, 'a') as log_file:
        log_file.write(message + '\n')
    # print(message)

def exponential_backoff_retry(function, *args, max_retries=5, base_delay=2, **kwargs):
    retry_count = 0
    delay = base_delay

    while retry_count < max_retries:
        try:
            # Make the request or function call
            response = function(*args, **kwargs)
            
            # Check if the response is a tuple (e.g., for query_wayback)
            if isinstance(response, tuple):
                return response  # Return the tuple as-is without checking status_code

            # If it's an HTTP response, check for rate limiting (HTTP 429) or other server errors
            if response.status_code in [429, 500, 502, 503, 504]:
                retry_count += 1
                log_message(f"Error {response.status_code}: {response.reason}. Retrying in {delay} seconds... (Attempt {retry_count}/{max_retries})")
                
                # Check for Retry-After header in case of rate limiting (HTTP 429)
                if response.status_code == 429 and 'Retry-After' in response.headers:
                    retry_after = int(response.headers['Retry-After'])
                    log_message(f"Rate-limited. Retry after {retry_after} seconds.")
                    time.sleep(retry_after)
                else:
                    # Use exponential backoff
                    time.sleep(delay)
                    delay *= 2  # Exponentially increase the delay

                # If max retries reached, log and return None
                if retry_count == max_retries:
                    log_message(f"Max retries reached for {function.__name__}. Giving up.")
                    return None
            else:
                # If the request is successful, return the response
                return response

        except requests.RequestException as e:
            # Handle network-related errors (e.g., timeout, connection errors)
            retry_count += 1
            log_message(f"Network error: {e}. Retrying in {delay} seconds... (Attempt {retry_count}/{max_retries})")
            time.sleep(delay)
            delay *= 2  # Exponentially increase the delay

            # If max retries reached, log and return None
            if retry_count == max_retries:
                log_message(f"Max retries reached for {function.__name__}. Giving up.")
                return None




def fetch_javascript(script_url):
    """
    Fetch the JavaScript file from the live web if it is not available from Wayback Machine.

    Args:
        script_url (str): The URL of the JavaScript file to fetch.
    
    Returns:
        - JavaScript code (if successful)
        - error message (if the request fails)
    """
    try:
        # Fetch the script with retries using exponential_backoff_retry with a longer timeout
        response = exponential_backoff_retry(session.get, script_url, timeout=30)
        
        # If the request is successful, return the script content and no error
        if response and response.status_code == 200:
            return response.text, None
        
        # Return an error message if the response is not successful
        return None, f"Error fetching JavaScript from {script_url}: {response.status_code if response else 'No response'}"
    
    except requests.RequestException as e:
        # Catch and return any exceptions that occur during the fetch
        return None, f"Error fetching JavaScript: {e}"





def create_database_if_not_exists():
    # Connect to the 'postgres' default database to create the target database if necessary
    log_message("Checking if the database exists...")
    postgres_engine = create_engine(f'postgresql+psycopg2://{DB_PARAMS["user"]}:{DB_PARAMS["password"]}@{DB_PARAMS["host"]}:{DB_PARAMS["port"]}/postgres', isolation_level="AUTOCOMMIT")
    
    with postgres_engine.connect() as conn:
        result = conn.execute(text(f"SELECT 1 FROM pg_database WHERE datname = '{DB_NAME}'")).fetchone()
        if result is None:
            log_message(f"Database {DB_NAME} does not exist. Creating...")
            conn.execute(text(f"CREATE DATABASE {DB_NAME}"))
            log_message(f"Database {DB_NAME} created successfully.")
        else:
            log_message(f"Database {DB_NAME} already exists.")
    postgres_engine.dispose()

def get_engine():
    log_message(f"Connecting to the {DB_NAME} database...")
    return create_engine(f'postgresql+psycopg2://{DB_PARAMS["user"]}:{DB_PARAMS["password"]}@{DB_PARAMS["host"]}:{DB_PARAMS["port"]}/{DB_PARAMS["dbname"]}')

def create_result_table_if_not_exists(conn):
    try:
        log_message("Attempting to create the doubleedgesword table if it doesn't exist...")
        
        # Explicitly create the table if it doesn't exist
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS doubleedgesword (
                id SERIAL PRIMARY KEY,
                script_url TEXT UNIQUE,
                wayback_url TEXT,
                archived TEXT,
                code TEXT
            );
        """))
        
        log_message("Table 'doubleedgesword' created successfully or already exists.")
        
        # Commit after creating the table to avoid transaction issues
        conn.commit()

    except SQLAlchemyError as e:
        log_message(f"Error creating doubleedgesword table: {e}")
        conn.rollback()  # Rollback if the table creation fails to reset the transaction


def already_fetched(conn, script_url):
    """
    Check if a script_url is already in the database.
    """
    try:
        result = conn.execute(text("SELECT 1 FROM doubleedgesword WHERE script_url = :script_url"), {'script_url': script_url}).fetchone()
        return result is not None
    except Exception as e:
        log_message(f"Error checking existing data for {script_url}: {e}")
        return False
def query_wayback(script_url, year):
    """
    Query the Wayback Machine API for the closest snapshot of the provided script URL for the given year.
    
    Args:
        script_url: The URL of the JavaScript file.
        year: The year to search for the archived snapshot.
    
    Returns:
        - wayback_url (str): The URL to the archived snapshot, if available.
        - error (str): An error message if the snapshot is not found or if the request fails.
    """
    # Create a timestamp using the beginning of the specified year
    timestamp = f"{year}0101000000"
    params = {"url": script_url, "timestamp": timestamp}

    try:
        # Query the Wayback Machine API
        response = session.get(WAYBACK_API_URL, params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            
            # Check if an archived snapshot is available
            if 'archived_snapshots' in data and 'closest' in data['archived_snapshots']:
                snapshot = data['archived_snapshots']['closest']
                
                if snapshot['available']:
                    return snapshot['url'], None  # Return the wayback URL
            
        return None, f"No snapshot found for {year}."  # Return an error if no snapshot found
    except requests.RequestException as e:
        return None, f"Error querying Wayback Machine: {e}"  # Return error if the request fails


def fetch_wayback_javascript(script_url, years=[2023, 2022]):
    """
    Tries to fetch the JavaScript file from the Wayback Machine for the provided URL.
    It will try for the specified years in descending order (most recent first).
    
    Returns:
        - JavaScript code (if found)
        - 'Yes' (if archived version is used)
        - Wayback URL of the archived JavaScript
    """
    for year in years:
        # Query Wayback Machine for available snapshot of the script URL
        wayback_tuple = exponential_backoff_retry(query_wayback, script_url, year)
        
        if wayback_tuple:  # Ensure it's not None
            wayback_url, error = wayback_tuple
            
            if wayback_url:
                # Fetch the JavaScript from the Wayback URL
                response = exponential_backoff_retry(session.get, wayback_url, timeout=30)
                
                # Check if the fetch was successful
                if response and response.status_code == 200:
                    return response.text, "Yes", wayback_url
                
                # Log error if the fetch failed
                log_message(f"Error fetching Wayback JavaScript from {wayback_url}: {response.status_code if response else 'No response'}")
                time.sleep(2)  # Adding a small delay to avoid rate limiting
    
    # If no archived version is found, return None values
    return None, None, None






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
        conn.commit()  # Commit after successful insertion
        # log_message(f"Inserted data for {script_url}.")
    except IntegrityError:
        conn.rollback()  # Rollback the transaction if there's a conflict
        log_message(f"Duplicate entry for {script_url} - skipping.")
    except Exception as e:
        conn.rollback()  # Rollback the transaction in case of any other error
        log_message(f"Error inserting data for {script_url}: {e}")


def process_scripts(batch_size=100):
    engine = get_engine()

    # Create the table before processing scripts
    with engine.connect() as conn:
        create_result_table_if_not_exists(conn)
    
    # Now process the scripts
    with engine.connect() as connection:
        df = pd.read_sql_query("SELECT script_url FROM duplicate_fp_types;", connection)

    total_urls = len(df)
    progress_bar = tqdm(total=total_urls)
    
    for start in range(0, total_urls, batch_size):
        end = min(start + batch_size, total_urls)
        batch_df = df[start:end]

        with engine.connect() as conn:
            try:
                for _, row in batch_df.iterrows():
                    script_url = row['script_url']

                    if already_fetched(conn, script_url):
                        progress_bar.update(1)
                        continue

                    # Try to fetch from Wayback Machine
                    script_code, archived, wayback_url = fetch_wayback_javascript(script_url)

                    # If Wayback Machine doesn't have it, fetch directly
                    if not script_code:
                        script_code, error = fetch_javascript(script_url)
                        archived = "No"
                        wayback_url = ""

                        # If no code is found, insert empty string for script_code
                        if not script_code:
                            log_message(f"Script not found for URL: {script_url}. {error}")
                            script_code = None  # Insert empty string (or None if you want NULL)
                            archived = "No"
                            wayback_url = ""

                    # Insert the results into the database, even if the script_code is empty
                    insert_into_results(conn, script_url, wayback_url, archived, script_code)
                    time.sleep(2)
                    progress_bar.update(1)

            except Exception as e:
                log_message(f"Error processing scripts: {e}")
            finally:
                gc.collect()

    progress_bar.close()


def main():
    with open(LOG_FILE, 'w') as log_file:
        log_file.write("Wayback fetch log:\n")

    create_database_if_not_exists()  # Ensure the database is created
    process_scripts()

if __name__ == "__main__":
    main()
