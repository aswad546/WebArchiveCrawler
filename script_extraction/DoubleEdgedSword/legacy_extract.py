import os
import time
import requests
import pandas as pd
import gc  # For garbage collection
from tqdm import tqdm
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData
from sqlalchemy.orm import sessionmaker
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

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

# PostgreSQL connection for SQLAlchemy
engine = create_engine(f'postgresql+psycopg2://{DB_PARAMS["user"]}:{DB_PARAMS["password"]}@{DB_PARAMS["host"]}:{DB_PARAMS["port"]}/{DB_PARAMS["dbname"]}')
Session = sessionmaker(bind=engine)
session = Session()

# Create a persistent session with retry logic
http_session = requests.Session()
retry = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry)
http_session.mount("http://", adapter)
http_session.mount("https://", adapter)

# Metadata for table creation
metadata = MetaData()

# Define the table structure
doubleedgesword_table = Table(
    'doubleedgesword', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('script_url', String, unique=True),
    Column('wayback_url', String),
    Column('archived', String),
    Column('code', String)
)

# Create the table if it doesn't exist
metadata.create_all(engine)

def log_message(message):
    with open(LOG_FILE, 'a') as log_file:
        log_file.write(message + '\n')

def insert_into_results(script_url, wayback_url, archived, code):
    try:
        ins = doubleedgesword_table.insert().values(
            script_url=script_url,
            wayback_url=wayback_url,
            archived=archived,
            code=code
        )
        session.execute(ins)
        session.commit()  # Make sure to commit after every successful insert
    except Exception as e:
        log_message(f"Error inserting data for {script_url}: {e}")
        session.rollback()  # Rollback in case of error

def query_wayback(script_url, year):
    timestamp = f"{year}0101000000"
    params = {"url": script_url, "timestamp": timestamp}

    try:
        response = http_session.get(WAYBACK_API_URL, params=params, timeout=30)  # Increased timeout
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
        response = http_session.get(script_url, timeout=20)  # Increased timeout
        if response.status_code == 200:
            return response.text, None
        return None, f"Error {response.status_code}: {response.reason}"
    except requests.RequestException as e:
        return None, f"Regular error fetching live script: {e}"

def fetch_wayback_javascript(script_url, years=[2023, 2022]):
    for year in years:
        time.sleep(5)  # Increased sleep between requests to avoid hitting rate limits
        wayback_url, error = query_wayback(script_url, year)
        if wayback_url:
            try:
                response = http_session.get(wayback_url, timeout=30)  # Increased timeout
                if response.status_code == 200:
                    return response.text, "Yes", wayback_url
            except requests.RequestException as e:
                log_message(f"Wayback error fetching {script_url} for {year}: {e}")
                continue
    return None, None, None

def already_fetched(script_url):
    try:
        query = session.query(doubleedgesword_table).filter_by(script_url=script_url).first()
        return query is not None
    except Exception as e:
        log_message(f"Error checking existing data for {script_url}: {e}")
        return False

def process_scripts(batch_size=500):
    """
    Process URLs in chunks to avoid memory issues or connection timeouts.
    """
    # Fetch the list of URLs from the duplicate_fp_types table using SQLAlchemy
    with engine.connect() as connection:
        df = pd.read_sql_query("SELECT script_url FROM duplicate_fp_types;", connection)

    total_urls = len(df)
    progress_bar = tqdm(total=total_urls)
    
    # Process in batches to avoid long-open connections and memory issues
    for start in range(0, total_urls, batch_size):
        end = min(start + batch_size, total_urls)
        batch_df = df[start:end]

        try:
            for index, row in batch_df.iterrows():
                script_url = row['script_url']

                # Skip already processed URLs
                if already_fetched(script_url):
                    progress_bar.update(1)
                    continue

                # Fetch from Wayback first
                script_code, archived, wayback_url = fetch_wayback_javascript(script_url)

                if not script_code:
                    # Fallback to live script if not available in Wayback
                    script_code, error = fetch_javascript(script_url)
                    archived = "No"
                    wayback_url = ""

                    if not script_code:
                        log_message(f"Script not found for URL: {script_url}. {error}")
                        # Insert into the database with empty code (even if script is not found)
                        insert_into_results(script_url, wayback_url, archived, "")
                        progress_bar.update(1)
                        continue

                # Insert results into the database immediately to save progress
                insert_into_results(script_url, wayback_url, archived, script_code)
                time.sleep(2)  # Throttling requests to avoid overloading the server
                progress_bar.update(1)

        except Exception as e:
            log_message(f"Error processing scripts in batch starting at {start}: {e}")
        finally:
            gc.collect()  # Collect garbage to free memory after each batch

    progress_bar.close()

def main():
    with open(LOG_FILE, 'w') as log_file:
        log_file.write("Wayback fetch log:\n")

    process_scripts()

if __name__ == "__main__":
    main()