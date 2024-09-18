import requests
import pandas as pd
from sqlalchemy import create_engine
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import time
from tqdm import tqdm

# PostgreSQL connection details
dbname = 'doubleedgesword'
user = 'postgres'
password = 'postgres'
host = 'localhost'
port = '5432'

# Create SQLAlchemy engine
engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}')

# Define the new table name where we'll save the results
new_table_name = 'regular_script_extraction_results'

# Log file for errors
LOG_FILE = 'script_errors.log'

# Setup requests session with retry logic
session = requests.Session()
retry = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry)
session.mount("http://", adapter)
session.mount("https://", adapter)

# Function to log errors to a file
def log_error(message):
    with open(LOG_FILE, 'a') as log_file:
        log_file.write(message + '\n')

# Function to fetch and extract the script from the provided URL
def extract_script_from_url(script_url):
    try:
        response = session.get(script_url, timeout=10)  # Add a timeout to avoid hanging
        if response.status_code == 200:
            return response.text
        else:
            log_error(f"Failed to retrieve script from {script_url}, status code: {response.status_code}")
            return None
    except requests.RequestException as e:
        log_error(f"Error fetching script from {script_url}: {e}")
        return None

# Fetch the data from the view 'duplicate_script_url_fp_types'
def fetch_duplicate_script_urls():
    query = "SELECT script_url, fp_types_json FROM duplicate_fp_types"
    with engine.connect() as connection:
        return pd.read_sql_query(query, connection)

# Batch save to the new table
def batch_save_to_new_table(batch_data):
    if not batch_data:
        return  # No data to save

    df = pd.DataFrame(batch_data)

    with engine.connect() as connection:
        df.to_sql(new_table_name, con=connection, if_exists='append', index=False)
        print(f"Batch of {len(df)} rows saved to table '{new_table_name}'")

# Main processing function
def process_and_save_scripts(batch_size=100):
    # Fetch URLs and associated JSON from the view
    df = fetch_duplicate_script_urls()

    batch_data = []

    # Initialize the progress bar
    progress_bar = tqdm(total=len(df), desc="Processing Scripts", unit="script")

    for i, row in df.iterrows():
        script_url = row['script_url']
        fp_types_json = row['fp_types_json']

        # Extract the script from the URL
        extracted_script = extract_script_from_url(script_url)

        if extracted_script:
            # Add data to batch
            batch_data.append({
                'script_url': script_url,
                'fp_types_json': fp_types_json,
                'extracted_script': extracted_script
            })

        # If batch size is reached, save the batch and clear it
        if len(batch_data) >= batch_size:
            batch_save_to_new_table(batch_data)
            batch_data.clear()  # Clear the batch after saving

        # Add a small delay to avoid overwhelming the server
        time.sleep(2)

        # Update the progress bar
        progress_bar.update(1)

    # Save any remaining data in the batch
    if batch_data:
        batch_save_to_new_table(batch_data)

    # Close the progress bar
    progress_bar.close()

# Run the process
process_and_save_scripts()
