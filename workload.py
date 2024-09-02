import requests
import json
import psycopg2
from psycopg2 import sql
from confluent_kafka import Consumer, KafkaException, KafkaError

# Database connection details
DB_NAME = "literature_scripts"
DB_USER = "vv8"
DB_PASSWORD = "vv8"
DB_HOST = "localhost"  
DB_PORT = "5434"  

# Kafka configuration
KAFKA_BROKER = 'localhost:9092' 
KAFKA_TOPIC = 'web_archive_script_extraction'  
KAFKA_GROUP = 'web_archive_group'  # Consumer group

# Function to fetch data from an API
def fetch_data_from_api(api_url, params=None):
    try:
        response = requests.get(api_url, params=params)
        response.raise_for_status()  # Raise an error for bad responses
        return response.json() 
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return None

# Function to fetch script content from a URL
def fetch_script_content(script_url):
    try:
        response = requests.get(script_url)
        response.raise_for_status()  # Raise an error for bad responses
        return response.text  # Get the raw text of the script file
    except requests.exceptions.RequestException as e:
        print(f"Error fetching script from URL: {e}")
        return None

# Function to connect to the PostgreSQL database
def connect_to_database():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        return conn
    except psycopg2.DatabaseError as e:
        print(f"Error connecting to the database: {e}")
        return None

# Function to create the table if it does not exist
def create_table_if_not_exists(conn):
    try:
        with conn.cursor() as cursor:
            create_table_query = """
            CREATE TABLE IF NOT EXISTS fingerprinters_fp_radar (
                id SERIAL PRIMARY KEY,
                rank INTEGER,
                url TEXT,
                web_archive_url TEXT,
                code TEXT,
                fp_type TEXT,
                timestamp TIMESTAMP
            );
            """
            cursor.execute(create_table_query)
            conn.commit()
    except psycopg2.DatabaseError as e:
        print(f"Error creating table: {e}")

# Function to insert the script content into the database
def insert_script_to_database(conn, rank, url, web_archive_url, code, fp_type):
    try:
        with conn.cursor() as cursor:
            insert_query = """
            INSERT INTO fingerprinters_fp_radar (rank, url, web_archive_url, code, fp_type, timestamp)
            VALUES (%s, %s, %s, %s, %s, now());
            """
            cursor.execute(insert_query, (rank, url, web_archive_url, code, fp_type))
            conn.commit()
    except psycopg2.DatabaseError as e:
        print(f"Error inserting data into the table: {e}")

def consume_messages():
    # Connect to the PostgreSQL database
    conn = connect_to_database()
    if conn is None:
        print("Database connection failed.")
        return
    
    # Create the table if it does not exist
    create_table_if_not_exists(conn)

    # Configure Kafka consumer
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': KAFKA_GROUP,
        'auto.offset.reset': 'earliest'
    })

    # Subscribe to Kafka topic
    consumer.subscribe([KAFKA_TOPIC])

    try:
        while True:
            msg = consumer.poll(1.0)  # Poll for new messages from Kafka

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Kafka error: {msg.error()}")
                    break

            # Process message
            data = json.loads(msg.value().decode('utf-8'))
            url = data.get('url')
            years = data.get('years')
            rank = data.get('rank') 
            api_type = data.get('api_type')

            # Ensure years is sorted latest to earliest and check all the years the script might have existed in
            years.sort(reverse=True)

            api_url = "https://archive.org/wayback/available?"
            params = {'url': url, 'timestamp': years[0]} # Get script from most recent year
            api_data = fetch_data_from_api(api_url, params)
            years.pop(0)
            while 'archived_snapshots' not in api_data and len(years) != 0:
                params = {'url': url, 'timestamp': years[0]}
                api_data = fetch_data_from_api(api_url, params)
                years.pop(0) # Remove the year that has no script in the web archive
            if not api_data or 'archived_snapshots' not in api_data:
                print("Failed to fetch data from API.")
                continue

            existing_snapshot = api_data['archived_snapshots'].get('closest')
            
            # Check if a snapshot exists for this page with the given year
            if existing_snapshot:
                script_url = existing_snapshot['url']
                
                if script_url:
                    # Fetch the script content from the extracted URL
                    script_content = fetch_script_content(script_url)
                    
                    if script_content:
                        print("Script fetched successfully.")
                        
                        # Insert the script content into the database
                        insert_script_to_database(conn, rank, url, script_url, script_content, api_type)
                        print("Script content inserted into the database.")
                    else:
                        print("Failed to fetch the script content.")
                else:
                    print("No script URL found in API response.")
            else:
                print("No snapshot available for the specified URL and timestamp.")

            # Check for stop signal
            if data.get('command') == 'stop':
                print("Received stop command. Shutting down.")
                break

    except KeyboardInterrupt:
        print("Interrupted by user")

    finally:
        # Close Kafka consumer and database connection
        consumer.close()
        conn.close()

if __name__ == "__main__":
    consume_messages()
