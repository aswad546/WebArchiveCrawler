import os
import requests
import json
import psycopg2
from psycopg2 import sql
from confluent_kafka import Consumer, KafkaError


# Configuration details
DB_NAME = os.getenv('DB_NAME', 'literature_scripts')
DB_USER = os.getenv('DB_USER', 'vv8')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'vv8')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'web_archive_script_extraction')
KAFKA_GROUP = os.getenv('KAFKA_GROUP', 'web_archive_group')

class DatabaseService:
    def __init__(self):
        self.conn = self.connect_to_database()

    def connect_to_database(self):
        try:
            conn = psycopg2.connect(
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                host=DB_HOST,
                port=DB_PORT
            )
            self.create_table_if_not_exists(conn)
            return conn
        except psycopg2.DatabaseError as e:
            print(f"Error connecting to the database: {e}")
            return None

    def create_table_if_not_exists(self, conn):
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

    def insert_script_to_database(self, rank, url, web_archive_url, code, fp_type):
        try:
            with self.conn.cursor() as cursor:
                insert_query = """
                INSERT INTO fingerprinters_fp_radar (rank, url, web_archive_url, code, fp_type, timestamp)
                VALUES (%s, %s, %s, %s, %s, now());
                """
                cursor.execute(insert_query, (rank, url, web_archive_url, code, fp_type))
                self.conn.commit()
        except psycopg2.DatabaseError as e:
            print(f"Error inserting data into the table: {e}")


class WebArchiveFetcher:
    def __init__(self, url, years):
        self.url = url
        self.years = years
        self.api_url = "https://archive.org/wayback/available?"

    def fetch_data_from_api(self, params=None):
        try:
            response = requests.get(self.api_url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data from API: {e}")
            return None

    def fetch_script_content(self, script_url):
        try:
            response = requests.get(script_url)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            print(f"Error fetching script from URL: {e}")
            return None

    def extract_js_url(self):
        js_index = self.url.find('.js')
        if js_index != -1:
            return self.url[:js_index + 3]
        return self.url

    def get_valid_snapshot(self):
        self.url = self.extract_js_url()
        orig_years = self.years.copy()
        orig_years.sort(reverse=True)
        for year in orig_years:
            print(f'Trying to get snapshot for URL:{self.url} in year: {year}')
            params = {'url': self.url, 'timestamp': year}
            api_data = self.fetch_data_from_api(params)
            if api_data and 'archived_snapshots' in api_data and api_data['archived_snapshots']:
                return api_data['archived_snapshots'].get('closest')
        print(f"Failed to find a valid snapshot for URL: {self.url} in timeframe: {orig_years}")
        return None


class KafkaConsumerService:
    def __init__(self, db_service):
        self.db_service = db_service
        self.consumer = Consumer({
            'bootstrap.servers': KAFKA_BROKER,
            'group.id': KAFKA_GROUP,
            'auto.offset.reset': 'earliest'
        })
        self.consumer.subscribe([KAFKA_TOPIC])

    def consume_messages(self):
        try:
            while True:
                msg = self.consumer.poll(1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(f"Kafka error: {msg.error()}")
                        break

                data = json.loads(msg.value().decode('utf-8'))
                url = data.get('url')
                years = data.get('years')
                rank = data.get('rank')
                api_type = data.get('api_type')

                fetcher = WebArchiveFetcher(url, years)
                existing_snapshot = fetcher.get_valid_snapshot()

                if existing_snapshot:
                    script_url = existing_snapshot['url']
                    script_content = fetcher.fetch_script_content(script_url)

                    if script_content:
                        print(f"Script fetched successfully for URL: {url} and snapshot: {script_url}")
                        self.db_service.insert_script_to_database(rank, url, script_url, script_content, api_type)
                        print(f"Script content inserted into the database for URL: {url} and snapshot: {script_url}")
                    else:
                        print(f"Failed to fetch script content from snapshot URL: {script_url} for original URL: {url}.")
                else:
                    print(f"No valid snapshot found for URL: {url} and years: {years}.")

                if data.get('command') == 'stop':
                    print("Received stop command. Shutting down.")
                    break

        except KeyboardInterrupt:
            print("Interrupted by user")

        finally:
            self.consumer.close()
            if self.db_service.conn:
                self.db_service.conn.close()


if __name__ == "__main__":
    db_service = DatabaseService()
    if db_service.conn:
        kafka_service = KafkaConsumerService(db_service)
        kafka_service.consume_messages()
