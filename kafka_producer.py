import os
import json
from confluent_kafka import Producer

def extract_data_from_json(json_content):
    """
    Extracts relevant data from the JSON content.

    Args:
    - json_content (dict): The JSON content loaded from the file.

    Returns:
    - List of extracted data tuples in the form (rank, top_domain, years).
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

    Args:
    - directory (str): The base directory to start searching for JSON files.

    Returns:
    - List of all extracted data tuples, each with an added 'api_type'.
    """
    all_extracted_data = []

    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".json"):
                file_path = os.path.join(root, file)
                
                # Extract the immediate parent directory name
                parent_directory = os.path.basename(os.path.dirname(file_path)).lower()

                with open(file_path, "r") as json_file:
                    try:
                        json_content = json.load(json_file)
                        extracted_data = extract_data_from_json(json_content)
                        
                        # Add the parent directory name as 'api_type' to each extracted data tuple
                        for data in extracted_data:
                            all_extracted_data.append((*data, parent_directory))
                    except json.JSONDecodeError as e:
                        print(f"Error decoding JSON from file {file_path}: {e}")

    return all_extracted_data

def delivery_report(err, msg):
    """
    Delivery report callback called once for each produced message to indicate delivery result.

    Args:
    - err (KafkaError): Error information, or None on success.
    - msg (Message): The produced message.
    """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def produce_messages(extracted_data, kafka_config, topic_name):
    """
    Produces messages to Kafka topic using the extracted data.

    Args:
    - extracted_data (list): List of tuples containing (rank, top_domain, years, api_type).
    - kafka_config (dict): Configuration for the Kafka producer.
    - topic_name (str): The Kafka topic name to produce messages to.
    """
    producer = Producer(kafka_config)

    for data in extracted_data:
        rank, top_domain, years, api_type = data
        message = json.dumps({
            "rank": rank,
            "url": top_domain,
            "years": years,
            "api_type": api_type
        })
        producer.produce(topic_name, message, callback=delivery_report)
        producer.poll(0)

    # Wait for any outstanding messages to be delivered and delivery reports to be received
    producer.flush()

def main():
    base_directory = "datasets/FP-Radar"
    
    # Kafka configuration
    kafka_config = {
        'bootstrap.servers': 'localhost:9092',  # Update with your Kafka broker address
    }
    topic_name = 'web_archive_script_extraction'  

    # Extract data from JSON files
    extracted_data = process_json_files(base_directory)

    # Produce messages to Kafka
    produce_messages(extracted_data, kafka_config, topic_name)

if __name__ == "__main__":
    main()
