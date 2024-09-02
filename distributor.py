import os
import json

def extract_data_from_json(json_content):
    """
    Extracts relevant data from the JSON content.
    
    Args:
    - json_content (dict): The JSON content loaded from the file.
    
    Returns:
    - List of extracted data tuples in the form (unique_id, top_domain, years).
    """
    extracted_data = []
    
    for unique_id, data in json_content.items():
        # Check if 'topDomain' is present in the entry
        if "topDomain" not in data:
            continue  # A non-fingerprinting script does not contain a topDomain hence I cannot check its existence
        
        top_domain_list = data["topDomain"]
        years = data.get("year", [])
        
        # Ensure 'topDomain' has values
        if not top_domain_list:
            continue  # Skip if 'topDomain' list is empty
        
        for top_domain in top_domain_list:
            # Save the years as an array (list)
            extracted_data.append((unique_id, top_domain, years))
    
    return extracted_data

def process_json_files(directory):
    """
    Processes all JSON files in the specified directory structure and extracts relevant data.
    
    Args:
    - directory (str): The base directory to start searching for JSON files.
    
    Returns:
    - List of all extracted data tuples.
    """
    all_extracted_data = []

    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".json"):
                file_path = os.path.join(root, file)
                
                with open(file_path, "r") as json_file:
                    try:
                        json_content = json.load(json_file)
                        extracted_data = extract_data_from_json(json_content)
                        all_extracted_data.extend(extracted_data)
                    except json.JSONDecodeError as e:
                        print(f"Error decoding JSON from file {file_path}: {e}")
    
    return all_extracted_data

def main():
    # Set the base directory path (modify as per your directory structure)
    base_directory = "datasets/FP-Radar"
    
    # Extract data from JSON files
    extracted_data = process_json_files(base_directory)
    
    # Print extracted data
    for data in extracted_data:
        unique_id, top_domain, years = data
        print(f"ID: {unique_id}, Domain: {top_domain}, Years: {years}")
if __name__ == "__main__":
    main()
