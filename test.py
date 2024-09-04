import os
import json
import requests

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


def main():
    base_directory = "datasets/FP-Radar"
    extracted_data = process_json_files(base_directory)
    for data in extracted_data:
        rank, top_domain, years, api_type = data
        years.sort(reverse=True)
        api_url = "https://archive.org/wayback/available?"
        for year in years:
            params = {
                "url": top_domain,
                "timestamp": year
            }
            try:
                response = requests.get(api_url, params=params)
                print(response.status_code)
                response.raise_for_status()
                result = response.json()
                if result and result['archived_snapshots'] and result['archived_snapshots']['closest']:
                    print(response.json())
                    break
                else:
                    print(f'Could not find archived url for {top_domain} in {year}')
            except requests.exceptions.RequestException as e:
                print(f"Error fetching data from API: {e}")

if __name__ == "__main__":
    main()
