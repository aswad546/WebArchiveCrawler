import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
from tqdm import tqdm  # Import tqdm for progress bar

# PostgreSQL connection details
dbname = 'postgres'  # Replace with your desired database name
user = 'postgres'
password = 'postgres'  # Ensure this is the correct password
host = 'localhost'
port = '5432'  # Confirm if this is the correct port or change to 5432 if needed

# Create SQLAlchemy engine
engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}')

def recreate_company_tables(recreate_tables=True):
    # Step 1: Read the Keywords from a Text File
    keywords_file_path = 'keywords.txt'  # Replace with your actual keywords text file path

    keywords_dict = {}

    # Read the keywords text file line by line
    with open(keywords_file_path, 'r') as file:
        for line in file:
            # Split each line by commas and convert to lowercase
            parts = [part.strip().lower() for part in line.split(',')]
            if len(parts) > 1:
                company = parts[0]
                keywords = parts[1:]
                keywords_dict[company] = keywords

    # Step 2: Connect to the database and get total number of rows in locality_result
    with engine.connect() as connection:
        # Get total number of rows in the original table
        total_rows_query = text("SELECT COUNT(*) FROM locality_result")
        total_rows = connection.execute(total_rows_query).scalar()
        
        print(f"Total rows in 'locality_result': {total_rows}")

        for company, keywords in keywords_dict.items():
            # Prepare the search conditions
            search_conditions = []
            parameters = {}

            for i, keyword in enumerate(keywords):
                # Handle specific keyword pattern with wildcard '*'
                keyword = keyword.lower()  # Convert keyword to lowercase
                if '*' in keyword:
                    keyword = keyword.replace('*', '%')  # Convert '*' to SQL's '%' wildcard
                    condition = f"(LOWER(code) ILIKE :keyword_{i} OR LOWER(script_name) ILIKE :keyword_{i})"
                else:
                    condition = f"(LOWER(code) ILIKE :keyword_{i} OR LOWER(script_name) ILIKE :keyword_{i})"
                    keyword = f"%{keyword}%"

                search_conditions.append(condition)
                parameters[f"keyword_{i}"] = keyword

            # Combine all search conditions with OR
            combined_conditions = " OR ".join(search_conditions)
            query = text(f"SELECT * FROM locality_result WHERE {combined_conditions}")

            # Execute query and fetch results
            try:
                print(f"Executing Query for {company}")
                result_df = pd.read_sql_query(query, connection, params=parameters)
            except Exception as e:
                print(f"Error executing query for {company}: {e}")
                continue
            print("Executed Query")

            # Step 3: Identify matching keywords for each row
            if not result_df.empty:
                # Initialize the 'matches' column with empty strings
                result_df['matches'] = ''

                # Add tqdm progress bar for analyzing rows
                with tqdm(total=total_rows, desc=f"Processing {company}", unit="row", dynamic_ncols=True) as pbar:
                    for i, row in result_df.iterrows():
                        matched_keywords = []
                        code_lower = row['code'].lower() if pd.notna(row['code']) else ''
                        script_name_lower = row['script_name'].lower() if pd.notna(row['script_name']) else ''
                        for keyword in keywords:
                            sql_wildcard_keyword = keyword.replace('*', '%') if '*' in keyword else f"%{keyword}%"
                            # Check if the keyword exists in the lowercased versions of code or script_name
                            if sql_wildcard_keyword.replace('%', '') in code_lower or \
                               sql_wildcard_keyword.replace('%', '') in script_name_lower:
                                matched_keywords.append(keyword)

                        # Join the matched keywords into a single string
                        result_df.at[i, 'matches'] = ', '.join(matched_keywords)

                        # Update progress bar
                        pbar.update(1)

                # Ensure that the DataFrame has the correct format
                table_name = company.replace(' ', '_').replace('.', '_')  # Ensure table name is SQL-friendly

                # Optionally drop the table if it exists to recreate it
                if recreate_tables:
                    try:
                        connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                        print(f"Table '{table_name}' dropped.")
                    except Exception as e:
                        print(f"Error dropping table '{table_name}': {e}")

                # Save the results to a new table named after the company
                try:
                    result_df.to_sql(table_name, engine, if_exists='replace', index=False)
                    print(f"Results for {company} saved to table '{table_name}'.")
                except Exception as e:
                    print(f"Error saving results for {company} to table '{table_name}': {e}")
            else:
                print(f"No results found for {company}.")

# Call the function to execute the logic
recreate_company_tables(recreate_tables=True)  # Change to False if you don't want to recreate tables
