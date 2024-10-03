import pandas as pd
from sqlalchemy import create_engine

# Replace with your database credentials
db_type = 'postgresql'  # or 'postgresql' or 'sqlite'
username = 'postgres'
password = 'postgres'
host = 'localhost'  # or your host
port = '5432'  # Default for MySQL (5432 for PostgreSQL)
database = 'doubleedgesword'

# Create the database engine
if db_type == 'mysql':
    engine = create_engine(f'mysql+mysqlconnector://{username}:{password}@{host}:{port}/{database}')
elif db_type == 'postgresql':
    engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{database}')
elif db_type == 'sqlite':
    engine = create_engine(f'sqlite:///{database}')

# Query the table and load it into a pandas DataFrame
table_name = 'doubleedgesword'
df = pd.read_sql(f'SELECT * FROM {table_name}', con=engine)

# Save the DataFrame to a CSV file
output_csv = 'doubleedgesword_output.csv'
df.to_csv(output_csv, index=False)

print(f"Table '{table_name}' has been successfully exported to '{output_csv}'")
