#!/bin/bash

# Variables
DB_NAME="doubleedgesword"
DB_USER="postgres"
DB_HOST="localhost"
DB_PORT="5432"
TABLE_NAME="doubledgesword_fp_results"
CSV_FILE_PATH="/home/vagrant/WebArchiveScriptCrawler/script_extraction/DoubleEdgedSword/2023_08_25_top_100k_fps.csv"

# SQL to create the table
CREATE_TABLE_SQL="
CREATE TABLE IF NOT EXISTS ${TABLE_NAME} (
    script_url TEXT,
    fp_type TEXT,
    initial_url TEXT,
    final_url TEXT,
    site_domain TEXT,
    is_homepage BOOLEAN,
    is_innerpage BOOLEAN,
    script_domain TEXT,
    description TEXT,
    access_type TEXT,
    arguments TEXT,
    frameUrl TEXT,
    return_value TEXT,
    is_third_party BOOLEAN,
    tracker_categories TEXT,
    tracker_owner TEXT,
    is_tracker INTEGER,
    hostname TEXT
);
"

# Create the table in PostgreSQL
echo "Creating table ${TABLE_NAME} if it doesn't exist..."
psql -h ${DB_HOST} -p ${DB_PORT} -U ${DB_USER} -d ${DB_NAME} -c "${CREATE_TABLE_SQL}"

# Import the CSV file into the PostgreSQL table
echo "Importing data from ${CSV_FILE_PATH} into ${TABLE_NAME}..."
psql -h ${DB_HOST} -p ${DB_PORT} -U ${DB_USER} -d ${DB_NAME} -c "\COPY ${TABLE_NAME} FROM '${CSV_FILE_PATH}' DELIMITER ',' CSV HEADER;"

echo "Data import completed."
