#!/bin/bash

# Variables
DB_NAME="doubleedgesword"
DB_USER="postgres"
DB_PASSWORD="postgres"
DB_HOST="localhost"
DB_PORT="5432"
SOURCE_TABLE="doubledgesword_fp_results"
TARGET_TABLE="duplicate_fp_types"

# Export PostgreSQL password to avoid prompt
export PGPASSWORD=$DB_PASSWORD

# SQL to drop the target table if it exists, then create the new table
DROP_AND_CREATE_TABLE_SQL="
DROP TABLE IF EXISTS ${TARGET_TABLE};
CREATE TABLE ${TARGET_TABLE} (
    id SERIAL PRIMARY KEY,
    script_url TEXT UNIQUE,
    fp_type JSONB
);
"

# SQL to insert unique script_urls and aggregate fp_type into the new table
INSERT_UNIQUE_SQL="
INSERT INTO ${TARGET_TABLE} (script_url, fp_type)
SELECT
    script_url,
    jsonb_agg(DISTINCT fp_type) AS fp_type
FROM ${SOURCE_TABLE}
GROUP BY script_url
ON CONFLICT DO NOTHING;
"

# Drop the existing table and create the new one
echo "Dropping table ${TARGET_TABLE} if it exists and creating a new table..."
psql -h ${DB_HOST} -p ${DB_PORT} -U ${DB_USER} -d ${DB_NAME} -c "${DROP_AND_CREATE_TABLE_SQL}"

# Insert unique script_urls with aggregated fp_type into the new table
echo "Inserting unique script_urls and aggregated fp_type from ${SOURCE_TABLE} into ${TARGET_TABLE}..."
psql -h ${DB_HOST} -p ${DB_PORT} -U ${DB_USER} -d ${DB_NAME} -c "${INSERT_UNIQUE_SQL}"

echo "Unique script_urls and aggregated fp_type have been inserted into ${TARGET_TABLE}."
