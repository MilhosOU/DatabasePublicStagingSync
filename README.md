# PostgreSQL Public Schema to Staging Schema

This Python script facilitates copying the structure, data, indexes, and triggers of tables from a PostgreSQL database's public schema to a dedicated staging schema. It leverages the `psycopg2` library to interact with the database.

## Key Features

- **Efficient Table Structure Replication**: Creates tables in the staging schema with identical structure as the source tables in the public schema.
- **Constraint Handling**: Copies primary and foreign key constraints to ensure data integrity in the staging schema.
- **Selective Index Copying**: Excludes pre-existing indexes created as foreign key constraints, preventing duplication.
- **Trigger Migration**: Replicates triggers associated with the source tables for consistent behavior in the staging environment.

## Prerequisites

- Python 3.x
- `psycopg2` library (installable via `pip install psycopg2`)
- A PostgreSQL database with a public schema and a designated staging schema.

## Environment Variables

The script relies on the following environment variables to connect to your PostgreSQL database:

- `DB_NAME`: Name of the database
- `DB_USER`: Username for database access
- `DB_PASS`: Password for the database user
- `DB_HOST`: Hostname or IP address of the database server

## Instructions

### Set Up Environment Variables:

1. Configure the required environment variables with your specific database credentials. You can set them permanently in your system's environment variables or temporarily within your script using libraries like `os`.

### Run the Script:

2. Execute the script using `python main.py`.

## Additional Notes

- This script assumes the existence of a staging schema in your database. Create it beforehand if necessary.
- Error handling can be further enhanced to provide more informative messages during execution.
- Consider incorporating logging mechanisms to track the script's operation and identify potential issues.

## Disclaimer

While this script aims to provide a robust table copying solution, it's recommended to thoroughly test it in a non-production environment before using it with critical data.
