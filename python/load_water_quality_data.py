import os
import pandas as pd
import snowflake.connector
import requests
from io import StringIO

# Prompt for CSV file path
csv_url = 'https://data.cnra.ca.gov/dataset/3f96977e-2597-4baa-8c9b-c433cea0685e/resource/a9e7ef50-54c3-4031-8e44-aa46f3c660fe/download/lab_results.csv'
print(f"Downloading CSV from {csv_url}...")
print("This may take a few minutes for large datasets...")

# Read CSV file with proper headers to avoid 403 errors
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
response = requests.get(csv_url, headers=headers)
response.raise_for_status()

# Ensure all data is read as string, which maps to VARCHAR in Snowflake
df = pd.read_csv(StringIO(response.text), dtype="str")

# --- IMPORTANT: Clean DataFrame Column Names ---
# This is crucial for valid Snowflake identifiers.
# Snowflake unquoted identifiers are uppercase and cannot contain spaces or special chars
# (except underscore). We will convert them to uppercase and replace invalid characters.
original_columns = df.columns.tolist()
df.columns = [
    col.strip().replace(' ', '_').replace('-', '_').replace('.', '_').upper()
    for col in df.columns
]
print(f"Original columns: {original_columns}")
print(f"Cleaned columns for Snowflake: {df.columns.tolist()}")

# Connect to Snowflake using environment variables
# Ensure case-consistency for database/schema/table names if you rely on unquoted identifiers
conn = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    authenticator=os.environ["SNOWFLAKE_AUTHENTICATOR"],
    database=os.environ["SNOWFLAKE_DATABASE"].upper(), # Ensure DB name is uppercase
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"].upper(), # Ensure Warehouse name is uppercase
    role=os.environ["SNOWFLAKE_ROLE"].upper(), # Ensure Role name is uppercase
    schema="WATER_QUALITY".upper(), # Explicitly uppercase for consistency
)

try:
    cursor = conn.cursor()
    database_name = os.environ["SNOWFLAKE_DATABASE"].upper()
    schema_name = "WATER_QUALITY".upper()
    table_name = "LAB_RESULTS_TEST_2026".upper() # Ensure table name is uppercase

    full_table_path = f"{database_name}.{schema_name}.{table_name}"

    print(f"Attempting to load data to: {full_table_path}")

    # Step 1: Drop the table if it exists (for clean runs)
    # This prevents issues if a previous run created it partially or with errors
    cursor.execute(f"DROP TABLE IF EXISTS {full_table_path}")
    print(f"Table {full_table_path} dropped if it existed.")

    # Step 2: Manually construct and execute CREATE TABLE statement
    # This bypasses write_pandas's internal creation logic that might be failing.
    columns_ddl = []
    for col in df.columns:
        # Since we read everything as 'str', we'll use VARCHAR for all columns.
        # Determine appropriate VARCHAR length. For simplicity, we can start with a large enough one.
        # If your data has very long strings, you might need to adjust or infer max length.
        # For 'str' dtype, pandas usually infers 'object'. Max length for VARCHAR is 16MB.
        # Let's use a default large enough for most lab results.
        columns_ddl.append(f"{col} VARCHAR(16777216)") # Max VARCHAR length in Snowflake

    create_table_sql = f"CREATE TABLE {full_table_path} ({', '.join(columns_ddl)})"

    print(f"\nExecuting CREATE TABLE DDL:\n{create_table_sql}")
    cursor.execute(create_table_sql)
    print(f"Table {full_table_path} created successfully.")

    # Step 3: Load data to Snowflake using PUT/COPY (no pyarrow needed)
    temp_csv = "/tmp/lab_results_temp.csv"
    df.to_csv(temp_csv, index=False, header=False)

    cursor.execute(f"PUT file://{temp_csv} @%{table_name}")
    cursor.execute(f"""
        COPY INTO {full_table_path}
        FROM @%{table_name}
        FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 0)
        ON_ERROR = CONTINUE
    """)

    print("Data loaded successfully.")

    # Clean up temp file
    import os as os_module
    if os_module.path.exists(temp_csv):
        os_module.remove(temp_csv)

except snowflake.connector.errors.ProgrammingError as e:
    print(f"Snowflake Programming Error: {e}")
    print("This error usually indicates an issue with SQL syntax, permissions, or object existence.")
    print("Please double-check your environment variables, Snowflake object names (case sensitivity), and user privileges.")
    print(f"Attempted to load to: {full_table_path}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
finally:
    if conn:
        conn.close()
        print("Snowflake connection closed.")