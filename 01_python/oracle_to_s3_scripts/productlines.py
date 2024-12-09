import os
import oracledb
import boto3
import botocore
from datetime import datetime
from dotenv import load_dotenv
# import psycopg2
import csv
from io import StringIO
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from function.fetch_date import *

# Load environment variables from .env file
load_dotenv()

#oracledb.init_oracle_client(lib_dir=os.getenv('ORACLE_INSTANT_CLIENT_PATH'))#oracledb client

# def connect_to_redshift():
#     """
#     Establishes a connection to Amazon Redshift.

#     Returns:
#     - conn: The Redshift connection object, or None if the connection fails.
#     """
#     host = os.environ.get('host')
#     database = os.environ.get('db_name')
#     port = 5439
#     user = os.environ.get('user')
#     password = os.environ.get('passs')

#     try:
#         # Connect to Redshift
#         conn = psycopg2.connect(
#             host=host,
#             dbname=database,
#             port=port,
#             user=user,
#             password=password
#         )
#         print("Connected to Redshift.")
#         return conn
#     except Exception as e:
#         print("Error connecting to Redshift:", e)
#         return None
    

# Oracle Connection Function
def connect_to_oracle():
    dsn = os.environ.get('ORACLE_DSN')
    user = os.environ.get('ORACLE_USER')
    password = os.environ.get('ORACLE_PASSWORD')
    connection=oracledb.connect(user=user, password=password, dsn=dsn)
    return connection


# #Fetch Date Value from redshift
# def fetch_date_value(conn):
#     cursor = conn.cursor()
#     query = """
#     SELECT etl_batch_no, etl_batch_date from metadata.batch_control
#     """
#     cursor.execute(query)
#     result = cursor.fetchone()
    
#     if result:
#         return result[1]  # Returns the batch_control_date as YYYYMMDD
#     else:
#         raise ValueError("No date value found in batch_control table")
    

# Extract Data from Oracle
def extract_data(conn, table_name, columns, date_value):
    cursor = conn.cursor()
    query = f"""
    SELECT {', '.join(columns)}
    FROM {table_name}@harshit_dblink_classicmodels
    WHERE UPDATE_TIMESTAMP >= :date_value
    """
    cursor.execute(query, date_value=date_value)  # Bind the date_value correctly
    return cursor.fetchall()


# Transform Data to CSV
def transform_data(data, columns):
    # Use StringIO to hold CSV data in memory
    output = StringIO()
    writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL)
    
    # Write column headers
    writer.writerow(columns)
    
    # Write rows
    for row in data:
        writer.writerow(row)
    
    # Get CSV content as a string
    return output.getvalue()


# Load CSV Data to S3
def load_to_s3(csv_data, table_name, date_value):
    s3 = boto3.client('s3')
    bucket_name = os.environ.get('S3_BUCKET')
    folder_name = f"{table_name}/{date_value}"
    file_name = f"{table_name}.csv"
    key = f"{folder_name}/{file_name}"

    try:
        s3.head_object(Bucket=bucket_name, Key=key)
        print(f"File {file_name} already exists. Adding timestamp to the new file.")
        timestamp = datetime.now().strftime('%Y-%m-%d')
        file_name = f"{table_name}_{timestamp}.csv"
        key = f"{folder_name}/{file_name}"
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print(f"No existing file found with the name {file_name}. Proceeding without timestamp.")
        else:
            raise

    s3.put_object(Body=csv_data, Bucket=bucket_name, Key=key)
    print(f"File {file_name} uploaded successfully.")


# Usage example:
def main(table_name, columns):
    conn1 = connect_to_redshift()

    conn2 = connect_to_oracle()

    date_value = fetch_date_value(conn1)

    data = extract_data(conn2, table_name, columns, date_value)

    csv_data = transform_data(data, columns)
        
    # Upload data to S3 with the fetched date value
    load_to_s3(csv_data, table_name, date_value)

    # conn1.close()
    conn2.close()


table_name = "productlines"
columns = ["productLine", "create_timestamp", "update_timestamp"]
main(table_name, columns)
