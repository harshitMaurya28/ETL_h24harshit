import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def connect_to_redshift():
    """
    Establishes a connection to Amazon Redshift.

    Returns:
    - conn: The Redshift connection object, or None if the connection fails.
    """
    host = os.environ.get('host')
    database = os.environ.get('db_name')
    port = 5439
    user = os.environ.get('user')
    password = os.environ.get('passs')

    try:
        # Connect to Redshift
        conn = psycopg2.connect(
            host=host,
            dbname=database,
            port=port,
            user=user,
            password=password
        )
        print("Connected to Redshift.")
        return conn
    except Exception as e:
        print("Error connecting to Redshift:", e)
        return None
    

#Fetch Date Value from redshift
def fetch_date_value(conn1):
    cursor = conn1.cursor()
    query = """
    SELECT etl_batch_no, etl_batch_date from etl_metadata.batch_control
    """
    cursor.execute(query)
    result = cursor.fetchone()
    
    if result:
        return result[1]  # Returns the batch_control_date as YYYYMMDD
    else:
        raise ValueError("No date value found in batch_control table")
    

def batch_no(conn1):
    cursor = conn1.cursor()
    query = """
    SELECT etl_batch_no, etl_batch_date from etl_metadata.batch_control
    """
    cursor.execute(query)
    result = cursor.fetchone()
    
    if result:
        return result[0]  # Returns the batch_control_date as YYYYMMDD
    else:
        raise ValueError("No date value found in batch_control table")


conn1 = connect_to_redshift()
fetch_date_value(conn1)
batch_no(conn1)




