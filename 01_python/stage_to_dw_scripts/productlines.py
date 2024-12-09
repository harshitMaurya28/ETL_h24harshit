import os
import redshift_connector
from dotenv import load_dotenv
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from function.fetch_date import *

load_dotenv()

# Connect to Redshift
conn = redshift_connector.connect(
    host=os.environ.get('host'),
    database=os.environ.get('db_name'),      
    user=os.environ.get('user'),                
    password=os.environ.get('passs'),         
    port=5439
)

cursor = conn.cursor()

# Fetch the date for the ETL batch, if applicable to this operation
etl_batch_no = batch_no(conn)
print("Batch_number: ", etl_batch_no)
etl_batch_date = fetch_date_value(conn)
print("Date Value:", etl_batch_date)


# SQL statement to insert data from devstage to devdw schema
transfer_query = [
        f"""
        UPDATE devdw.ProductLines B
        SET 
            src_update_timestamp = A.update_timestamp,
            dw_update_timestamp = CURRENT_TIMESTAMP,
            etl_batch_no = {etl_batch_no},  -- for etl_batch_no
            etl_batch_date = '{etl_batch_date}'::DATE   -- for etl_batch_date
        FROM devstage.ProductLines A
        WHERE A.productLine = B.productLine;
        """,
        f"""
        INSERT INTO devdw.ProductLines
        (
            productLine,
            src_create_timestamp,
            src_update_timestamp,
            etl_batch_no,
            etl_batch_date
        )
        SELECT 
            A.productLine,
            A.create_timestamp,
            A.update_timestamp,
            {etl_batch_no},  -- for etl_batch_no
            '{etl_batch_date}'::DATE    -- for etl_batch_date
        FROM devstage.ProductLines A
        LEFT JOIN devdw.ProductLines B ON A.productLine = B.productLine
        WHERE B.productLine IS NULL;
        """
    ]

# Execute the queries
try:
    cursor.execute(transfer_query[0])
    conn.commit()
    cursor.execute(transfer_query[1])
    conn.commit()
    print("Productlines Data transferred successfully from devstage to devdw schema.")
except Exception as e:
    print(f"Error transferring data: {e}")
    conn.rollback()
finally:
    cursor.close()
    conn.close()
