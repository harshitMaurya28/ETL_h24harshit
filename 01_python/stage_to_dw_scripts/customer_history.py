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
        UPDATE devdw.customer_history ch
        SET 
            dw_active_record_ind = 0,
            effective_to_date = '{etl_batch_date}'::DATE - INTERVAL '1 day',  -- for etl_batch_date, adjusted for PostgreSQL
            update_etl_batch_no = {etl_batch_no},  -- for etl_batch_no
            update_etl_batch_date = '{etl_batch_date}'::DATE,  -- for etl_batch_date
            dw_update_timestamp = CURRENT_TIMESTAMP
        FROM devdw.Customers C
        WHERE ch.dw_customer_id = C.dw_customer_id
        AND ch.dw_active_record_ind = 1
        AND C.creditLimit <> ch.creditLimit;
        """,
        f"""
        INSERT INTO devdw.customer_history 
        (
            dw_customer_id,
            creditLimit,
            effective_from_date,
            dw_active_record_ind,
            create_etl_batch_no,
            create_etl_batch_date
        )
        SELECT 
            C.dw_customer_id,
            C.creditLimit,
            '{etl_batch_date}'::DATE,  -- for etl_batch_date
            1,  -- for dw_active_record_ind
            {etl_batch_no},  -- for etl_batch_no
            '{etl_batch_date}'::DATE   -- for etl_batch_date
        FROM devdw.Customers C
        LEFT JOIN devdw.customer_history ch
            ON C.dw_customer_id = ch.dw_customer_id 
            AND ch.dw_active_record_ind = 1
        WHERE ch.dw_customer_id IS NULL;
        """
    ]

# Execute the queries
try:
    cursor.execute(transfer_query[0])
    conn.commit()
    cursor.execute(transfer_query[1])
    conn.commit()
    print("Customer_history Data transferred successfully from devstage to devdw schema.")
except Exception as e:
    print(f"Error transferring data: {e}")
    conn.rollback()
finally:
    cursor.close()
    conn.close()
