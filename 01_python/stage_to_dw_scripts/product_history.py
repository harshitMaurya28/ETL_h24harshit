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
        UPDATE devdw.product_history ph
        SET 
            dw_active_record_ind = 0,
            effective_to_date = '{etl_batch_date}'::DATE - INTERVAL '1 day',  -- Adjusting date calculation for PostgreSQL
            update_etl_batch_no = {etl_batch_no},  -- for etl_batch_no
            update_etl_batch_date = '{etl_batch_date}'::DATE,  -- for etl_batch_date
            dw_update_timestamp = CURRENT_TIMESTAMP
        FROM devdw.Products P
        WHERE ph.dw_product_id = P.dw_product_id
        AND ph.dw_active_record_ind = 1
        AND P.MSRP <> ph.MSRP;
        """,
        f"""
        INSERT INTO devdw.product_history
        (
            dw_product_id,
            MSRP,
            effective_from_date,
            dw_active_record_ind,
            create_etl_batch_no,
            create_etl_batch_date
        )
        SELECT 
            P.dw_product_id,
            P.MSRP,
            '{etl_batch_date}'::DATE,  -- for etl_batch_date
            1,  -- dw_active_record_ind is always 1 for new records
            {etl_batch_no},  -- for etl_batch_no
            '{etl_batch_date}'::DATE   -- for etl_batch_date
        FROM devdw.Products P
        LEFT JOIN devdw.product_history ph
            ON P.dw_product_id = ph.dw_product_id 
            AND ph.dw_active_record_ind = 1
        WHERE ph.dw_product_id IS NULL;
        """
    ]

# Execute the queries
try:
    cursor.execute(transfer_query[0])
    conn.commit()
    cursor.execute(transfer_query[1])
    conn.commit()
    print("Product_history Data transferred successfully from devstage to devdw schema.")
except Exception as e:
    print(f"Error transferring data: {e}")
    conn.rollback()
finally:
    cursor.close()
    conn.close()
