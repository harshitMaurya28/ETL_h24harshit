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
        UPDATE devdw.Orders B
        SET 
            requiredDate = A.requiredDate,
            cancelledDate = A.cancelledDate,
            shippedDate = A.shippedDate,
            status = A.status,
            src_update_timestamp = A.update_timestamp,
            dw_update_timestamp = CURRENT_TIMESTAMP,
            etl_batch_no = {etl_batch_no},  -- for etl_batch_no
            etl_batch_date = '{etl_batch_date}'::DATE   -- for etl_batch_date
        FROM devstage.Orders A
        WHERE A.orderNumber = B.src_orderNumber;
        """,
        f"""
        INSERT INTO devdw.Orders
        (
            dw_customer_id,
            src_orderNumber,
            orderDate,
            requiredDate,
            cancelledDate,
            shippedDate,
            status,
            src_customerNumber,
            src_create_timestamp,
            src_update_timestamp,
            etl_batch_no,
            etl_batch_date
        )
        SELECT 
            C.dw_customer_id,
            A.orderNumber,
            A.orderDate,
            A.requiredDate,
            A.cancelledDate,
            A.shippedDate,
            A.status,
            A.customerNumber,
            A.create_timestamp,
            A.update_timestamp,
            {etl_batch_no},  -- for etl_batch_no
            '{etl_batch_date}'::DATE    -- for etl_batch_date
        FROM devstage.Orders A
        LEFT JOIN devdw.Orders B ON A.orderNumber = B.src_orderNumber
        JOIN devdw.Customers C ON A.customerNumber = C.src_customerNumber
        WHERE B.src_orderNumber IS NULL;
        """
    ]

# Execute the queries
try:
    cursor.execute(transfer_query[0])
    conn.commit()
    cursor.execute(transfer_query[1])
    conn.commit()
    print("Orders Data transferred successfully from devstage to devdw schema.")
except Exception as e:
    print(f"Error transferring data: {e}")
    conn.rollback()
finally:
    cursor.close()
    conn.close()
