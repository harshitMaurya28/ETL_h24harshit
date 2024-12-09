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
        UPDATE devdw.OrderDetails B
        SET 
            dw_order_id = O.dw_order_id,
            dw_product_id = P.dw_product_id,
            src_orderNumber = A.orderNumber,
            src_productCode = A.productCode,
            quantityOrdered = A.quantityOrdered,
            priceEach = A.priceEach,
            orderLineNumber = A.orderLineNumber,
            src_create_timestamp = A.create_timestamp,
            src_update_timestamp = A.update_timestamp,
            dw_update_timestamp = CURRENT_TIMESTAMP,  -- PostgreSQL doesn't need parentheses
            etl_batch_no = {etl_batch_no},
            etl_batch_date = '{etl_batch_date}'::DATE  
        FROM devstage.OrderDetails A
        JOIN devdw.Products P ON A.productCode = P.src_productCode
        JOIN devdw.Orders O ON A.orderNumber = O.src_orderNumber
        WHERE A.orderNumber = B.src_orderNumber
        AND A.productCode = B.src_productCode;
        """,
        f"""
        INSERT INTO devdw.OrderDetails
        (
        dw_order_id,
        dw_product_id,
        src_orderNumber,
        src_productCode,
        quantityOrdered,
        priceEach,
        orderLineNumber,
        src_create_timestamp,
        src_update_timestamp,
        etl_batch_no,
        etl_batch_date
        )
        SELECT O.dw_order_id,
            P.dw_product_id,
            A.orderNumber,
            A.productCode,
            A.quantityOrdered,
            A.priceEach,
            A.orderLineNumber,
            A.create_timestamp,
            A.update_timestamp,
            {etl_batch_no},  
            '{etl_batch_date}'::DATE   
        FROM devstage.OrderDetails A
        LEFT JOIN devdw.OrderDetails B
        ON A.orderNumber = B.src_orderNumber
        AND A.productCode = B.src_productCode
        JOIN devdw.Products P ON A.productCode = P.src_productCode
        JOIN devdw.Orders O ON A.orderNumber = O.src_orderNumber
        WHERE B.src_orderNumber IS NULL;
        """
    ]

# Execute the queries
try:
    cursor.execute(transfer_query[0])
    conn.commit()
    cursor.execute(transfer_query[1])
    conn.commit()
    print("Orderdetails Data transferred successfully from devstage to devdw schema.")
except Exception as e:
    print(f"Error transferring data: {e}")
    conn.rollback()
finally:
    cursor.close()
    conn.close()
