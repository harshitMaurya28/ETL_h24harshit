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
        UPDATE devdw.Products B
        SET 
            productName = A.productName,
            productLine = A.productLine,
            productScale = A.productScale,
            productVendor = A.productVendor,
            quantityInStock = A.quantityInStock,
            buyPrice = A.buyPrice,
            MSRP = A.MSRP,
            dw_product_line_id = PL.dw_product_line_id,
            src_update_timestamp = A.update_timestamp,
            dw_update_timestamp = CURRENT_TIMESTAMP,
            etl_batch_no = {etl_batch_no},  -- for etl_batch_no
            etl_batch_date = '{etl_batch_date}'::DATE   -- for etl_batch_date
        FROM devstage.Products A
        JOIN devdw.ProductLines PL ON A.productLine = PL.productLine
        WHERE A.productCode = B.src_productCode;
        """,
        f"""
        INSERT INTO devdw.Products
        (
            src_productCode,
            productName,
            productLine,
            productScale,
            productVendor,
            quantityInStock,
            buyPrice,
            MSRP,
            dw_product_line_id,
            src_create_timestamp,
            src_update_timestamp,
            etl_batch_no,
            etl_batch_date
        )
        SELECT 
            A.productCode,
            A.productName,
            A.productLine,
            A.productScale,
            A.productVendor,
            A.quantityInStock,
            A.buyPrice,
            A.MSRP,
            PL.dw_product_line_id,
            A.create_timestamp,
            A.update_timestamp,
            {etl_batch_no},  -- for etl_batch_no
            '{etl_batch_date}'::DATE    -- for etl_batch_date
        FROM devstage.Products A
        LEFT JOIN devdw.Products B ON A.productCode = B.src_productCode
        JOIN devdw.ProductLines PL ON A.productLine = PL.productLine
        WHERE B.src_productCode IS NULL;
        """
    ]

# Execute the queries
try:
    cursor.execute(transfer_query[0])
    conn.commit()
    cursor.execute(transfer_query[1])
    conn.commit()
    print("Products Data transferred successfully from devstage to devdw schema.")
except Exception as e:
    print(f"Error transferring data: {e}")
    conn.rollback()
finally:
    cursor.close()
    conn.close()
