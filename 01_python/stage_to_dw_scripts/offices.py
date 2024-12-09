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
transfer_query = [f"""
        UPDATE devdw.Offices AS B
        SET
            city = A.city,
            phone = A.phone,
            addressLine1 = A.addressLine1,
            addressLine2 = A.addressLine2,
            state = A.state,
            country = A.country,
            postalCode = A.postalCode,
            territory = A.territory,
            src_update_timestamp = A.update_timestamp,
            dw_update_timestamp = CURRENT_TIMESTAMP,  
            etl_batch_no = {etl_batch_no},  
            etl_batch_date = '{etl_batch_date}'::DATE
        FROM devstage.Offices AS A
        WHERE A.officeCode = B.officeCode;
        """,
        f"""
        INSERT INTO devdw.Offices
        (
            officeCode,
            city,
            phone,
            addressLine1,
            addressLine2,
            state,
            country,
            postalCode,
            territory,
            src_create_timestamp,
            src_update_timestamp,
            etl_batch_no,
            etl_batch_date
        )
        SELECT
            A.officeCode,
            A.city,
            A.phone,
            A.addressLine1,
            A.addressLine2,
            A.state,
            A.country,
            A.postalCode,
            A.territory,
            A.create_timestamp,
            A.update_timestamp,
            {etl_batch_no}, 
            '{etl_batch_date}'::DATE 
        FROM devstage.Offices A
        LEFT JOIN devdw.Offices B ON A.officeCode = B.officeCode
        WHERE B.officeCode IS NULL;
        """]

# Execute the queries
try:
    cursor.execute(transfer_query[0])
    conn.commit()
    cursor.execute(transfer_query[1])
    conn.commit()
    print("Offices Data transferred successfully from devstage to devdw schema.")
except Exception as e:
    print(f"Error transferring data: {e}")
    conn.rollback()
finally:
    cursor.close()
    conn.close()
