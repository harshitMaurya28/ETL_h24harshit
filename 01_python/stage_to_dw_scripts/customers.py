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
        UPDATE devdw.Customers B
        SET 
            contactLastName = A.contactLastName,
            contactFirstName = A.contactFirstName,
            phone = A.phone,
            addressLine1 = A.addressLine1,
            addressLine2 = A.addressLine2,
            city = A.city,
            state = A.state,
            postalCode = A.postalCode,
            country = A.country,
            salesRepEmployeeNumber = A.salesRepEmployeeNumber,
            creditLimit = A.creditLimit,
            dw_sales_employee_id = E.dw_employee_id,
            src_update_timestamp = A.update_timestamp,
            dw_update_timestamp = CURRENT_TIMESTAMP,
            etl_batch_no = {etl_batch_no},
            etl_batch_date = '{etl_batch_date}'::DATE
        FROM devstage.Customers A
        LEFT JOIN devdw.Employees E ON A.salesRepEmployeeNumber = E.employeeNumber
        WHERE A.customerNumber = B.src_customerNumber;
        """,
        f"""
        INSERT INTO devdw.Customers
        (
            src_customerNumber,
            customerName,
            contactLastName,
            contactFirstName,
            phone,
            addressLine1,
            addressLine2,
            city,
            state,
            postalCode,
            country,
            salesRepEmployeeNumber,
            creditLimit,
            dw_sales_employee_id,
            src_create_timestamp,
            src_update_timestamp,
            etl_batch_no,
            etl_batch_date
        )
        SELECT 
            A.customerNumber,
            A.customerName,
            A.contactLastName,
            A.contactFirstName,
            A.phone,
            A.addressLine1,
            A.addressLine2,
            A.city,
            A.state,
            A.postalCode,
            A.country,
            A.salesRepEmployeeNumber,
            A.creditLimit,
            E.dw_employee_id,
            A.create_timestamp,
            A.update_timestamp,
            {etl_batch_no},
            '{etl_batch_date}'::DATE
        FROM devstage.Customers A
        LEFT JOIN devdw.Customers B ON A.customerNumber = B.src_customerNumber
        LEFT JOIN devdw.Employees E ON A.salesRepEmployeeNumber = E.employeeNumber
        WHERE B.src_customerNumber IS NULL;
        """
    ]

# Execute the queries
try:
    cursor.execute(transfer_query[0])
    conn.commit()
    cursor.execute(transfer_query[1])
    conn.commit()
    print("Customers Data transferred successfully from devstage to devdw schema.")
except Exception as e:
    print(f"Error transferring data: {e}")
    conn.rollback()
finally:
    cursor.close()
    conn.close()
