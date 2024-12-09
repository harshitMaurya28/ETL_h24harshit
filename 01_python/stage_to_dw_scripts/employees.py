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
        UPDATE devdw.Employees
        SET
            lastName = A.lastName,
            firstName = A.firstName,
            extension = A.extension,
            email = A.email,
            officeCode = A.officeCode,
            reportsTo = A.reportsTo,
            jobTitle = A.jobTitle,
            dw_office_id = O.dw_office_id,
            src_update_timestamp = A.update_timestamp,
            dw_update_timestamp = CURRENT_TIMESTAMP,
            etl_batch_no = {etl_batch_no},  
            etl_batch_date = '{etl_batch_date}'::DATE  
        FROM devdw.Employees AS B 
        JOIN devstage.Employees AS A ON A.employeeNumber = B.employeeNumber
        JOIN devdw.Offices AS O ON O.officeCode = A.officeCode
        WHERE A.employeeNumber = B.employeeNumber;
        """,
        f"""
        INSERT INTO devdw."Employees"
        (
            employeeNumber,
            lastName,
            firstName,
            extension,
            email,
            officeCode,
            reportsTo,
            jobTitle,
            dw_office_id,
            src_create_timestamp,
            src_update_timestamp,
            etl_batch_no,
            etl_batch_date
        )
        SELECT
            A.employeeNumber,
            A.lastName,
            A.firstName,
            A.extension,
            A.email,
            A.officeCode,
            A.reportsTo,
            A.jobTitle,
            O.dw_office_id,
            A.create_timestamp,
            A.update_timestamp,
            {etl_batch_no}, 
            '{etl_batch_date}'::DATE 
        FROM devstage."Employees" A
        LEFT JOIN devdw."Employees" B ON A.employeeNumber = B.employeeNumber
        JOIN devdw."Offices" O ON A.officeCode = O.officeCode
        WHERE B.employeeNumber IS NULL;
        """,
        f"""
        UPDATE devdw."Employees" AS dw1
        SET
            dw_reporting_employee_id = dw2.dw_employee_id
        FROM devdw."Employees" AS dw2
        WHERE dw1.reportsTo = dw2.employeeNumber;
        """

    ]

# Execute the queries
try:
    cursor.execute(transfer_query[0])
    conn.commit()
    cursor.execute(transfer_query[1])
    conn.commit()
    cursor.execute(transfer_query[2])
    conn.commit()
    print("Employees Data transferred successfully from devstage to devdw schema.")
except Exception as e:
    print(f"Error transferring data: {e}")
    conn.rollback()
finally:
    cursor.close()
    conn.close()
