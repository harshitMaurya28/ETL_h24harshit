import os
from dotenv import load_dotenv
import redshift_connector
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


etl_batch_no = batch_no(conn)
etl_batch_date = fetch_date_value(conn)


# Execute the start batch query 
def execute_end_batch(redshift_conn, etl_batch_no, etl_batch_date):
    query = f"""
            UPDATE etl_metadata.batch_control_log
            SET etl_batch_status = 'C', etl_batch_end_time = CURRENT_TIMESTAMP
            WHERE etl_batch_no = {etl_batch_no};
            """
    
    cursor = redshift_conn.cursor()

    try:
        cursor.execute(query)
        redshift_conn.commit()
        print(f"Batch Control Log updated successfully for ETL Batch No: {etl_batch_no}")
    except Exception as e:
        print(f"An error occurred while executing the end batch query: {e}")
    finally:
        cursor.close()


try:
    execute_end_batch(conn, etl_batch_no, etl_batch_date)
except Exception as e:
    print(f"An error occurred while updating the Batch Control Log table: {e}")
finally:
    conn.close()