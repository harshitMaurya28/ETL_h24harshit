import os
import sys
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

IAM_ROLE = os.environ.get('IAM_role')
etl_batch_date = fetch_date_value(conn1)
print("Date Value:", etl_batch_date)

#query to copy data 
query = f"""
COPY harshit_db.devstage.offices (officeCode, city, phone, addressLine1, addressLine2, state, country, postalCode, territory, create_timestamp, update_timestamp)
FROM 's3://etl-bucket-hk/offices/{etl_batch_date}/offices.csv' 
IAM_ROLE '{IAM_ROLE}'
FORMAT AS CSV DELIMITER ',' DATEFORMAT 'auto' QUOTE '"' IGNOREHEADER 1 REGION AS 'eu-north-1'
"""

# Execute the COPY command
try:
    cursor.execute('truncate harshit_db.devstage.offices;')
    cursor.execute(query)
    conn.commit()
    print(f"Offices {etl_batch_date} Data loaded successfully from S3 to Redshift.")
except Exception as e:
    print(f"Error loading data: {e}")
    conn.rollback()
finally:
    cursor.close()
    conn.close()

