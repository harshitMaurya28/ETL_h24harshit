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
        WITH CTE AS
(
  SELECT TO_CHAR(dps.summary_date, 'YYYY-MM-01')::DATE AS start_of_the_month_date, -- Date formatting in PostgreSQL
         dps.dw_product_id,
         SUM(dps.customer_apd) AS customer_apd,
         CASE WHEN MAX(dps.customer_apd) > 0 THEN 1 ELSE 0 END AS customer_apm,
         SUM(dps.product_cost_amount) AS product_cost_amount,
         SUM(dps.product_mrp_amount) AS product_mrp_amount,
         SUM(dps.cancelled_product_qty) AS cancelled_product_qty,
         SUM(dps.cancelled_cost_amount) AS cancelled_cost_amount,
         SUM(dps.cancelled_mrp_amount) AS cancelled_mrp_amount,
         SUM(dps.cancelled_order_apd) AS cancelled_order_apd,
         CASE WHEN MAX(dps.cancelled_order_apd) > 0 THEN 1 ELSE 0 END AS cancelled_order_apm
  FROM devdw.daily_product_summary dps
  WHERE dps.summary_date >= '{etl_batch_date}'::DATE  -- Replaced with parameter placeholder
  GROUP BY TO_CHAR(dps.summary_date, 'YYYY-MM-01')::DATE, dps.dw_product_id
) 
UPDATE devdw.monthly_product_summary
SET
    customer_apd = mps.customer_apd + c.customer_apd,
    customer_apm = (mps.customer_apm::int | c.customer_apm::int),  -- Bitwise OR, casting booleans to integers
    product_cost_amount = mps.product_cost_amount + c.product_cost_amount,
    product_mrp_amount = mps.product_mrp_amount + c.product_mrp_amount,
    cancelled_product_qty = mps.cancelled_product_qty + c.cancelled_product_qty,
    cancelled_cost_amount = mps.cancelled_cost_amount + c.cancelled_cost_amount,
    cancelled_mrp_amount = mps.cancelled_mrp_amount + c.cancelled_mrp_amount,
    cancelled_order_apd = mps.cancelled_order_apd + c.cancelled_order_apd,
    cancelled_order_apm = (mps.cancelled_order_apm::int | c.cancelled_order_apm::int),  -- Bitwise OR
    dw_update_timestamp = CURRENT_TIMESTAMP,
    etl_batch_no = {etl_batch_no},  -- Replaced with parameter placeholder
    etl_batch_date = '{etl_batch_date}'::DATE  -- Replaced with parameter placeholder
FROM CTE c
JOIN devdw.monthly_product_summary mps
ON mps.start_of_the_month_date = c.start_of_the_month_date
  AND mps.dw_product_id = c.dw_product_id;
""",
f"""
-- Insert new records into the monthly_product_summary table
INSERT INTO devdw.monthly_product_summary (
   start_of_the_month_date,
   dw_product_id,
   customer_apd,
   customer_apm, 
   product_cost_amount,
   product_mrp_amount,
   cancelled_product_qty,
   cancelled_cost_amount,
   cancelled_mrp_amount,
   cancelled_order_apd,
   cancelled_order_apm,
   etl_batch_no,
   etl_batch_date
)
SELECT
   TO_CHAR(dps.summary_date, 'YYYY-MM-01')::DATE AS start_of_the_month_date, -- Date formatting in PostgreSQL
   dps.dw_product_id, 
   SUM(dps.customer_apd),
   CASE WHEN MAX(dps.customer_apd) > 0 THEN 1 ELSE 0 END AS customer_apm,
   SUM(dps.product_cost_amount),
   SUM(dps.product_mrp_amount),
   SUM(dps.cancelled_product_qty),
   SUM(dps.cancelled_cost_amount),
   SUM(dps.cancelled_mrp_amount),
   SUM(dps.cancelled_order_apd),
   CASE WHEN MAX(dps.cancelled_order_apd) > 0 THEN 1 ELSE 0 END AS cancelled_order_apm,
   {etl_batch_no},  -- Replaced with parameter placeholder
   '{etl_batch_date}'::DATE   -- Replaced with parameter placeholder
FROM devdw.daily_product_summary dps
LEFT JOIN devdw.monthly_product_summary mps
  ON TO_CHAR(dps.summary_date, 'YYYY-MM-01')::DATE = mps.start_of_the_month_date
  AND dps.dw_product_id = mps.dw_product_id
WHERE mps.dw_product_id IS NULL
GROUP BY TO_CHAR(dps.summary_date, 'YYYY-MM-01')::DATE, dps.dw_product_id;
"""
    ]

# Execute the queries
try:
    cursor.execute(transfer_query[0])
    conn.commit()
    cursor.execute(transfer_query[1])
    conn.commit()
    print("MPS Data transferred successfully from devstage to devdw schema.")
except Exception as e:
    print(f"Error transferring data: {e}")
    conn.rollback()
finally:
    cursor.close()
    conn.close()
