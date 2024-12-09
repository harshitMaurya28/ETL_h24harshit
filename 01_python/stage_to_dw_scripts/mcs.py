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
WITH CTE AS (
  SELECT TO_CHAR(dcs.summary_date, 'YYYY-MM-01')::DATE AS start_of_the_month_date,
         dcs.dw_customer_id,
         SUM(dcs.order_count) AS order_count,
         SUM(dcs.order_apd) AS order_apd,
         CASE
           WHEN SUM(dcs.order_apd) > 0 THEN 1
           ELSE 0
         END AS order_apm,
         SUM(dcs.order_amount) AS order_amount,
         SUM(dcs.order_cost_amount) AS order_cost_amount,
         SUM(dcs.order_mrp_amount) AS order_mrp_amount,
         SUM(dcs.products_ordered_qty) AS products_ordered_qty,
         SUM(dcs.products_items_qty) AS products_items_qty,
         SUM(dcs.cancelled_order_count) AS cancelled_order_count,
         SUM(dcs.cancelled_order_amount) AS cancelled_order_amount,
         SUM(dcs.cancelled_order_apd) AS cancelled_order_apd,
         CASE
           WHEN SUM(dcs.cancelled_order_apd) > 0 THEN 1
           ELSE 0
         END AS cancelled_order_apm,
         SUM(dcs.shipped_order_count) AS shipped_order_count,
         SUM(dcs.shipped_order_amount) AS shipped_order_amount,
         SUM(dcs.shipped_order_apd) AS shipped_order_apd,
         CASE
           WHEN SUM(dcs.shipped_order_apd) > 0 THEN 1
           ELSE 0
         END AS shipped_order_apm,
         SUM(dcs.payment_apd) AS payment_apd,
         CASE
           WHEN SUM(dcs.payment_apd) > 0 THEN 1
           ELSE 0
         END AS payment_apm,
         SUM(dcs.payment_amount) AS payment_amount,
         SUM(dcs.new_customer_apd) AS new_customer_apd,
         CASE
           WHEN SUM(dcs.new_customer_apd) > 0 THEN 1
           ELSE 0
         END AS new_customer_apm
  FROM devdw.daily_customer_summary dcs
  WHERE dcs.summary_date >= '{etl_batch_date}'::DATE  -- etl_batch_date parameter
  GROUP BY TO_CHAR(dcs.summary_date, 'YYYY-MM-01')::DATE, dcs.dw_customer_id
) 
UPDATE devdw.monthly_customer_summary
SET 
    order_count = mcs.order_count + c.order_count,
    order_apd = mcs.order_apd + c.order_apd,
    order_apm = (mcs.order_apm::int | c.order_apm::int),  -- bitwise OR
    order_amount = mcs.order_amount + c.order_amount,
    order_cost_amount = mcs.order_cost_amount + c.order_cost_amount,
    order_mrp_amount = mcs.order_mrp_amount + c.order_mrp_amount,
    products_ordered_qty = mcs.products_ordered_qty + c.products_ordered_qty,
    products_items_qty = mcs.products_items_qty + c.products_items_qty,
    cancelled_order_count = mcs.cancelled_order_count + c.cancelled_order_count,
    cancelled_order_amount = mcs.cancelled_order_amount + c.cancelled_order_amount,
    cancelled_order_apd = mcs.cancelled_order_apd + c.cancelled_order_apd,
    cancelled_order_apm = (mcs.cancelled_order_apm::int | c.cancelled_order_apm::int),  -- bitwise OR
    shipped_order_count = mcs.shipped_order_count + c.shipped_order_count,
    shipped_order_amount = mcs.shipped_order_amount + c.shipped_order_amount,
    shipped_order_apd = mcs.shipped_order_apd + c.shipped_order_apd,
    shipped_order_apm = (mcs.shipped_order_apm::int | c.shipped_order_apm::int),  -- bitwise OR
    payment_apd = mcs.payment_apd + c.payment_apd,
    payment_apm = (mcs.payment_apm::int | c.payment_apm::int),  -- bitwise OR
    payment_amount = mcs.payment_amount + c.payment_amount,
    new_customer_apd = mcs.new_customer_apd + c.new_customer_apd,
    new_customer_apm = (mcs.new_customer_apm::int | c.new_customer_apm::int),  -- bitwise OR
    dw_update_timestamp = CURRENT_TIMESTAMP,
    etl_batch_no = {etl_batch_no},  -- etl_batch_no parameter
    etl_batch_date = '{etl_batch_date}'::DATE  -- etl_batch_date parameter
FROM CTE c
JOIN devdw.monthly_customer_summary mcs
ON mcs.start_of_the_month_date = c.start_of_the_month_date
  AND mcs.dw_customer_id = c.dw_customer_id;
        """,
        f"""
        -- Insert new records into the monthly_customer_summary table
        INSERT INTO devdw.monthly_customer_summary (
            start_of_the_month_date,
            dw_customer_id,
            order_count,
            order_apd,
            order_apm,
            order_amount,
            order_cost_amount,
            order_mrp_amount,
            products_ordered_qty,
            products_items_qty,
            cancelled_order_count,
            cancelled_order_amount,
            cancelled_order_apd,
            cancelled_order_apm,
            shipped_order_count,
            shipped_order_amount,
            shipped_order_apd,
            shipped_order_apm,
            payment_apd,
            payment_apm,
            payment_amount,
            new_customer_apd,
            new_customer_apm,
            new_customer_paid_apd,
            new_customer_paid_apm,
            etl_batch_no,
            etl_batch_date
        )
        SELECT
            TO_CHAR(dcs.summary_date, 'YYYY-MM-01')::DATE AS start_of_the_month_date,
            dcs.dw_customer_id, 
            SUM(dcs.order_count),
            SUM(dcs.order_apd),
            CASE WHEN SUM(dcs.order_apd) > 0 THEN 1 ELSE 0 END AS order_apm,
            SUM(dcs.order_amount),
            SUM(dcs.order_cost_amount),
            SUM(dcs.order_mrp_amount),
            SUM(dcs.products_ordered_qty),
            SUM(dcs.products_items_qty),
            SUM(dcs.cancelled_order_count),
            SUM(dcs.cancelled_order_amount),
            SUM(dcs.cancelled_order_apd),
            CASE WHEN SUM(dcs.cancelled_order_apd) > 0 THEN 1 ELSE 0 END AS cancelled_order_apm,
            SUM(dcs.shipped_order_count),
            SUM(dcs.shipped_order_amount),
            SUM(dcs.shipped_order_apd),
            CASE WHEN SUM(dcs.shipped_order_apd) > 0 THEN 1 ELSE 0 END AS shipped_order_apm,
            SUM(dcs.payment_apd),
            CASE WHEN SUM(dcs.payment_apd) > 0 THEN 1 ELSE 0 END AS payment_apm,
            SUM(dcs.payment_amount),
            SUM(dcs.new_customer_apd),
            CASE WHEN SUM(dcs.new_customer_apd) > 0 THEN 1 ELSE 0 END AS new_customer_apm,
            0 AS new_customer_paid_apd,
            0 AS new_customer_paid_apm,
            {etl_batch_no},  -- etl_batch_no parameter
            '{etl_batch_date}'::DATE   -- etl_batch_date parameter
        FROM devdw.daily_customer_summary dcs
        LEFT JOIN devdw.monthly_customer_summary mcs
            ON TO_CHAR(dcs.summary_date, 'YYYY-MM-01')::DATE = mcs.start_of_the_month_date
            AND dcs.dw_customer_id = mcs.dw_customer_id
        WHERE mcs.dw_customer_id IS NULL
        GROUP BY TO_CHAR(dcs.summary_date, 'YYYY-MM-01')::DATE, dcs.dw_customer_id;
        """
    ]

# Execute the queries
try:
    cursor.execute(transfer_query[0])
    conn.commit()
    cursor.execute(transfer_query[1])
    conn.commit()
    print("MCS Data transferred successfully from devstage to devdw schema.")
except Exception as e:
    print(f"Error transferring data: {e}")
    conn.rollback()
finally:
    cursor.close()
    conn.close()
