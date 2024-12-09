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
        INSERT INTO devdw.daily_product_summary
        (
            summary_date,
            dw_product_id,
            customer_apd,
            product_cost_amount,
            product_mrp_amount,
            cancelled_product_qty,
            cancelled_cost_amount,
            cancelled_mrp_amount,
            cancelled_order_apd,
            dw_create_timestamp,
            etl_batch_no,
            etl_batch_date
        )
        WITH CTE AS
        (
            -- Orders data (non-cancelled orders)
            SELECT CAST(o.orderDate AS DATE) AS summary_date,
                p.dw_product_id,
                1 AS customer_apd,
                SUM(od.quantityOrdered * od.priceEach) AS product_cost_amount,
                SUM(od.quantityOrdered * p.MSRP) AS product_mrp_amount,
                0 AS cancelled_product_qty,
                0 AS cancelled_cost_amount,
                0 AS cancelled_mrp_amount,
                0 AS cancelled_order_apd
            FROM devdw.Products p
            JOIN devdw.OrderDetails od ON p.dw_product_id = od.dw_product_id
            JOIN devdw.Orders o ON od.dw_order_id = o.dw_order_id
            WHERE o.orderDate >= '{etl_batch_date}'::DATE -- etl_batch_date parameter
            GROUP BY CAST(o.orderDate AS DATE),
                    p.dw_product_id

            UNION ALL

            -- Cancelled orders data
            SELECT CAST(o.cancelledDate AS DATE) AS summary_date,
                p.dw_product_id,
                1 AS customer_apd,
                0 AS product_cost_amount,
                0 AS product_mrp_amount,
                COUNT(DISTINCT o.dw_order_id) AS cancelled_product_qty,
                SUM(od.quantityOrdered * od.priceEach) AS cancelled_cost_amount,
                SUM(od.quantityOrdered * p.MSRP) AS cancelled_mrp_amount,
                1 AS cancelled_order_apd
            FROM devdw.Products p
            JOIN devdw.OrderDetails od ON p.dw_product_id = od.dw_product_id
            JOIN devdw.Orders o ON od.dw_order_id = o.dw_order_id
            WHERE o.cancelledDate >= '{etl_batch_date}'::DATE  -- etl_batch_date parameter
            GROUP BY CAST(o.cancelledDate AS DATE),
                    p.dw_product_id
        )
        SELECT summary_date,
            dw_product_id,
            MAX(customer_apd) AS customer_apd,
            MAX(product_cost_amount) AS product_cost_amount,
            MAX(product_mrp_amount) AS product_mrp_amount,
            MAX(cancelled_product_qty) AS cancelled_product_qty,
            MAX(cancelled_cost_amount) AS cancelled_cost_amount,
            MAX(cancelled_mrp_amount) AS cancelled_mrp_amount,
            MAX(cancelled_order_apd) AS cancelled_order_apd,
            CURRENT_TIMESTAMP AS dw_create_timestamp,
            {etl_batch_no} AS etl_batch_no,  -- etl_batch_no parameter
            '{etl_batch_date}'::DATE AS etl_batch_date  -- etl_batch_date parameter
        FROM CTE
        GROUP BY summary_date,
                dw_product_id;
        """
    ]

# Execute the queries
try:
    cursor.execute(transfer_query[0])
    conn.commit()

    print("DPS Data transferred successfully from devstage to devdw schema.")
except Exception as e:
    print(f"Error transferring data: {e}")
    conn.rollback()
finally:
    cursor.close()
    conn.close()
