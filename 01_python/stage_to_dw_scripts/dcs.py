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
        INSERT INTO devdw.daily_customer_summary
        (
            summary_date,
            dw_customer_id,
            order_count,
            order_apd,
            order_amount,
            order_cost_amount,
            order_mrp_amount,
            products_ordered_qty,
            products_items_qty,
            cancelled_order_count,
            cancelled_order_amount,
            cancelled_order_apd,
            shipped_order_count,
            shipped_order_amount,
            shipped_order_apd,
            payment_apd,
            payment_amount,
            new_customer_apd,
            new_customer_paid_apd,
            create_timestamp,
            etl_batch_no,
            etl_batch_date
        )
        WITH CTE AS
        (
            -- Orders data
            SELECT CAST(o.orderDate AS DATE) AS summary_date,
                o.dw_customer_id,
                COUNT(DISTINCT o.dw_order_id) AS order_count,
                1 AS order_apd,
                SUM(od.priceEach * od.quantityOrdered) AS order_amount,
                SUM(p.buyPrice * od.quantityOrdered) AS order_cost_amount,
                SUM(p.MSRP * od.quantityOrdered) AS order_mrp_amount,
                COUNT(DISTINCT od.src_productCode) AS products_ordered_qty,
                SUM(od.quantityOrdered) AS products_items_qty,
                0 AS cancelled_order_count,
                0 AS cancelled_order_amount,
                0 AS cancelled_order_apd,
                0 AS shipped_order_count,
                0 AS shipped_order_amount,
                0 AS shipped_order_apd,
                0 AS payment_apd,
                0 AS payment_amount,
                0 AS new_customer_apd,
                0 AS new_customer_paid_apd
            FROM devdw.Orders o
            JOIN devdw.OrderDetails od ON o.dw_order_id = od.dw_order_id
            JOIN devdw.Products p ON od.dw_product_id = p.dw_product_id
            WHERE o.orderDate >= '{etl_batch_date}'::DATE  -- etl_batch_date parameter
            GROUP BY CAST(o.orderDate AS DATE),
                    o.dw_customer_id

            UNION ALL

            -- Cancelled orders data
            SELECT CAST(o.cancelledDate AS DATE) AS summary_date,
                o.dw_customer_id,
                0 AS order_count,
                0 AS order_apd,
                0 AS order_amount,
                0 AS order_cost_amount,
                0 AS order_mrp_amount,
                0 AS products_ordered_qty,
                0 AS products_items_qty,
                COUNT(DISTINCT o.dw_order_id) AS cancelled_order_count,
                SUM(od.priceEach * od.quantityOrdered) AS cancelled_order_amount,
                1 AS cancelled_order_apd,
                0 AS shipped_order_count,
                0 AS shipped_order_amount,
                0 AS shipped_order_apd,
                0 AS payment_apd,
                0 AS payment_amount,
                0 AS new_customer_apd,
                0 AS new_customer_paid_apd
            FROM devdw.Orders o
            JOIN devdw.OrderDetails od ON o.dw_order_id = od.dw_order_id
            WHERE o.cancelledDate >= '{etl_batch_date}'::DATE  -- etl_batch_date parameter
            GROUP BY CAST(o.cancelledDate AS DATE),
                    o.dw_customer_id

            UNION ALL

            -- Shipped orders data
            SELECT CAST(o.shippedDate AS DATE) AS summary_date,
                o.dw_customer_id,
                0 AS order_count,
                0 AS order_apd,
                0 AS order_amount,
                0 AS order_cost_amount,
                0 AS order_mrp_amount,
                0 AS products_ordered_qty,
                0 AS products_items_qty,
                0 AS cancelled_order_count,
                0 AS cancelled_order_amount,
                0 AS cancelled_order_apd,
                COUNT(DISTINCT o.dw_order_id) AS shipped_order_count,
                SUM(od.priceEach * od.quantityOrdered) AS shipped_order_amount,
                1 AS shipped_order_apd,
                0 AS payment_apd,
                0 AS payment_amount,
                0 AS new_customer_apd,
                0 AS new_customer_paid_apd
            FROM devdw.Orders o
            JOIN devdw.OrderDetails od ON o.dw_order_id = od.dw_order_id
            WHERE o.shippedDate >= '{etl_batch_date}'::DATE  -- etl_batch_date parameter
            AND o.status = 'Shipped'
            GROUP BY CAST(o.shippedDate AS DATE),
                    o.dw_customer_id

            UNION ALL

            -- Payments data
            SELECT CAST(p.paymentDate AS DATE) AS summary_date,
                p.dw_customer_id,
                0 AS order_count,
                0 AS order_apd,
                0 AS order_amount,
                0 AS order_cost_amount,
                0 AS order_mrp_amount,
                0 AS products_ordered_qty,
                0 AS products_items_qty,
                0 AS cancelled_order_count,
                0 AS cancelled_order_amount,
                0 AS cancelled_order_apd,
                0 AS shipped_order_count,
                0 AS shipped_order_amount,
                0 AS shipped_order_apd,
                1 AS payment_apd,
                SUM(p.amount) AS payment_amount,
                0 AS new_customer_apd,
                0 AS new_customer_paid_apd
            FROM devdw.Payments p
            WHERE p.paymentDate >= '{etl_batch_date}'::DATE  -- etl_batch_date parameter
            GROUP BY CAST(p.paymentDate AS DATE),
                    p.dw_customer_id

            UNION ALL

            -- New customer data
            SELECT CAST(c.src_create_timestamp AS DATE) AS summary_date,
                c.dw_customer_id,
                0 AS order_count,
                0 AS order_apd,
                0 AS order_amount,
                0 AS order_cost_amount,
                0 AS order_mrp_amount,
                0 AS products_ordered_qty,
                0 AS products_items_qty,
                0 AS cancelled_order_count,
                0 AS cancelled_order_amount,
                0 AS cancelled_order_apd,
                0 AS shipped_order_count,
                0 AS shipped_order_amount,
                0 AS shipped_order_apd,
                0 AS payment_apd,
                0 AS payment_amount,
                1 AS new_customer_apd,
                0 AS new_customer_paid_apd
            FROM devdw.Customers c
            WHERE c.src_create_timestamp >= '{etl_batch_date}'::DATE  -- etl_batch_date parameter
        )
        SELECT summary_date,
            dw_customer_id,
            MAX(order_count) AS order_count,
            MAX(order_apd) AS order_apd,
            MAX(order_amount) AS order_amount,
            MAX(order_cost_amount) AS order_cost_amount,
            MAX(order_mrp_amount) AS order_mrp_amount,
            MAX(products_ordered_qty) AS products_ordered_qty,
            MAX(products_items_qty) AS products_items_qty,
            MAX(cancelled_order_count) AS cancelled_order_count,
            MAX(cancelled_order_amount) AS cancelled_order_amount,
            MAX(cancelled_order_apd) AS cancelled_order_apd,
            MAX(shipped_order_count) AS shipped_order_count,
            MAX(shipped_order_amount) AS shipped_order_amount,
            MAX(shipped_order_apd) AS shipped_order_apd,
            MAX(payment_apd) AS payment_apd,
            MAX(payment_amount) AS payment_amount,
            MAX(new_customer_apd) AS new_customer_apd,
            MAX(new_customer_paid_apd) AS new_customer_paid_apd,
            CURRENT_TIMESTAMP AS create_timestamp,
            {etl_batch_no} AS etl_batch_no,  -- etl_batch_no parameter
            '{etl_batch_date}'::DATE AS etl_batch_date  -- etl_batch_date parameter
        FROM CTE
        GROUP BY summary_date,
                dw_customer_id;
        """
    ]

# Execute the queries
try:
    cursor.execute(transfer_query[0])
    conn.commit()
    print("DCS Data transferred successfully from devstage to devdw schema.")
except Exception as e:
    print(f"Error transferring data: {e}")
    conn.rollback()
finally:
    cursor.close()
    conn.close()
