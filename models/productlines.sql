{{ config(
    materialized='incremental',
    unique_key='productline'
) }}

WITH ranked_data AS (
    SELECT
        sd.productline,
        coalesce(ed.dw_product_line_id, ROW_NUMBER() OVER (ORDER BY sd.productline) + COALESCE(MAX(ed.dw_product_line_id) OVER (), 0)) AS dw_product_line_id,
        CASE
            WHEN sd.productline IS NOT NULL AND ed.productline IS NULL THEN sd.create_timestamp
            ELSE ed.src_create_timestamp
        END AS src_create_timestamp,
        COALESCE(sd.update_timestamp, ed.src_update_timestamp) AS src_update_timestamp,
        em.etl_batch_no,
        em.etl_batch_date,
        CASE
            WHEN sd.productline IS NOT NULL THEN CURRENT_TIMESTAMP
            ELSE ed.dw_update_timestamp
        END AS dw_update_timestamp,
        CURRENT_TIMESTAMP AS dw_create_timestamp
    FROM
        devstage.productlines sd
    LEFT JOIN devdw.productlines ed ON sd.productline = ed.productline
    CROSS JOIN etl_metadata.batch_control em
)

SELECT * FROM ranked_data

{% if is_incremental() %}
WHERE
    ranked_data.productline IS NOT NULL  -- Only process new or updated rows
{% endif %}

