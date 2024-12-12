{{ config(
    materialized='incremental',
    unique_key='src_productcode'
) }}

WITH ranked_data AS (
    SELECT
        sd.productcode AS src_productcode,
        sd.productname,
        sd.productline,
        sd.productscale,
        sd.productvendor,
        sd.quantityinstock,
        sd.buyprice,
        sd.msrp,
        COALESCE(pl.dw_product_line_id, ed.dw_product_line_id) AS dw_product_line_id,
        CASE
            WHEN ed.src_productcode IS NULL THEN sd.create_timestamp
            ELSE ed.src_create_timestamp
        END AS src_create_timestamp,
        COALESCE(sd.update_timestamp, ed.src_update_timestamp) AS src_update_timestamp,
        em.etl_batch_no,
        em.etl_batch_date,
        CURRENT_TIMESTAMP AS dw_create_timestamp,
        CASE
            WHEN sd.productcode IS NOT NULL THEN CURRENT_TIMESTAMP
            ELSE ed.dw_update_timestamp
        END AS dw_update_timestamp,
        coalesce(dw_product_id,ROW_NUMBER() OVER (ORDER BY sd.productcode) + COALESCE(MAX(ed.dw_product_id) OVER (), 0)) AS dw_product_id
    FROM
        devstage.products sd
    LEFT JOIN devdw.products ed ON sd.productcode = ed.src_productcode
    LEFT JOIN {{ ref('productlines') }} pl ON sd.productline = pl.productline
    CROSS JOIN etl_metadata.batch_control em
)

SELECT *
FROM ranked_data

{% if is_incremental() %}
WHERE
    ranked_data.src_productcode IS NOT NULL  -- Only process new or updated rows
{% endif %}