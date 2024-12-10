{{ config(
    materialized='incremental',
    unique_key='officecode'
) }}

WITH ranked_data AS (
    SELECT
        sd.officecode AS officecode,
        sd.city,
        sd.phone,
        sd.addressline1,
        sd.addressline2,
        sd.state,
        sd.country,
        sd.postalcode,
        sd.territory,
        sd.create_timestamp AS src_create_timestamp,
        COALESCE(sd.update_timestamp, ed.src_update_timestamp) AS src_update_timestamp,
        em.etl_batch_no,
        em.etl_batch_date,
        CURRENT_TIMESTAMP AS dw_create_timestamp,
        CASE
            WHEN ed.officecode IS NOT NULL THEN CURRENT_TIMESTAMP
            ELSE ed.dw_update_timestamp
        END AS dw_update_timestamp,
        ROW_NUMBER() OVER (ORDER BY sd.officecode) + COALESCE(MAX(ed.dw_office_id) OVER (), 0) AS dw_office_id
    FROM
        devstage.offices sd
    LEFT JOIN devdw.offices ed ON sd.officecode = ed.officecode
    CROSS JOIN etl_metadata.batch_control em
)

SELECT *
FROM ranked_data

{% if is_incremental() %}
WHERE
    ranked_data.officecode IS NOT NULL  -- Process only new or updated rows in incremental runs
{% endif %}
