{{
    config(
        materialized='incremental',
        unique_key='Id'
    )
}}

WITH

new_contracts AS (
    SELECT * FROM {{ source('databricks_catalog', 'raw_contracts') }}

    {% if is_incremental() %}
    WHERE CreatedDate > (SELECT MAX(CreatedDate) FROM {{ this }})
    {% endif %}
),

latest_records AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY Id ORDER BY CreatedDate DESC) AS row_num
        FROM new_contracts
    )
    WHERE row_num = 1
)

SELECT
    *,
    CURRENT_TIMESTAMP() AS LastUpdated,
    '{{ invocation_id }}' AS BatchId
FROM latest_records
