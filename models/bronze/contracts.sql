{{
    config(
        materialized='incremental'
    )
}}

WITH

new_contracts AS (
    SELECT * FROM hive_metastore.dbt_ychristoffel.raw_contracts

    {% if is_incremental() %}
    WHERE CreatedDate > (SELECT MAX(CreatedDate) FROM {{ this }})
    {% endif %}
)

SELECT
    *,
    CURRENT_TIMESTAMP() AS LastUpdated,
    '{{ invocation_id }}' AS BatchId
FROM new_contracts
