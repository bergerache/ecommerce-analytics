{{
  config(
    materialized='view',
    tags=['staging', 'daily']
  )
}}

WITH source AS (
  SELECT * FROM {{ ref('stg_invoices') }}
),

unique_products AS (
  SELECT DISTINCT
    product_id,
    product_description,
    
    -- Add metadata
    CURRENT_TIMESTAMP() AS _loaded_at
    
  FROM source
)

SELECT * FROM unique_products