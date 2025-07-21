{{
  config(
    materialized='view',
    tags=['staging', 'daily']
  )
}}

WITH source AS (
  SELECT * FROM `ecommerce-analytics-rb.raw_data.invoices`
),

cleaned AS (
  SELECT
    -- Invoice fields
    TRIM(InvoiceNo) AS invoice_id,
    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', TRIM(InvoiceDate)) AS invoice_datetime,
    DATE(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', TRIM(InvoiceDate))) AS invoice_date,
    
    -- Product fields  
    TRIM(StockCode) AS product_id,
    TRIM(Description) AS product_description,
    
    -- Transaction fields
    CAST(CAST(TRIM(Quantity) AS FLOAT64) AS INT64) AS quantity,
    CAST(TRIM(UnitPrice) AS FLOAT64) AS unit_price,
    ROUND(CAST(CAST(TRIM(Quantity) AS FLOAT64) AS INT64) * CAST(TRIM(UnitPrice) AS FLOAT64), 2) AS line_total,
    
    -- Customer fields
    CAST(CAST(TRIM(CustomerID) AS FLOAT64) AS INT64) AS customer_id,
    TRIM(UPPER(Country)) AS country,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS _loaded_at
    
  FROM source
  WHERE
    TRIM(CustomerID) != 'nan'
    AND CAST(CAST(TRIM(Quantity) AS FLOAT64) AS INT64) > 0   
    AND CAST(TRIM(UnitPrice) AS FLOAT64) > 0
    AND TRIM(StockCode) IS NOT NULL
    AND TRIM(StockCode) != ''
    AND TRIM(Description) IS NOT NULL  
    AND TRIM(Description) != ''
    AND NOT STARTS_WITH(TRIM(InvoiceNo), 'C')  -- Exclude cancellations
)

SELECT * FROM cleaned