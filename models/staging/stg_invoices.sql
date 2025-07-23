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
    UPPER(TRIM(Description)) AS raw_product_description,
    
    -- Transaction fields
    CAST(CAST(TRIM(Quantity) AS FLOAT64) AS INT64) AS quantity,
    CAST(TRIM(UnitPrice) AS FLOAT64) AS unit_price,
    ROUND(CAST(CAST(TRIM(Quantity) AS FLOAT64) AS INT64) * CAST(TRIM(UnitPrice) AS FLOAT64), 2) AS line_total,
    
    -- Customer fields
    CAST(CAST(TRIM(CustomerID) AS FLOAT64) AS INT64) AS customer_id,
    
    -- Standardize country names
    CASE 
      WHEN UPPER(TRIM(Country)) = 'EIRE' THEN 'IRELAND'
      ELSE UPPER(TRIM(Country))
    END AS country,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS _loaded_at
    
  FROM source
  WHERE
    -- Data quality filters - Check for NULLs and empty strings across ALL critical columns
    
    -- Invoice fields
    TRIM(InvoiceNo) IS NOT NULL
    AND TRIM(InvoiceNo) != ''
    AND NOT STARTS_WITH(TRIM(InvoiceNo), 'C') -- Exclude cancellations
    AND TRIM(InvoiceDate) IS NOT NULL
    AND TRIM(InvoiceDate) != ''
    
    -- Product fields
    AND TRIM(StockCode) IS NOT NULL
    AND TRIM(StockCode) != ''
    AND TRIM(Description) IS NOT NULL
    AND TRIM(Description) != ''
    
    -- Transaction fields
    AND TRIM(Quantity) IS NOT NULL
    AND TRIM(Quantity) != ''
    AND CAST(CAST(TRIM(Quantity) AS FLOAT64) AS INT64) > 0
    AND TRIM(UnitPrice) IS NOT NULL
    AND TRIM(UnitPrice) != ''
    AND CAST(TRIM(UnitPrice) AS FLOAT64) > 0.01  -- Remove £0.001 pricing errors
    
    -- Customer fields
    AND TRIM(CustomerID) IS NOT NULL
    AND TRIM(CustomerID) != ''
    AND TRIM(CustomerID) != 'nan'  -- Also exclude 'nan' strings
    AND TRIM(Country) IS NOT NULL
    AND TRIM(Country) != ''
    
    -- Geographic filters - Keep only UK and EU countries (filter on raw data)  
    AND UPPER(TRIM(Country)) IN (
      'UNITED KINGDOM',
      'EIRE',              -- Keep original EIRE for filtering
      'GERMANY', 
      'FRANCE', 
      'SPAIN', 
      'BELGIUM', 
      'SWITZERLAND', 
      'PORTUGAL', 
      'ITALY', 
      'FINLAND', 
      'AUSTRIA', 
      'NORWAY', 
      'NETHERLANDS', 
      'DENMARK', 
      'SWEDEN', 
      'POLAND',
      'GREECE', 
      'CYPRUS',
      'CHANNEL ISLANDS'
    )
),

description_cleanup AS (
  SELECT
    *,
    -- Clean product descriptions step by step (much more readable!)
    CASE 
      -- Fix clear spelling errors first
      WHEN raw_product_description LIKE '%MISELTOE%' THEN 
        REPLACE(raw_product_description, 'MISELTOE', 'MISTLETOE')
      WHEN raw_product_description LIKE '%RETO %' THEN 
        REPLACE(raw_product_description, 'RETO ', 'RETRO ')
      
      -- Standardize to UK spellings  
      WHEN raw_product_description LIKE '%DOILEY%' THEN 
        REPLACE(raw_product_description, 'DOILEY', 'DOILY')
      
      -- Fix abbreviations
      WHEN raw_product_description LIKE '%B\'DRAW%' THEN 
        REPLACE(raw_product_description, 'B\'DRAW', 'DRAWER')
      WHEN raw_product_description LIKE '%DRAW LINER%' THEN 
        REPLACE(raw_product_description, 'DRAW LINER', 'DRAWER LINER')
      
      -- Fix spacing issues in compound words
      WHEN raw_product_description LIKE '%CUP CAKES%' THEN 
        REPLACE(raw_product_description, 'CUP CAKES', 'CUPCAKES')
      WHEN raw_product_description LIKE '%CAKE STAND%' THEN 
        REPLACE(raw_product_description, 'CAKE STAND', 'CAKESTAND')
      
      -- Clean up extra spaces (apply to all)
      ELSE REGEXP_REPLACE(raw_product_description, r'\s+', ' ')
    END AS cleaned_description
  FROM cleaned
),

-- REQUIREMENT 1: Find the most frequent description for each product across the dataset
product_description_frequency AS (
  SELECT
    product_id,
    cleaned_description,
    COUNT(*) AS description_frequency,
    -- Rank descriptions by frequency (most frequent = 1)
    ROW_NUMBER() OVER (
      PARTITION BY product_id 
      ORDER BY COUNT(*) DESC, cleaned_description ASC  -- Use alphabetical as tiebreaker
    ) AS description_rank
  FROM description_cleanup
  GROUP BY product_id, cleaned_description
),

-- Get the canonical (most frequent) description for each product
canonical_product_descriptions AS (
  SELECT
    product_id,
    -- REQUIREMENT 2: Ensure all selected descriptions are in UPPER CASE
    UPPER(cleaned_description) AS canonical_product_description
  FROM product_description_frequency
  WHERE description_rank = 1
),

-- Apply canonical descriptions to all transactions
with_canonical_descriptions AS (
  SELECT
    dc.invoice_id,
    dc.invoice_datetime,
    dc.invoice_date,
    dc.product_id,
    dc.quantity,
    dc.unit_price,
    dc.line_total,
    dc.customer_id,
    dc.country,
    dc._loaded_at,
    -- Use canonical description instead of the potentially inconsistent one
    cpd.canonical_product_description AS product_description
  FROM description_cleanup dc
  LEFT JOIN canonical_product_descriptions cpd 
    ON dc.product_id = cpd.product_id
),

-- REQUIREMENT 3: Remove duplicate rows (entire rows that are identical)
deduplicated AS (
  SELECT DISTINCT
    invoice_id,
    invoice_datetime,
    invoice_date,
    product_id,
    product_description,
    quantity,
    unit_price,
    line_total,
    customer_id,
    country,
    _loaded_at
  FROM with_canonical_descriptions
)

-- REQUIREMENT 4: All other cleaning solutions remain (geographic filters, data quality filters, description cleanup, etc.)
SELECT 
  invoice_id,
  invoice_datetime,
  invoice_date,
  product_id,
  product_description,  -- Canonical, UPPER CASE, consistent description
  quantity,
  unit_price,
  line_total,
  customer_id,
  country,
  _loaded_at
FROM deduplicated