{{
  config(
    materialized='incremental',
    unique_key='order_item_id',
    tags=['intermediate', 'daily']
  )
}}

WITH base_transactions AS (
  SELECT * FROM {{ ref('stg_invoices') }}
  {% if is_incremental() %}
    WHERE invoice_datetime > (SELECT MAX(invoice_datetime) FROM {{ this }})
  {% endif %}
),

enriched_transactions AS (
  SELECT
    -- Unique identifier for each transaction line item
    {{ dbt_utils.generate_surrogate_key(['invoice_id', 'product_id']) }} AS order_item_id,
    
    -- Core transaction data
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
    
    -- Time intelligence for operational patterns
    EXTRACT(YEAR FROM invoice_date) AS order_year,
    EXTRACT(MONTH FROM invoice_date) AS order_month,
    EXTRACT(DAY FROM invoice_date) AS order_day,
    EXTRACT(DAYOFWEEK FROM invoice_date) AS day_of_week,
    EXTRACT(HOUR FROM invoice_datetime) AS order_hour,
    
    -- Business classifications for segmentation
    CASE 
      WHEN country = 'UNITED KINGDOM' THEN 'UK'
      ELSE 'EU'
    END AS market_segment,
    
    -- Time classifications (choose one approach)
    CASE 
      WHEN EXTRACT(DAYOFWEEK FROM invoice_date) IN (1, 7) THEN 'Weekend'
      ELSE 'Weekday'
    END AS day_type,
    
    CASE
      WHEN EXTRACT(HOUR FROM invoice_datetime) BETWEEN 9 AND 17 
        AND EXTRACT(DAYOFWEEK FROM invoice_date) NOT IN (1, 7)
      THEN 'Business Hours'
      ELSE 'Off Hours'
    END AS time_segment,
    
    -- Monthly grouping for trends
    DATE_TRUNC(DATE(invoice_date), MONTH) AS order_month_date,
    
    -- Metadata
    _loaded_at
    
    
  FROM base_transactions
)

SELECT * FROM enriched_transactions