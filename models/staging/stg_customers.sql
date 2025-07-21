{{
  config(
    materialized='view',
    tags=['staging', 'daily']
  )
}}

WITH source AS (
  SELECT * FROM {{ ref('stg_invoices') }}
),

customer_latest_country AS (
  SELECT 
    customer_id,
    country,
    invoice_date,
    -- Rank countries by most recent order date
    ROW_NUMBER() OVER (
      PARTITION BY customer_id 
      ORDER BY invoice_date DESC
    ) as country_rank
  FROM source
),

customer_metrics AS (
  SELECT 
    customer_id,
    MIN(invoice_date) AS first_order_date,
    MAX(invoice_date) AS last_order_date,
    COUNT(DISTINCT invoice_id) AS total_orders,
    ROUND(SUM(line_total), 2) AS total_spent,
    COUNT(DISTINCT country) AS country_count,
    STRING_AGG(DISTINCT country, ', ' ORDER BY country) AS all_countries
  FROM source
  GROUP BY customer_id
),

unique_customers AS (
  SELECT 
    m.customer_id,
    c.country AS primary_country,
    m.first_order_date,
    m.last_order_date,
    m.total_orders,
    m.total_spent,
    m.country_count,
    m.all_countries,
    CASE 
      WHEN m.country_count > 1 THEN 'Multi-Country'
      ELSE 'Single-Country' 
    END as geographic_segment,
    
    -- Add metadata
    CURRENT_TIMESTAMP() AS _loaded_at
    
  FROM customer_metrics m
  LEFT JOIN customer_latest_country c 
    ON m.customer_id = c.customer_id 
    AND c.country_rank = 1
)

SELECT * FROM unique_customers