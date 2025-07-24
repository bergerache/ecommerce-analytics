{{
  config(
    materialized='table',
    tags=['intermediate', 'customer']
  )
}}

WITH order_data AS (
  SELECT * FROM {{ ref('int_order_items') }}
),

-- First, get country frequencies per customer
customer_country_frequency AS (
  SELECT
    customer_id,
    country,
    COUNT(*) AS country_order_count,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY COUNT(*) DESC, country ASC) AS country_rank
  FROM order_data
  GROUP BY customer_id, country
),

-- Get primary country for each customer
customer_primary_country AS (
  SELECT
    customer_id,
    country AS primary_country
  FROM customer_country_frequency
  WHERE country_rank = 1
),

customer_aggregations AS (
  SELECT
    od.customer_id,
    
    -- Basic metrics
    COUNT(DISTINCT od.invoice_id) AS total_orders,
    SUM(od.line_total) AS total_revenue,
    SUM(od.quantity) AS total_items_purchased,
    COUNT(*) AS total_line_items,
    
    -- Derived metrics
    ROUND(SUM(od.line_total) / COUNT(DISTINCT od.invoice_id), 2) AS avg_order_value,
    ROUND(SUM(od.quantity) / COUNT(DISTINCT od.invoice_id), 2) AS avg_items_per_order,
    ROUND(SUM(od.line_total) / SUM(od.quantity), 2) AS avg_price_per_item,
    
    -- Time-based metrics
    MIN(od.invoice_date) AS first_order_date,
    MAX(od.invoice_date) AS last_order_date,
    DATE_DIFF(MAX(od.invoice_date), MIN(od.invoice_date), DAY) AS customer_lifespan_days,
    
    -- Geographic metrics
    COUNT(DISTINCT od.country) AS countries_purchased_from,
    cpc.primary_country,
    
    -- Market segment (based on primary country)
    CASE 
      WHEN cpc.primary_country = 'UNITED KINGDOM' THEN 'UK'
      ELSE 'EU'
    END AS primary_market,
    
    -- Customer behavioral classifications
    CASE
      WHEN COUNT(DISTINCT od.invoice_id) = 1 THEN 'One-Time'
      WHEN COUNT(DISTINCT od.invoice_id) BETWEEN 2 AND 4 THEN 'Occasional'
      WHEN COUNT(DISTINCT od.invoice_id) BETWEEN 5 AND 9 THEN 'Regular'
      ELSE 'Frequent'
    END AS purchase_frequency_segment,
    
    CASE
      WHEN SUM(od.line_total) < 50 THEN 'Low Value'
      WHEN SUM(od.line_total) BETWEEN 50 AND 200 THEN 'Medium Value'
      WHEN SUM(od.line_total) BETWEEN 200 AND 500 THEN 'High Value'
      ELSE 'VIP'
    END AS customer_value_segment,
    
    -- Product diversity
    COUNT(DISTINCT od.product_id) AS unique_products_purchased,
    ROUND(COUNT(DISTINCT od.product_id) * 1.0 / COUNT(DISTINCT od.invoice_id), 2) AS avg_products_per_order,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS _calculated_at
    
  FROM order_data od
  LEFT JOIN customer_primary_country cpc ON od.customer_id = cpc.customer_id
  GROUP BY od.customer_id, cpc.primary_country
)


SELECT * FROM customer_aggregations