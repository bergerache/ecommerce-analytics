{{
  config(
    materialized='view',
    tags=['intermediate', 'marketing']
  )
}}

WITH customer_orders AS (
  SELECT * FROM {{ ref('stg_invoices') }}
),

rfm_calculations AS (
  SELECT
    customer_id,
    
    -- Calculate max date inline and use for recency
    DATE_DIFF(
      (SELECT MAX(invoice_date) FROM customer_orders), 
      MAX(invoice_date), 
      DAY
    ) AS recency_days,
    
    -- Frequency: Number of distinct orders
    COUNT(DISTINCT invoice_id) AS frequency,
    
    -- Monetary: Total amount spent
    ROUND(SUM(line_total), 2) AS monetary_value,
    
    -- Additional metrics for context
    COUNT(*) AS total_line_items,
    ROUND(AVG(line_total), 2) AS avg_line_value,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS _loaded_at
    
  FROM customer_orders
  GROUP BY customer_id
)

SELECT * FROM rfm_calculations