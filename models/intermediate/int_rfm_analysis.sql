{{
  config(
    materialized='table',
    tags=['intermediate', 'customer', 'rfm']
  )
}}

WITH customer_metrics AS (
  SELECT * FROM {{ ref('int_customer_metrics') }}
),

rfm_calculations AS (
  SELECT
    customer_id,
    primary_country,
    primary_market,
    total_orders,
    total_revenue,
    first_order_date,
    last_order_date,
    
    -- RFM raw metrics
    DATE_DIFF(CURRENT_DATE(), last_order_date, DAY) AS recency_days,
    total_orders AS frequency,
    total_revenue AS monetary_value,
    
    -- INTUITIVE RFM scores: R(1=best), F(5=best), M(5=best)
    NTILE(5) OVER (ORDER BY DATE_DIFF(CURRENT_DATE(), last_order_date, DAY) ASC) AS recency_score,
    NTILE(5) OVER (ORDER BY total_orders ASC) AS frequency_score,
    NTILE(5) OVER (ORDER BY total_revenue ASC) AS monetary_score
    
  FROM customer_metrics
),

rfm_segments AS (
  SELECT
    *,
    -- Concatenated RFM score for reference
    CONCAT(CAST(recency_score AS STRING), CAST(frequency_score AS STRING), CAST(monetary_score AS STRING)) AS rfm_segment,
    
    -- Business-friendly segment names based on INTUITIVE RFM combinations
    CASE
      WHEN recency_score <= 2 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'Champions'
      WHEN recency_score <= 3 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'Loyal Customers'
      WHEN recency_score <= 3 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'Potential Loyalists'
      WHEN recency_score <= 2 AND frequency_score <= 2 THEN 'New Customers'
      WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'At Risk'
      WHEN recency_score >= 4 AND frequency_score <= 2 AND monetary_score <= 2 THEN 'Lost'
      WHEN recency_score >= 4 AND frequency_score >= 3 THEN 'Dormant Loyalists'
      WHEN recency_score <= 3 AND frequency_score <= 2 AND monetary_score <= 2 THEN 'Promising'
      ELSE 'Others'
    END AS rfm_segment_name,
    
    -- Customer lifetime stage classification
    CASE
      WHEN DATE_DIFF(CURRENT_DATE(), first_order_date, DAY) <= 90 THEN 'New'
      WHEN DATE_DIFF(CURRENT_DATE(), first_order_date, DAY) <= 365 THEN 'Developing'
      ELSE 'Established'
    END AS customer_lifecycle_stage,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS _calculated_at
    
  FROM rfm_calculations
)

SELECT * FROM rfm_segments