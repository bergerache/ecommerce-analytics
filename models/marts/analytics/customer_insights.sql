{{
  config(
    materialized='table',
    tags=['marts', 'analytics', 'customer']
  )
}}

WITH customers AS (
  SELECT * FROM {{ ref('dim_customers') }}
),

sales_data AS (
  SELECT * FROM {{ ref('fct_sales') }}
),

-- Customer aggregations from fct_sales instead of int_customer_metrics
customer_aggregations AS (
  SELECT
    customer_id,
    
    -- Basic metrics
    COUNT(DISTINCT invoice_id) AS total_orders,
    ROUND(SUM(line_total),1) AS total_revenue,
    SUM(quantity) AS total_items_purchased,
    COUNT(*) AS total_line_items,
    ROUND(SUM(line_total) / COUNT(DISTINCT invoice_id), 2) AS avg_order_value,
    ROUND(AVG(quantity), 2) AS avg_items_per_order,
    ROUND(AVG(unit_price), 2) AS avg_price_per_item,
    
    -- Time attributes
    MIN(invoice_date) AS first_order_date,
    MAX(invoice_date) AS last_order_date,
    DATE_DIFF(MAX(invoice_date), MIN(invoice_date), DAY) AS customer_lifespan_days,
    
    -- Geographic attributes
    COUNT(DISTINCT country) AS countries_purchased_from,
    STRING_AGG(DISTINCT country ORDER BY country) AS countries_list,
    
    -- Market segment (based on primary country)
    CASE
      WHEN COUNTIF(country = 'UNITED KINGDOM') > COUNTIF(country != 'UNITED KINGDOM') THEN 'UK'
      ELSE 'EU'
    END AS primary_market,
    
    -- Additional metrics
    COUNT(DISTINCT product_id) AS unique_products_purchased,
    ROUND(COUNT(DISTINCT product_id) * 1.0 / COUNT(DISTINCT invoice_id), 2) AS avg_products_per_order,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS _calculated_at
    
  FROM sales_data
  GROUP BY customer_id
),

-- Customer behavioral classifications
customer_classifications AS (
  SELECT
    ca.*,
    
    -- Customer behavioral classifications
    CASE
      WHEN ca.total_orders = 1 THEN 'One-Time'
      WHEN ca.total_orders BETWEEN 2 AND 4 THEN 'Occasional'
      WHEN ca.total_orders BETWEEN 5 AND 9 THEN 'Regular'
      ELSE 'Frequent'
    END AS purchase_frequency_segment,
    
    CASE
      WHEN ca.total_revenue < 50 THEN 'Low Value'
      WHEN ca.total_revenue BETWEEN 50 AND 200 THEN 'Medium Value'
      WHEN ca.total_revenue BETWEEN 200 AND 500 THEN 'High Value'
      ELSE 'VIP'
    END AS customer_value_segment,
    
    -- Business flags
    CASE WHEN ca.total_orders = 1 THEN TRUE ELSE FALSE END AS is_one_time_customer,
    CASE WHEN ca.total_revenue > 500 THEN TRUE ELSE FALSE END AS is_vip_customer,
    CASE WHEN ca.countries_purchased_from > 1 THEN TRUE ELSE FALSE END AS is_multi_country_customer
    
  FROM customer_aggregations ca
),

-- Revenue concentration analysis for business insights
revenue_concentration AS (
  SELECT
    customer_id,
    total_revenue,
    ROW_NUMBER() OVER (ORDER BY total_revenue DESC) AS customer_rank,
    ROUND(
      SUM(total_revenue) OVER (ORDER BY total_revenue DESC) * 100.0 / 
      SUM(total_revenue) OVER (), 1
    ) AS cumulative_revenue_percentage
  FROM customer_aggregations
),

customer_tiers AS (
  SELECT
    *,
    CASE
      WHEN customer_rank <= ROUND(COUNT(*) OVER () * 0.1) THEN 'Top 10%'
      WHEN customer_rank > ROUND(COUNT(*) OVER () * 0.1) 
        AND customer_rank <= ROUND(COUNT(*) OVER () * 0.2) THEN 'Top 11-20%'
      WHEN customer_rank > ROUND(COUNT(*) OVER () * 0.2) 
        AND customer_rank <= ROUND(COUNT(*) OVER () * 0.5) THEN 'Top 21-50%'
      ELSE 'Bottom 50%'
    END AS customer_tier,
    
    CASE
      WHEN cumulative_revenue_percentage <= 20 THEN 'Champions'
      WHEN cumulative_revenue_percentage <= 50 THEN 'Loyal Customers'
      WHEN cumulative_revenue_percentage <= 80 THEN 'Potential Loyalists'
      ELSE 'New Customers'
    END AS revenue_tier
    
  FROM revenue_concentration
),

-- Combine all customer insights
enriched_customer_analysis AS (
  SELECT
    -- Customer identifiers
    cc.customer_id,
    
    -- Transaction metrics
    cc.total_orders,
    cc.total_revenue,
    cc.total_items_purchased,
    cc.total_line_items,
    cc.avg_order_value,
    cc.avg_items_per_order,
    cc.avg_price_per_item,
    
    -- Time attributes
    cc.first_order_date,
    cc.last_order_date,
    cc.customer_lifespan_days,
    
    -- Geographic attributes
    cc.countries_purchased_from,
    cc.primary_market,
    
    -- Additional metrics
    cc.unique_products_purchased,
    cc.avg_products_per_order,
    
    -- Segmentation from existing models
    cc.purchase_frequency_segment,
    cc.customer_value_segment,
    
    -- RFM Analysis from dim_customers
    c.rfm_segment_name,
    c.rfm_segment,
    c.recency_score,
    c.frequency_score,
    c.monetary_score,
    c.customer_lifecycle_stage,
    
    -- Revenue concentration insights
    ct.customer_tier,
    ct.revenue_tier,
    ct.cumulative_revenue_percentage,

    ROUND(
      SUM(cc.total_revenue) OVER (PARTITION BY ct.customer_tier) * 100.0 / 
      SUM(cc.total_revenue) OVER (), 1
    ) AS tier_revenue_percentage,
    
    -- Business insights (calculated fields)
    CASE WHEN cc.total_revenue > 500 THEN TRUE ELSE FALSE END AS is_vip_customer,
    CASE WHEN cc.total_orders = 1 THEN TRUE ELSE FALSE END AS is_one_time_customer,
    CASE WHEN cc.countries_purchased_from > 1 THEN TRUE ELSE FALSE END AS is_multi_country_customer,
    
    -- Metadata
    cc._calculated_at
    
  FROM customer_classifications cc
  LEFT JOIN customers c ON cc.customer_id = c.customer_id
  LEFT JOIN customer_tiers ct ON cc.customer_id = ct.customer_id
)

SELECT * FROM enriched_customer_analysis