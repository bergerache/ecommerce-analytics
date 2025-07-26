{{
  config(
    materialized='table',
    tags=['marts', 'analytics', 'product']
  )
}}

WITH sales_data AS (
  SELECT * FROM {{ ref('fct_sales') }}
),

-- Product aggregations from fct_sales instead of int_product_metrics
product_aggregations AS (
  SELECT
    product_id,
    product_description,
    
    -- Volume metrics
    COUNT(DISTINCT invoice_id) AS orders_containing_product,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(quantity) AS total_quantity_sold,
    COUNT(*) AS total_line_items,
    
    -- Revenue metrics
    SUM(line_total) AS total_revenue,
    ROUND(AVG(unit_price), 2) AS avg_selling_price,
    ROUND(SUM(line_total) / COUNT(DISTINCT invoice_id), 2) AS avg_revenue_per_order,
    ROUND(SUM(line_total) / COUNT(DISTINCT customer_id), 2) AS avg_revenue_per_customer,
    
    -- Geographic performance
    COUNT(DISTINCT CASE WHEN country = 'UNITED KINGDOM' THEN invoice_id END) AS uk_orders,
    COUNT(DISTINCT CASE WHEN country != 'UNITED KINGDOM' THEN invoice_id END) AS eu_orders,
    SUM(CASE WHEN country = 'UNITED KINGDOM' THEN line_total ELSE 0 END) AS uk_revenue,
    SUM(CASE WHEN country != 'UNITED KINGDOM' THEN line_total ELSE 0 END) AS eu_revenue,
    SUM(CASE WHEN country = 'UNITED KINGDOM' THEN quantity ELSE 0 END) AS uk_quantity,
    SUM(CASE WHEN country != 'UNITED KINGDOM' THEN quantity ELSE 0 END) AS eu_quantity,
    
    -- Time-based insights
    MIN(invoice_date) AS first_sold_date,
    MAX(invoice_date) AS last_sold_date,
    DATE_DIFF(MAX(invoice_date), MIN(invoice_date), DAY) AS product_lifespan_days,
    
    -- Seasonal performance (month distribution)
    COUNT(DISTINCT CASE WHEN EXTRACT(MONTH FROM invoice_date) IN (12, 1, 2) THEN invoice_id END) AS winter_orders,
    COUNT(DISTINCT CASE WHEN EXTRACT(MONTH FROM invoice_date) IN (3, 4, 5) THEN invoice_id END) AS spring_orders,
    COUNT(DISTINCT CASE WHEN EXTRACT(MONTH FROM invoice_date) IN (6, 7, 8) THEN invoice_id END) AS summer_orders,
    COUNT(DISTINCT CASE WHEN EXTRACT(MONTH FROM invoice_date) IN (9, 10, 11) THEN invoice_id END) AS autumn_orders,
    
    -- Business vs weekend performance
    COUNT(DISTINCT CASE WHEN day_type = 'Weekday' THEN invoice_id END) AS weekday_orders,
    COUNT(DISTINCT CASE WHEN day_type = 'Weekend' THEN invoice_id END) AS weekend_orders,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS _calculated_at
    
  FROM sales_data
  GROUP BY product_id, product_description
),

-- Complete product intelligence analysis in one flat table
product_intelligence AS (
  SELECT
    -- Product identifiers
    product_id,
    product_description,
    
    -- Core performance metrics
    total_revenue,
    total_quantity_sold,
    unique_customers,
    orders_containing_product,
    avg_selling_price,
    
    -- Rankings
    ROW_NUMBER() OVER (ORDER BY total_revenue DESC) AS revenue_rank,
    ROW_NUMBER() OVER (ORDER BY total_quantity_sold DESC) AS volume_rank,
    ROW_NUMBER() OVER (ORDER BY unique_customers DESC) AS customer_reach_rank,
    
    -- Portfolio concentration analysis
    ROUND(
      SUM(total_revenue) OVER (ORDER BY total_revenue DESC) * 100.0 / 
      SUM(total_revenue) OVER (), 1
    ) AS cumulative_revenue_percentage,
    
    -- Revenue tier classification
    CASE
      WHEN ROW_NUMBER() OVER (ORDER BY total_revenue DESC) <= 50 THEN 'Top 50'
      WHEN ROW_NUMBER() OVER (ORDER BY total_revenue DESC) <= 200 THEN 'Top 200'
      WHEN ROW_NUMBER() OVER (ORDER BY total_revenue DESC) <= 500 THEN 'Top 500'
      ELSE 'Long Tail'
    END AS revenue_tier,
    
    -- Geographic performance
    uk_revenue,
    eu_revenue,
    uk_orders,
    eu_orders,
    
    -- Geographic ratios
    ROUND(uk_revenue * 100.0 / NULLIF(total_revenue, 0), 1) AS uk_revenue_percentage,
    ROUND(eu_revenue * 100.0 / NULLIF(total_revenue, 0), 1) AS eu_revenue_percentage,
    
    -- Geographic preference classification
    CASE
      WHEN uk_revenue > eu_revenue * 2 THEN 'UK-Focused'
      WHEN eu_revenue > uk_revenue * 2 THEN 'EU-Focused'
      WHEN uk_revenue > 0 AND eu_revenue > 0 THEN 'Balanced'
      WHEN uk_revenue > 0 THEN 'UK-Only'
      ELSE 'EU-Only'
    END AS geographic_preference,
    
    -- Revenue per order by market
    ROUND(uk_revenue / NULLIF(uk_orders, 0), 2) AS uk_revenue_per_order,
    ROUND(eu_revenue / NULLIF(eu_orders, 0), 2) AS eu_revenue_per_order,
    
    -- Performance categorization
    CASE 
      WHEN ROW_NUMBER() OVER (ORDER BY total_revenue DESC) <= 10 THEN 'Revenue Star'
      WHEN ROW_NUMBER() OVER (ORDER BY total_quantity_sold DESC) <= 10 THEN 'Volume Star'
      WHEN ROW_NUMBER() OVER (ORDER BY unique_customers DESC) <= 10 THEN 'Reach Star'
      ELSE 'Standard'
    END AS performance_category,
    
    -- Multi-metric performance score
    ROUND(
      (total_revenue / 1000) + 
      (total_quantity_sold / 100) + 
      (unique_customers * 2), 1
    ) AS composite_performance_score,
    
    -- Business insights flags
    CASE WHEN total_revenue < 100 THEN TRUE ELSE FALSE END AS is_underperforming,
    CASE WHEN ROW_NUMBER() OVER (ORDER BY total_revenue DESC) <= 50 THEN TRUE ELSE FALSE END AS is_top_performer,
    CASE WHEN uk_revenue > 0 AND eu_revenue > 0 THEN TRUE ELSE FALSE END AS is_multi_market,
    
    -- Metadata
    _calculated_at
    
  FROM product_aggregations
)

SELECT * FROM product_intelligence