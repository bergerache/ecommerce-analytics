{{
  config(
    materialized='table',
    tags=['marts', 'analytics', 'executive']
  )
}}

WITH customers AS (
  SELECT * FROM {{ ref('dim_customers') }}
),

sales AS (
  SELECT * FROM {{ ref('fct_sales') }}
),

-- All metrics in one query - no cross joins needed
business_summary AS (
  SELECT
    -- Core business metrics from sales
    COUNT(DISTINCT s.invoice_id) AS total_orders,
    COUNT(DISTINCT s.customer_id) AS total_customers,
    COUNT(DISTINCT s.product_id) AS total_products,
    COUNT(DISTINCT s.country) AS total_countries,
    SUM(s.line_total) AS total_revenue,
    SUM(s.quantity) AS total_items_sold,
    
    -- KPIs calculated from sales
    ROUND(SUM(s.line_total) / COUNT(DISTINCT s.invoice_id), 2) AS avg_order_value,
    ROUND(SUM(s.quantity) / COUNT(DISTINCT s.invoice_id), 2) AS avg_items_per_order,
    
    -- Customer metrics using subqueries (no cross join!)
    (SELECT ROUND(AVG(total_revenue), 2) FROM customers) AS revenue_per_customer,
    (SELECT ROUND(AVG(total_orders), 2) FROM customers) AS avg_orders_per_customer,
    
    -- Date range information
    MIN(s.invoice_date) AS data_start_date,
    MAX(s.invoice_date) AS data_end_date,
    DATE_DIFF(MAX(s.invoice_date), MIN(s.invoice_date), DAY) AS data_span_days,
    
    -- Market segment insights
    SUM(CASE WHEN s.market_segment = 'UK' THEN s.line_total ELSE 0 END) AS uk_total_revenue,
    SUM(CASE WHEN s.market_segment = 'EU' THEN s.line_total ELSE 0 END) AS eu_total_revenue,
    
    ROUND(
      SUM(CASE WHEN s.market_segment = 'UK' THEN s.line_total ELSE 0 END) 
      / COUNT(DISTINCT CASE WHEN s.market_segment = 'UK' THEN s.invoice_id END), 2
    ) AS uk_avg_order_value,
    
    ROUND(
      SUM(CASE WHEN s.market_segment = 'EU' THEN s.line_total ELSE 0 END) 
      / COUNT(DISTINCT CASE WHEN s.market_segment = 'EU' THEN s.invoice_id END), 2
    ) AS eu_avg_order_value,
    
    -- Monthly insights using subqueries (no cross join!)
    (
      SELECT MAX(monthly_total) 
      FROM (
        SELECT SUM(line_total) AS monthly_total
        FROM sales 
        GROUP BY order_year, order_month
      )
    ) AS highest_monthly_revenue,
    
    (
      SELECT MIN(monthly_total) 
      FROM (
        SELECT SUM(line_total) AS monthly_total
        FROM sales 
        GROUP BY order_year, order_month
      )
    ) AS lowest_monthly_revenue,
    
    (
      SELECT ROUND(AVG(monthly_total), 2) 
      FROM (
        SELECT SUM(line_total) AS monthly_total
        FROM sales 
        GROUP BY order_year, order_month
      )
    ) AS avg_monthly_revenue,
    
    (
      SELECT COUNT(*) 
      FROM (
        SELECT SUM(line_total) AS monthly_total
        FROM sales 
        GROUP BY order_year, order_month
      )
    ) AS total_months,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS _calculated_at
    
  FROM sales s
)

SELECT * FROM business_summary