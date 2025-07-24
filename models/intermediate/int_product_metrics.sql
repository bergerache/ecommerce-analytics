{{
  config(
    materialized='table',
    tags=['intermediate', 'product']
  )
}}

WITH order_data AS (
  SELECT * FROM {{ ref('int_order_items') }}
),

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
    ROUND(SUM(line_total) / SUM(quantity), 2) AS avg_revenue_per_unit,
    ROUND(SUM(line_total) / COUNT(DISTINCT invoice_id), 2) AS avg_revenue_per_order,
    
    -- Performance insights
    ROUND(SUM(quantity) / COUNT(DISTINCT invoice_id), 2) AS avg_quantity_per_order,
    ROUND(COUNT(DISTINCT invoice_id) * 1.0 / COUNT(DISTINCT customer_id), 2) AS orders_per_customer,
    
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
    
  FROM order_data
  GROUP BY product_id, product_description
)

SELECT * FROM product_aggregations