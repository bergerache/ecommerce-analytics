{{
  config(
    materialized='table',
    tags=['marts', 'analytics', 'customer']
  )
}}

WITH customer_metrics AS (
  SELECT * FROM {{ ref('int_customer_metrics') }}
),

rfm_data AS (
  SELECT * FROM {{ ref('int_rfm_analysis') }}
),

marketing_actions AS (
  SELECT * FROM {{ ref('rfm_marketing_actions') }}
),

-- Revenue concentration analysis for business insights
revenue_concentration AS (
  SELECT
    customer_id,
    total_revenue,
    ROW_NUMBER() OVER (ORDER BY total_revenue DESC) as customer_rank,
    SUM(total_revenue) OVER () as total_business_revenue,
    SUM(total_revenue) OVER (ORDER BY total_revenue DESC ROWS UNBOUNDED PRECEDING) as cumulative_revenue
  FROM customer_metrics
),

customer_tiers AS (
  SELECT
    customer_id,
    CASE 
      WHEN customer_rank <= ROUND((SELECT COUNT(*) FROM customer_metrics) * 0.1) THEN 'Top 10%'
      WHEN customer_rank <= ROUND((SELECT COUNT(*) FROM customer_metrics) * 0.2) THEN 'Top 20%'
      WHEN customer_rank <= ROUND((SELECT COUNT(*) FROM customer_metrics) * 0.5) THEN 'Top 50%'
      ELSE 'Bottom 50%'
    END AS customer_tier,
    ROUND(cumulative_revenue / total_business_revenue * 100, 1) as cumulative_revenue_percentage
  FROM revenue_concentration
),

-- Combine all customer insights
enriched_customer_analysis AS (
  SELECT
    -- Customer identifiers
    cm.customer_id,
    cm.primary_country,
    cm.primary_market,
    
    -- Basic metrics
    cm.total_orders,
    cm.total_revenue,
    cm.avg_order_value,
    cm.total_items_purchased,
    cm.avg_items_per_order,
    cm.avg_price_per_item,
    
    -- Time-based insights
    cm.first_order_date,
    cm.last_order_date,
    cm.customer_lifespan_days,
    
    -- Geographic insights
    cm.countries_purchased_from,
    
    -- Behavioral classifications
    cm.purchase_frequency_segment,
    cm.customer_value_segment,
    
    -- Product insights
    cm.unique_products_purchased,
    cm.avg_products_per_order,
    
    -- RFM Analysis
    rfm.recency_days,
    rfm.recency_score,
    rfm.frequency_score,
    rfm.monetary_score,
    rfm.rfm_segment,
    rfm.rfm_segment_name,
    rfm.customer_lifecycle_stage,
    
    -- Marketing recommendations
    ma.recommended_action,
    
    -- Revenue concentration insights
    ct.customer_tier,
    ct.cumulative_revenue_percentage,
    
    -- Business insights (calculated fields)
    CASE 
      WHEN cm.total_orders = 1 THEN 'Single Purchase Risk'
      WHEN rfm.recency_days > 180 THEN 'Dormancy Risk'
      WHEN cm.total_revenue > 500 AND rfm.recency_days > 90 THEN 'High Value At Risk'
      WHEN cm.total_orders >= 5 AND cm.avg_order_value > 100 THEN 'VIP Potential'
      ELSE 'Standard'
    END AS business_risk_flag,
    
    CASE
      WHEN cm.avg_order_value > (SELECT AVG(avg_order_value) * 1.5 FROM customer_metrics) THEN 'High AOV'
      WHEN cm.total_orders > (SELECT AVG(total_orders) * 2 FROM customer_metrics) THEN 'High Frequency'
      WHEN cm.unique_products_purchased > 10 THEN 'Product Diverse'
      ELSE 'Standard'
    END AS customer_strength,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS _calculated_at
    
  FROM customer_metrics cm
  LEFT JOIN rfm_data rfm ON cm.customer_id = rfm.customer_id
  LEFT JOIN marketing_actions ma ON rfm.rfm_segment_name = ma.rfm_segment_name
  LEFT JOIN customer_tiers ct ON cm.customer_id = ct.customer_id
)

SELECT * FROM enriched_customer_analysis