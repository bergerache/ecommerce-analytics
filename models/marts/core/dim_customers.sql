{{
  config(
    materialized='table',
    tags=['marts', 'core', 'daily']
  )
}}

WITH customer_metrics AS (
  SELECT * FROM {{ ref('int_customer_metrics') }}
),

rfm_data AS (
  SELECT * FROM {{ ref('int_rfm_analysis') }}
),

-- Combine customer metrics with RFM analysis
final_dimension AS (
  SELECT
    -- Customer identifiers
    cm.customer_id,
    
    -- Transaction metrics
    cm.total_orders,
    cm.total_revenue,
    cm.total_items_purchased,
    cm.total_line_items,
    cm.avg_order_value,
    cm.avg_items_per_order,
    cm.avg_price_per_item,
    
    -- Time attributes
    cm.first_order_date,
    cm.last_order_date,
    cm.customer_lifespan_days,
    
    -- Geographic attributes  
    cm.countries_purchased_from,
    cm.primary_country,
    cm.primary_market,
    
    -- Additional metrics
    cm.unique_products_purchased,
    cm.avg_products_per_order,
    
    -- Segmentation from existing models
    cm.purchase_frequency_segment,
    cm.customer_value_segment,
    
    -- RFM Analysis
    rfm.rfm_segment_name,
    rfm.rfm_segment,
    rfm.recency_score,
    rfm.frequency_score,
    rfm.monetary_score,
    rfm.customer_lifecycle_stage,
    
    -- Business flags
    CASE WHEN cm.total_orders = 1 THEN TRUE ELSE FALSE END AS is_one_time_customer,
    CASE WHEN cm.total_revenue >= 500 THEN TRUE ELSE FALSE END AS is_vip_customer,
    CASE WHEN cm.countries_purchased_from > 1 THEN TRUE ELSE FALSE END AS is_multi_country_customer,


    -- RFM Grouped Segments for simplified analysis
    CASE 
      WHEN rfm.rfm_segment_name IN ('Champions', 'Loyal Customers') 
        THEN 'HIGH VALUE'
      WHEN rfm.rfm_segment_name IN ('Potential Loyalists', 'Promising') 
        THEN 'MODERATE VALUE'
      WHEN rfm.rfm_segment_name IN ('At Risk', 'Dormant Loyalists') 
        THEN 'AT RISK'
      WHEN rfm.rfm_segment_name IN ('Lost', 'New Customers', 'Others') 
        THEN 'LOW ENGAGEMENT'
      ELSE 'UNCLASSIFIED'
    END AS rfm_grouped_segments,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS _loaded_at
    
  FROM customer_metrics cm
  LEFT JOIN rfm_data rfm ON cm.customer_id = rfm.customer_id
)

SELECT * FROM final_dimension