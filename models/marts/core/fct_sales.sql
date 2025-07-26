{{
  config(
    materialized='incremental',
    unique_key='order_item_id',
    partition_by={
      "field": "invoice_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by=['customer_id', 'product_id'],
    tags=['marts', 'analytics', 'daily']
  )
}}

WITH order_items AS (
  SELECT * FROM {{ ref('int_order_items') }}
  {% if is_incremental() %}
    WHERE invoice_datetime > (SELECT MAX(invoice_datetime) FROM {{ this }})
  {% endif %}
),

enhanced_sales AS (
  SELECT
    -- Keys for joining
    order_item_id,
    invoice_id,
    customer_id,
    product_id,
    invoice_date,
    
    -- Time attributes
    invoice_datetime,
    order_year,
    order_month,
    order_day,
    day_of_week,
    order_hour,
    order_month_date,
    
    -- Product attributes
    product_description,
    
    -- Geographic attributes
    country,
    market_segment,
    
    -- Existing time classifications
    day_type,
    time_segment,
    
    -- Measures
    quantity,
    unit_price,
    line_total,
    
    -- ENHANCED OPERATIONAL INTELLIGENCE
    
    -- Day of week analysis (human-readable)
    CASE day_of_week
      WHEN 1 THEN 'Sunday'
      WHEN 2 THEN 'Monday' 
      WHEN 3 THEN 'Tuesday'
      WHEN 4 THEN 'Wednesday'
      WHEN 5 THEN 'Thursday'
      WHEN 6 THEN 'Friday'
      WHEN 7 THEN 'Saturday'
    END AS day_name,
    
    -- Removed hour_segment - redundant with peak_period and shopper_behavior_type
    
    -- Peak period identification (KEY INSIGHT)
    CASE
      WHEN day_type = 'Weekday' AND order_hour BETWEEN 10 AND 14 THEN 'Peak Weekday'
      WHEN day_type = 'Weekend' AND order_hour BETWEEN 11 AND 16 THEN 'Peak Weekend'
      ELSE 'Off Peak'
    END AS peak_period,
    
    -- Monthly seasonality
    CASE 
      WHEN order_month IN (12, 1, 2) THEN 'Winter'
      WHEN order_month IN (3, 4, 5) THEN 'Spring'
      WHEN order_month IN (6, 7, 8) THEN 'Summer'
      WHEN order_month IN (9, 10, 11) THEN 'Autumn'
    END AS season,
    
    -- Order value tier for pattern analysis (KEY INSIGHT)
    CASE
      WHEN line_total < 10 THEN 'Low Value'
      WHEN line_total BETWEEN 10 AND 50 THEN 'Medium Value'
      WHEN line_total BETWEEN 50 AND 100 THEN 'High Value'
      ELSE 'Premium Value'
    END AS order_value_tier,
    
    -- Customer behavior classification (KEY INSIGHT) - FIXED LOGIC
    CASE
      WHEN day_type = 'Weekday' AND time_segment = 'Business Hours' THEN 'Business Day Shopper'
      WHEN day_type = 'Weekday' AND time_segment = 'Off Hours' THEN 'Weekday Off-Hours Shopper'
      WHEN day_type = 'Weekend' AND time_segment = 'Business Hours' THEN 'Weekend Daytime Shopper'
      WHEN day_type = 'Weekend' AND time_segment = 'Off Hours' THEN 'Weekend Off-Hours Shopper'
      ELSE 'Unclassified'
    END AS shopper_behavior_type,
    
    -- Geographic timing patterns (KEY INSIGHT)
    CASE
      WHEN market_segment = 'UK' AND day_type = 'Weekend' THEN 'UK Weekend'
      WHEN market_segment = 'UK' AND day_type = 'Weekday' THEN 'UK Weekday'
      WHEN market_segment = 'EU' AND day_type = 'Weekend' THEN 'EU Weekend'
      WHEN market_segment = 'EU' AND day_type = 'Weekday' THEN 'EU Weekday'
    END AS geo_timing_pattern,
    
    -- Business insight flags for dashboard filtering
    CASE WHEN day_type = 'Weekend' THEN TRUE ELSE FALSE END AS is_weekend_order,
    CASE WHEN time_segment = 'Business Hours' THEN TRUE ELSE FALSE END AS is_business_hours,
    CASE WHEN order_hour BETWEEN 10 AND 14 THEN TRUE ELSE FALSE END AS is_peak_hours,
    CASE WHEN order_month = 12 THEN TRUE ELSE FALSE END AS is_december_order,
    CASE WHEN line_total > 50 THEN TRUE ELSE FALSE END AS is_high_value_order,
    CASE WHEN day_type = 'Weekend' AND line_total > 50 THEN TRUE ELSE FALSE END AS is_premium_weekend_order,
    
    -- Dashboard story insights (calculated fields for direct dashboard use)
    -- "Weekend orders have 25% higher AOV" - Weekend premium flag
    CASE 
      WHEN day_type = 'Weekend' THEN 'Premium Weekend'
      ELSE 'Standard'
    END AS weekend_premium_segment,
    
    -- "Peak hours drive volume but weekend drives value" 
    CASE
      WHEN order_hour BETWEEN 10 AND 14 AND day_type = 'Weekday' THEN 'High Volume Peak'
      WHEN day_type = 'Weekend' THEN 'High Value Weekend'
      ELSE 'Standard'
    END AS volume_value_segment,
    
    -- Country performance comparison for dashboard
    CASE
      WHEN market_segment = 'UK' THEN 'High Frequency Market'
      WHEN market_segment = 'EU' THEN 'High Value Market'
      ELSE 'Other'
    END AS market_characteristic,
    
    -- Metadata
    _loaded_at,
    CURRENT_TIMESTAMP() AS _fact_loaded_at
    
  FROM order_items
)

SELECT * FROM enhanced_sales