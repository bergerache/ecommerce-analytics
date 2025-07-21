{{
  config(
    materialized='view',
    tags=['intermediate', 'geographic']
  )
}}

WITH customer_geography AS (
  SELECT * FROM {{ ref('stg_customers') }}
),

geographic_analysis AS (
  SELECT
    customer_id,
    primary_country,
    country_count,
    all_countries,
    geographic_segment,
    total_spent,
    total_orders,
    
    -- Geographic metrics
    ROUND(total_spent / total_orders, 2) AS avg_order_value,
    DATE_DIFF(last_order_date, first_order_date, DAY) AS customer_lifespan_days,
    
    -- Geographic flags (derived from existing segment)
    geographic_segment = 'Multi-Country' AS is_multi_country_customer,
    
    CASE
      WHEN country_count = 2 THEN 'Two Countries'
      WHEN country_count = 3 THEN 'Three Countries'
      WHEN country_count > 3 THEN 'Four+ Countries'
      ELSE 'Single Country'
    END AS geographic_complexity,
    
    -- Metadata
    first_order_date,
    last_order_date,
    CURRENT_TIMESTAMP() AS _loaded_at
    
  FROM customer_geography
)

SELECT * FROM geographic_analysis