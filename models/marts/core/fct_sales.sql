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
    tags=['marts', 'core', 'daily']
  )
}}

WITH order_items AS (
  SELECT * FROM {{ ref('int_order_items') }}
  {% if is_incremental() %}
    WHERE invoice_datetime > (SELECT MAX(invoice_datetime) FROM {{ this }})
  {% endif %}
),

final AS (
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
    
    -- Time classifications
    day_type,
    time_segment,
    
    -- Measures
    quantity,
    unit_price,
    line_total,
    
    -- Metadata
    _loaded_at,
    CURRENT_TIMESTAMP() AS _fact_loaded_at
    
  FROM order_items
)

SELECT * FROM final