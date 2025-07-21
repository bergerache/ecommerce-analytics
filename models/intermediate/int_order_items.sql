{{
  config(
    materialized='incremental',
    unique_key='invoice_item_id',
    tags=['intermediate', 'daily']
  )
}}

WITH invoice_items AS (
  SELECT * FROM {{ ref('stg_invoices') }}
  {% if is_incremental() %}
    WHERE invoice_datetime > (SELECT MAX(invoice_datetime) FROM {{ this }})
  {% endif %}
),

products AS (
  SELECT * FROM {{ ref('stg_products') }}
),

customers AS (
  SELECT * FROM {{ ref('stg_customers') }}
),

enriched_items AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['ii.invoice_id', 'ii.product_id']) }} AS invoice_item_id,
    
    -- Keys
    ii.invoice_id,
    ii.customer_id,
    ii.product_id,
    ii.invoice_date,
    
    -- Product context
    p.product_description,
    
    -- Customer context
    c.primary_country,
    c.geographic_segment,
    ii.invoice_date = c.first_order_date AS is_first_order,
    
    -- Time intelligence
    EXTRACT(YEAR FROM ii.invoice_date) AS order_year,
    EXTRACT(QUARTER FROM ii.invoice_date) AS order_quarter,
    EXTRACT(MONTH FROM ii.invoice_date) AS order_month,
    EXTRACT(HOUR FROM ii.invoice_datetime) AS order_hour,
    
    -- Measures
    ii.quantity,
    ii.unit_price,
    ii.line_total,
    
    -- Metadata
    ii.invoice_datetime,
    CURRENT_TIMESTAMP() AS _loaded_at
    
  FROM invoice_items ii
  LEFT JOIN products p ON ii.product_id = p.product_id
  LEFT JOIN customers c ON ii.customer_id = c.customer_id
)

SELECT * FROM enriched_items

