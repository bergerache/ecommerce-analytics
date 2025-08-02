-- Test customer counts are consistent across models
-- Critical for ensuring our customer analytics are accurate
-- Test FAILS if any model has different customer count than dim_customers

WITH customer_counts AS (
  SELECT 
    'fct_sales' as source,
    COUNT(DISTINCT customer_id) as customer_count
  FROM {{ ref('fct_sales') }}
  
  UNION ALL
  
  SELECT 
    'dim_customers' as source,
    COUNT(*) as customer_count
  FROM {{ ref('dim_customers') }}
  
  UNION ALL
  
  SELECT 
    'business_overview' as source,
    total_customers as customer_count
  FROM {{ ref('business_overview') }}
  
  UNION ALL
  
  SELECT 
    'customer_insights' as source,
    COUNT(*) as customer_count
  FROM {{ ref('customer_insights') }}
),

-- Get the expected count from dim_customers (our source of truth)
expected_count AS (
  SELECT customer_count as expected_count
  FROM customer_counts 
  WHERE source = 'dim_customers'
)

-- Test fails if any model has different customer count
-- If this query returns rows, the test FAILS
SELECT 
  cc.source,
  cc.customer_count,
  ec.expected_count,
  cc.customer_count - ec.expected_count as difference,
  'Customer count mismatch detected!' as error_message
FROM customer_counts cc
CROSS JOIN expected_count ec
WHERE cc.customer_count != ec.expected_count