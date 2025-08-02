-- Test that fct_sales revenue matches business_overview total
-- This ensures data consistency across our core models
-- Test FAILS if there's a discrepancy > £0.01

WITH fct_sales_total AS (
  SELECT
    'fct_sales' as source,
    ROUND(SUM(line_total), 2) as total_revenue
  FROM {{ ref('fct_sales') }}
),

business_overview_total AS (
  SELECT
    'business_overview' as source,
    ROUND(total_revenue, 2) as total_revenue
  FROM {{ ref('business_overview') }}
),

revenue_comparison AS (
  SELECT * FROM fct_sales_total
  UNION ALL
  SELECT * FROM business_overview_total
),

-- Calculate the difference between the two revenue totals
revenue_discrepancy AS (
  SELECT 
    source,
    total_revenue,
    total_revenue - LAG(total_revenue) OVER (ORDER BY source) as difference
  FROM revenue_comparison
)

-- Return records only if there's a discrepancy > £0.01
-- If this query returns rows, the test FAILS
SELECT 
  source,
  total_revenue,
  difference,
  'Revenue mismatch detected!' as error_message
FROM revenue_discrepancy
WHERE ABS(COALESCE(difference, 0)) > 0.01