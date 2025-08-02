-- Test RFM segment logic is mathematically sound
-- Validates that Champions have high RFM scores and Lost customers have low scores
-- Test FAILS if segment classifications don't match expected RFM score patterns

WITH rfm_validation AS (
  SELECT 
    customer_id,
    rfm_segment_name,
    recency_score,
    frequency_score,
    monetary_score,
    
    -- Calculate average RFM score for validation
    ROUND((recency_score + frequency_score + monetary_score) / 3.0, 1) as avg_rfm_score,
    
    -- Define expected score ranges for key segments
    CASE 
      WHEN rfm_segment_name = 'Champions' AND 
           NOT (recency_score <= 2 AND frequency_score >= 4 AND monetary_score >= 4) THEN 'Champions_invalid_logic'
      WHEN rfm_segment_name = 'Lost' AND 
           (recency_score + frequency_score + monetary_score) > 9 THEN 'Lost_too_high'
      WHEN rfm_segment_name = 'Loyal Customers' AND 
           (frequency_score < 3 OR monetary_score < 3) THEN 'Loyal_insufficient_scores'
      ELSE 'valid'
    END as validation_flag
    
  FROM {{ ref('dim_customers') }}
  WHERE rfm_segment_name IN ('Champions', 'Lost', 'Loyal Customers')
)

-- Return records that violate RFM business logic
-- If this query returns rows, the test FAILS
SELECT 
  customer_id,
  rfm_segment_name,
  recency_score,
  frequency_score, 
  monetary_score,
  avg_rfm_score,
  validation_flag,
  'RFM segment logic violation detected!' as error_message
FROM rfm_validation
WHERE validation_flag != 'valid'