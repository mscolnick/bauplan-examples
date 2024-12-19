-- bauplan: materialization_strategy=REPLACE

SELECT
  user_session as purchase_session,
  event_hour,
  count(*) as session_count
FROM ecommerce_clean
WHERE event_type = 'purchase'
GROUP BY 1, 2
ORDER BY 2 ASC