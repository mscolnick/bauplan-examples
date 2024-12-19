-- bauplan: materialization_strategy=REPLACE

SELECT
  brand,
  COUNT(product_id)::FLOAT/count(distinct user_session) AS products_per_user_session,
  round(sum(price),2) AS revenue
FROM ecommerce_clean
WHERE event_type = 'purchase'
GROUP BY 1
ORDER BY 3 DESC