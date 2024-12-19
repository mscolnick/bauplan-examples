-- bauplan: materialization_strategy=REPLACE

SELECT 
    DATE_TRUNC('hour', event_time::TIMESTAMP) AS event_hour,
    event_type,
    product_id,
    brand,
    price,
    user_id,
    user_session
FROM public.ecommerce
