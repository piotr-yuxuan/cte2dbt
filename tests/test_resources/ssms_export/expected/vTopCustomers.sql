SELECT TOP 10 customer_id, total
FROM {{ ref("vOrders") }} AS vOrders
ORDER BY total DESC
