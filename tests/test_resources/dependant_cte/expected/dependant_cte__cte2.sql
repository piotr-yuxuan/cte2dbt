SELECT o.id, o.product_id, oi.quantity
FROM {{ source("main", "orders") }} AS o
JOIN {{ source("main", "order_items") }} AS oi ON o.id = oi.order_id
JOIN {{ ref("dependant_cte__cte1") }} AS cte1 ON o.id = cte1.id /* Using cte1 here */
WHERE oi.quantity > 5
