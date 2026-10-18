SELECT id, product_id, quantity
FROM {{ source("main", "order_items") }} AS order_items
WHERE quantity > 5
