SELECT o.id, o.product_id, oi.quantity
FROM (
  SELECT cte1, *
  FROM {{ source("main", "orders") }} AS o
  FULL OUTER JOIN {{ ref("nested_subqueries__cte1") }} AS cte1 ON o.id = cte1.id
) AS o
JOIN {{ source("main", "order_items") }} AS oi ON o.id = oi.order_id
WHERE oi.quantity > 5
