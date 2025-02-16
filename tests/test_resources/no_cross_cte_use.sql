WITH
  cte1 AS (
    SELECT id, name, amount
    FROM sales
    WHERE amount > 100
  ),
  cte2 AS (
    SELECT id, product_id, quantity
    FROM order_items
    WHERE quantity > 5
  ),
  cte3 AS (
    SELECT o.id, o.customer_id, s.name AS salesperson_name
    FROM orders o
    JOIN employees s ON o.salesperson_id = s.id
  )
SELECT
  cte1.name,
  cte1.amount,
  cte2.product_id,
  cte2.quantity,
  cte3.salesperson_name
FROM
  cte1
JOIN cte2 ON cte1.id = cte2.id
JOIN cte3 ON cte1.id = cte3.id
WHERE cte1.amount > 150
ORDER BY cte1.amount DESC;
