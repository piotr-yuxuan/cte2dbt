WITH
  cte1 AS (
    SELECT id, name, amount
    FROM sales
    WHERE amount > 100
  ),
  cte2 AS (
    SELECT o.id, o.product_id, oi.quantity
    FROM (SELECT cte1, *
          FROM orders o
          FULL OUTER JOIN cte1 ON o.id = cte1.id) o
    JOIN order_items oi ON o.id = oi.order_id
    WHERE oi.quantity > 5
  ),
  cte3 AS (
    SELECT o.id, o.customer_id, e.name AS salesperson_name
    FROM orders o
    JOIN db.schema.employees e ON o.salesperson_id = e.id
    JOIN cte2 ON o.id = cte2.id  -- Using cte2 here
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
