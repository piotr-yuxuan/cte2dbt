SELECT *
FROM (
  SELECT c.id, SUM(Orders.amount)
  FROM {{ ref("vOrders__active_customers") }} AS c
  JOIN {{ source("dbo", "Orders") }} AS Orders WITH (NOLOCK) ON c.id = Orders.customer_id
  GROUP BY c.id
) AS totals(customer_id, total)
