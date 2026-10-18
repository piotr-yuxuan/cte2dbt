SELECT o.id, o.customer_id, s.name AS salesperson_name
FROM {{ source("main", "orders") }} AS o
JOIN {{ source("main", "employees") }} AS s ON o.salesperson_id = s.id
