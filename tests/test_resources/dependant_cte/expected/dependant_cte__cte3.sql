SELECT o.id, o.customer_id, e.name AS salesperson_name
FROM {{ source("main", "orders") }} AS o
JOIN {{ source("main", "employees") }} AS e ON o.salesperson_id = e.id
JOIN {{ ref("dependant_cte__cte2") }} AS cte2 ON o.id = cte2.id /* Using cte2 here */
