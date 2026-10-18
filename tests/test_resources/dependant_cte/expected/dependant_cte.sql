SELECT cte1.name, cte1.amount, cte2.product_id, cte2.quantity, cte3.salesperson_name
FROM {{ ref("dependant_cte__cte1") }} AS cte1
JOIN {{ ref("dependant_cte__cte2") }} AS cte2 ON cte1.id = cte2.id
JOIN {{ ref("dependant_cte__cte3") }} AS cte3 ON cte1.id = cte3.id
WHERE cte1.amount > 150
ORDER BY cte1.amount DESC
