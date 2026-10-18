SELECT id, name, amount
FROM {{ source("main", "sales") }} AS sales
WHERE amount > 100
