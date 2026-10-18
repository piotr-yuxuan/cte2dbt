SELECT id, name
FROM {{ source("dbo", "Customers") }} AS Customers
WHERE active = 1
