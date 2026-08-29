SELECT
    o.customer_id::int AS customer_id,
    c.name,
    SUM(o.amount) AS total
FROM {schema}.orders o
JOIN customers c ON o.customer_id = c.id
WHERE o.status = 'paid'
GROUP BY o.customer_id, c.name
HAVING SUM(o.amount) > 100
