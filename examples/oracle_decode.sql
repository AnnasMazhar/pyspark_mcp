SELECT
    o.order_id,
    DECODE(o.status, 1, 'active', 0, 'inactive', 2, 'hold', 'unknown') AS status_label,
    DECODE(c.region, 'US', 'domestic', 'CA', 'domestic', 'intl') AS geo,
    COALESCE(c.email, 'none') AS email
FROM orders o
JOIN customers c ON o.customer_id = c.id
WHERE o.order_date >= DATE '2024-01-01'
