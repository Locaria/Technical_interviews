SELECT DISTINCT p.product_id, p.product_name, p.category, .price, p.availability, p.brand, p.last_updated, i.stock_quantity, i.restock_date, i.warehouse_location, (i.stock_quantity / p.price) AS stock_to_price_ratio,
CASE WHEN i.stock_quantity > 100 THEN 'High Stock'
WHEN i.stock_quantity BETWEEN 50 AND 100 THEN 'Medium Stock'
ELSE 'Low Stock' END AS stock_level, p.price * 1.1 AS adjusted_price
FROM product_feed p, inventory i
WHERE p.product_id = i.product_id
AND p.availability = 'in stock'
AND p.price > 0
AND p.category IN ('Electronics', 'Clothing', 'Toys')
AND i.stock_quantity > 30
AND p.last_updated > '2024-01-01'
GROUP BY p.product_id, p.product_name, p.category, p.price, p.availability, p.brand, p.last_updated, i.stock_quantity, i.restock_date, i.warehouse_location
ORDER BY stock_level, adjusted_price DESC;
