CREATE EXTERNAL TABLE orders ( order_id INT,
order_date TIMESTAMP, order_customer_id INT, order_status STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION 's3://retaildbbucket-tanaya/orders';

CREATE EXTERNAL TABLE order_items ( order_item_id INT,
order_item_order_id INT, order_item_product_id INT, order_item_quantity INT, order_item_subtotal FLOAT, order_item_product_price FLOAT
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION 's3://retaildbbucket-tanaya/order_items';

CREATE EXTERNAL TABLE products ( product_id INT,
product_category_id INT, product_name STRING, product_description STRING, product_price FLOAT, product_image STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION 's3://retaildbbucket-tanaya/products';


CREATE TABLE daily_product_revenue ( order_date TIMESTAMP, product_name STRING,
revenue FLOAT
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION 's3://retaildbbucket-tanaya/daily_product_revenue';



INSERT OVERWRITE TABLE daily_product_revenue SELECT order_date, product_name, round(sum(order_item_subtotal), 2) AS revenue FROM orders JOIN order_items
ON order_id = order_item_order_id
JOIN products
ON product_id = order_item_product_id
WHERE order_status IN ('COMPLETE', 'CLOSED') GROUP BY order_date, product_name
ORDER BY order_date, revenue DESC;
