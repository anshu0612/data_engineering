-- Please check the README for the assumptions regrading the queries

-- Query 1: list of our customers and their spending.
SELECT c.customer_name, coalesce(sum(r.price), 0) AS spending 
FROM 
  Cars AS r
  JOIN Transactions AS t ON r.serial_number = t.car_serial_number 
  RIGHT JOIN Customers AS c ON c.customer_id = t.customer_id 
GROUP BY 
  c.customer_id 
ORDER BY 
  spending DESC;

-- Query 2 top 3 car manufacturers that customers bought by sales (quantity) and 
-- the sales number for it in the current month

-- get top 3 sales 
WITH Top_3_Sales_Count AS (
  SELECT count(t.car_serial_number) 
  FROM 
    Transactions AS t 
    JOIN Cars AS r ON t.car_serial_number = r.serial_number 
  WHERE 
    date_part('month', t.transaction_datetime) = date_part('month', CURRENT_DATE) 
    AND date_part('year', t.transaction_datetime) = date_part('year', CURRENT_DATE) 
  GROUP BY 
    r.manufacturer_id 
  ORDER BY 
    count(t.car_serial_number) DESC 
  LIMIT 3
) 

SELECT 
  m.manufacturer_name, 
  count(t.car_serial_number) AS sales 
FROM 
  Transactions AS t 
  JOIN Cars r ON t.car_serial_number = r.serial_number 
  JOIN Manufacturers m ON m.manufacturer_id = r.manufacturer_id 
WHERE 
  date_part('month', t.transaction_datetime) = date_part('month', CURRENT_DATE) 
  AND date_part('year', t.transaction_datetime) = date_part('year', CURRENT_DATE) 
GROUP BY 
  m.manufacturer_id 
HAVING 
  count(t.car_serial_number) IN (SELECT * FROM Top_3_Sales_Count) 
ORDER BY 
  sales DESC;
