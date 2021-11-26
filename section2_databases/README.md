SELECT customer_name, sum(price)
FROM Transcations t
JOIN Car c ON t.Serial_Number = c.Car_Serial_Number
JOIN Manufacturer m on m.Manufacturer_ID = c.Manufacturer_ID
WHERE m.Manufacturer_Name = "X"
GROUPBY Customer_Name


WITH TOP_3_SALES_COUNT AS (
    SELECT distinct(count(Car_Serial_Number))
    FROM Transcations t
    JOIN Car c ON t.Serial_Number = c.Car_Serial_Number
    WHERE MONTH(t.Datetime) = CURR_MONTH
    GROUPBY c.Manufacturer_ID
    LIMIT 3
)

SELECT m.Manufacturer_ID, m.Manufacturer_Name, count(t.Car_Serial_Number) AS sales
FROM Transcations t
JOIN Car c ON t.Serial_Number = c.Car_Serial_Number
JOIN Manufacturer m on m.Manufacturer_ID = c.Manufacturer_ID
WHERE MONTH(t.Datetime) = CURR_MONTH
GROUPBY m.Manufacturer_ID
HAVING count(t.Car_Serial_Number) IN TOP_3_SALES_COUNT

https://docs.docker.com/samples/postgresql_service/


