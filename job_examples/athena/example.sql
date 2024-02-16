WITH billing AS (
SELECT * FROM <raw.Billing table>
),
customers AS (
SELECT * FROM <raw.Customers table>
)
SELECT b.CustomerID, c.FirstName, c.LastName, SUM(cast(b.Amount as double)) AS TotalBilled
FROM billing b
JOIN customers c ON b.CustomerID = c.CustomerID
GROUP BY b.CustomerID, c.FirstName, c.LastName
ORDER BY TotalBilled DESC
