CREATE TABLE clean_data AS
SELECT 
    Invoice,
    StockCode,
    Description,
    Quantity,
    InvoiceDate,
    Price,
    CustomerID,
    Country,
    (Quantity * Price) AS Revenue
FROM raw_data
WHERE CustomerID IS NOT NULL
    AND Quantity > 0
    AND Price > 0;

CREATE TABLE rfm_analysis AS
SELECT
    CustomerID, 
    MAX(InvoiceDate) as last_purchase_date,
    DATEDIFF ((SELECT MAX(InvoiceDate) FROM clean_data), MAX(InvoiceDate)) as Recency,
    COUNT(DISTINCT Invoice) as Frequency,
    SUM(Revenue) as Monetary
FROM clean_data
GROUP BY CustomerID;