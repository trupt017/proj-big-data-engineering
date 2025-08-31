-- Drop old table if exists
IF OBJECT_ID('dbo.store_info_india', 'U') IS NOT NULL
    DROP TABLE dbo.store_info_india;
GO

-- Create table
CREATE TABLE dbo.store_info_india (
    store_id        BIGINT PRIMARY KEY,
    store_name      NVARCHAR(100),
    store_addr      NVARCHAR(200),
    store_district  NVARCHAR(100),
    store_state     NVARCHAR(100),
    store_country   NVARCHAR(100)
);
GO

-- Reference geo table (states + districts for India)
IF OBJECT_ID('tempdb..#geo_india') IS NOT NULL DROP TABLE #geo_india;
CREATE TABLE #geo_india (
    geo_id INT IDENTITY(1,1) PRIMARY KEY,
    state NVARCHAR(50),
    district NVARCHAR(50)
);

-- Insert sample states/districts for India
INSERT INTO #geo_india (state, district)
VALUES
('Maharashtra', 'Mumbai'),
('Maharashtra', 'Pune'),
('Maharashtra', 'Nagpur'),
('Karnataka', 'Bangalore'),
('Karnataka', 'Mysore'),
('Tamil Nadu', 'Chennai'),
('Tamil Nadu', 'Coimbatore'),
('Delhi', 'New Delhi'),
('Gujarat', 'Ahmedabad'),
('Gujarat', 'Surat'),
('West Bengal', 'Kolkata'),
('West Bengal', 'Darjeeling'),
('Rajasthan', 'Jaipur'),
('Rajasthan', 'Udaipur'),
('Uttar Pradesh', 'Lucknow'),
('Uttar Pradesh', 'Varanasi'),
('Telangana', 'Hyderabad'),
('Kerala', 'Kochi'),
('Kerala', 'Thiruvananthapuram'),
('Punjab', 'Amritsar');
GO

-- Generate 1,000,000 stores with random geo assignments
;WITH Numbers AS (
    SELECT TOP (1000000)
           ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n,
           ABS(CHECKSUM(NEWID())) % (SELECT COUNT(*) FROM #geo_india) + 1 AS rand_geo_id
    FROM sys.all_objects a
    CROSS JOIN sys.all_objects b
)
INSERT INTO dbo.store_info_india (store_id, store_name, store_addr, store_district, store_state, store_country)
SELECT 
    n AS store_id,
    CONCAT('Store_', n) AS store_name,
    CONCAT('Address_', n, ' Main Road') AS store_addr,
    g.district,
    g.state,
    'India' AS store_country
FROM Numbers n
JOIN #geo_india g ON g.geo_id = n.rand_geo_id;
GO

-- Check total rows
SELECT COUNT(*) AS total_stores FROM dbo.store_info_india;

-- Check random sample
SELECT TOP 20 * FROM dbo.store_info_india ORDER BY NEWID();
