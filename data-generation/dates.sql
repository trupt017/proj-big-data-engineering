
DROP TABLE iF EXISTS dev_FMCG_db.dbo.CalendarDates;
CREATE TABLE dev_FMCG_db.dbo.CalendarDates (
    DateValue DATE NOT NULL
);

-- SQL Server 
WITH Dates AS (
    SELECT CAST(GETDATE() AS DATE) AS dt  -- Start from today
    UNION ALL
    SELECT DATEADD(DAY, 1, dt)
    FROM Dates
    WHERE dt < DATEADD(DAY, 364, CAST(GETDATE() AS DATE))
)
INSERT INTO dev_FMCG_db.dbo.CalendarDates (DateValue)
SELECT dt
FROM Dates
ORDER BY dt
OPTION (MAXRECURSION 365); -- SQL Server needs this to allow 365 recursions



SELECT * FROM dev_FMCG_db.dbo.CalendarDates ORDER BY DateValue;


