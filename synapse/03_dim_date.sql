IF NOT EXISTS (SELECT 1 FROM dwh.dim_date)
BEGIN
    INSERT INTO dwh.dim_date
        (date_key, full_date, [year], [month], month_name, [day], day_of_week, day_name, quarter)
    SELECT
        YEAR(d) * 10000 + MONTH(d) * 100 + DAY(d),
        d,
        YEAR(d),
        MONTH(d),
        DATENAME(month, d),
        DAY(d),
        DATEPART(weekday, d),
        DATENAME(weekday, d),
        DATEPART(quarter, d)
    FROM (
        SELECT TOP (1826)
            DATEADD(day, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1, CAST('2026-01-01' AS DATE)) AS d
        FROM sys.all_columns a CROSS JOIN sys.all_columns b
    ) x;
END;