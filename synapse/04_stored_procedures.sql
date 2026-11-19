-- RUN ONE BY ONE


-- Procedure to load dim_customer  @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

IF OBJECT_ID('dwh.usp_load_dim_customer', 'P') IS NOT NULL
    DROP PROCEDURE dwh.usp_load_dim_customer;

CREATE PROCEDURE dwh.usp_load_dim_customer
    @process_date DATE
AS
BEGIN
    SET NOCOUNT ON;

    IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;
    IF OBJECT_ID('tempdb..#changed') IS NOT NULL DROP TABLE #changed;

    CREATE TABLE #src (
        customer_id    INT NOT NULL,
        customer_name  NVARCHAR(200) NOT NULL,
        customer_email NVARCHAR(200) NOT NULL,
        city           NVARCHAR(100) NOT NULL,
        country        NVARCHAR(100) NOT NULL
    ) WITH (DISTRIBUTION = ROUND_ROBIN);

    CREATE TABLE #changed (customer_id INT NOT NULL) WITH (DISTRIBUTION = ROUND_ROBIN);

    -- daily customers
    INSERT INTO #src
    SELECT DISTINCT customer_id, customer_name, customer_email, city, country
    FROM staging.sales;

    BEGIN TRY
        BEGIN TRANSACTION;

        -- 1. changed clients
        INSERT INTO #changed
        SELECT c.customer_id
        FROM dwh.dim_customer c
        JOIN #src s ON s.customer_id = c.customer_id
        WHERE c.is_current = 1
          AND (   c.customer_name  <> s.customer_name
               OR c.customer_email <> s.customer_email
               OR c.city           <> s.city
               OR c.country        <> s.country);

        -- 2. closing older versions
        UPDATE dwh.dim_customer
        SET valid_to = @process_date, is_current = 0
        WHERE is_current = 1
          AND customer_id IN (SELECT customer_id FROM #changed);

        -- 3. inserting changed and new clients
        INSERT INTO dwh.dim_customer
            (customer_id, customer_name, customer_email, city, country, valid_from, valid_to, is_current)
        SELECT s.customer_id, s.customer_name, s.customer_email, s.city, s.country,
               @process_date, '9999-12-31', 1
        FROM #src s
        LEFT JOIN dwh.dim_customer c
               ON c.customer_id = s.customer_id AND c.is_current = 1
        WHERE c.customer_id IS NULL;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH;
END; 


-- Procedure to load dim_product  @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

IF OBJECT_ID('dwh.usp_load_dim_product', 'P') IS NOT NULL
    DROP PROCEDURE dwh.usp_load_dim_product;

CREATE PROCEDURE dwh.usp_load_dim_product
    @process_date DATE
AS
BEGIN
    SET NOCOUNT ON;

    IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;
    IF OBJECT_ID('tempdb..#changed') IS NOT NULL DROP TABLE #changed;

    CREATE TABLE #src (
        product_id   INT NOT NULL,
        product_name NVARCHAR(200) NOT NULL,
        category     NVARCHAR(100) NOT NULL,
        unit_price   DECIMAL(10,2) NOT NULL
    ) WITH (DISTRIBUTION = ROUND_ROBIN);

    CREATE TABLE #changed (product_id INT NOT NULL) WITH (DISTRIBUTION = ROUND_ROBIN);

    -- daily products
    INSERT INTO #src
    SELECT DISTINCT product_id, product_name, category, unit_price
    FROM staging.sales;

    BEGIN TRY
        BEGIN TRANSACTION;

        -- 1. changed products
        INSERT INTO #changed
        SELECT p.product_id
        FROM dwh.dim_product p
        JOIN #src s ON s.product_id = p.product_id
        WHERE p.is_current = 1
          AND (   p.product_name <> s.product_name
               OR p.category     <> s.category
               OR p.unit_price   <> s.unit_price);

        -- 2. closing older versions
        UPDATE dwh.dim_product
        SET valid_to = @process_date, is_current = 0
        WHERE is_current = 1
          AND product_id IN (SELECT product_id FROM #changed);

        -- 3. inserting changed and new products
        INSERT INTO dwh.dim_product
            (product_id, product_name, category, unit_price, valid_from, valid_to, is_current)
        SELECT s.product_id, s.product_name, s.category, s.unit_price,
               @process_date, '9999-12-31', 1
        FROM #src s
        LEFT JOIN dwh.dim_product p
               ON p.product_id = s.product_id AND p.is_current = 1
        WHERE p.product_id IS NULL;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH;
END;


-- Procedure to load fact_sales  @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

IF OBJECT_ID('dwh.usp_load_fact_sales', 'P') IS NOT NULL
    DROP PROCEDURE dwh.usp_load_fact_sales;

CREATE PROCEDURE dwh.usp_load_fact_sales
    @process_date DATE
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @date_key INT;
    SET @date_key = YEAR(@process_date) * 10000 + MONTH(@process_date) * 100 + DAY(@process_date);

    BEGIN TRY
        BEGIN TRANSACTION;

        
        DELETE FROM dwh.fact_sales WHERE date_key = @date_key;

        INSERT INTO dwh.fact_sales
            (order_id, customer_key, product_key, date_key, unit_price, quantity, discount, total_amount)
        SELECT s.order_id, c.customer_key, p.product_key, @date_key,
               s.unit_price, s.quantity, s.discount, s.total_amount
        FROM staging.sales s
        JOIN dwh.dim_customer c
              ON c.customer_id = s.customer_id
             AND s.order_date >= c.valid_from AND s.order_date < c.valid_to
        JOIN dwh.dim_product p
              ON p.product_id = s.product_id
             AND s.order_date >= p.valid_from AND s.order_date < p.valid_to;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH;
END;

