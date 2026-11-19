-- Setup script. It can run multiple times: it creates only the missing tables
-- and doesn't modify or delete anything from the existing tables. 

-- STAGING: same structure as the PARQUET export from gold 
IF OBJECT_ID('staging.sales', 'U') IS NULL
BEGIN
    CREATE TABLE staging.sales (
        order_id       INT,
        order_date     DATE,
        [year]         INT,
        [month]        INT,
        [day]          INT,
        customer_id    INT,
        customer_name  NVARCHAR(200),
        customer_email NVARCHAR(200),
        city           NVARCHAR(100),
        country        NVARCHAR(100),
        product_id     INT,
        product_name   NVARCHAR(200),
        category       NVARCHAR(100),
        unit_price     DECIMAL(10,2),
        quantity       INT,
        discount       DECIMAL(4,2),
        total_amount   DECIMAL(12,2)
    )
    WITH (DISTRIBUTION = ROUND_ROBIN, HEAP);
END;

-- Customer Dimension Table (SCD2)
IF OBJECT_ID('dwh.dim_customer', 'U') IS NULL
BEGIN
    CREATE TABLE dwh.dim_customer (
        customer_key   INT IDENTITY(1,1) NOT NULL,
        customer_id    INT NOT NULL,
        customer_name  NVARCHAR(200) NOT NULL,
        customer_email NVARCHAR(200) NOT NULL,
        city           NVARCHAR(100) NOT NULL,
        country        NVARCHAR(100) NOT NULL,
        valid_from     DATE NOT NULL,
        valid_to       DATE NOT NULL,
        is_current     BIT  NOT NULL,
        CONSTRAINT pk_dim_customer PRIMARY KEY NONCLUSTERED (customer_key) NOT ENFORCED
    )
    WITH (DISTRIBUTION = REPLICATE);
END;

-- Product Dimension Table (SCD2)
IF OBJECT_ID('dwh.dim_product', 'U') IS NULL
BEGIN
    CREATE TABLE dwh.dim_product (
        product_key    INT IDENTITY(1,1) NOT NULL,
        product_id     INT NOT NULL,
        product_name   NVARCHAR(200) NOT NULL,
        category       NVARCHAR(100) NOT NULL,
        unit_price     DECIMAL(10,2) NOT NULL,
        valid_from     DATE NOT NULL,
        valid_to       DATE NOT NULL,
        is_current     BIT  NOT NULL,
        CONSTRAINT pk_dim_product PRIMARY KEY NONCLUSTERED (product_key) NOT ENFORCED
    )
    WITH (DISTRIBUTION = REPLICATE);
END;

-- Date Dimension Table
IF OBJECT_ID('dwh.dim_date', 'U') IS NULL
BEGIN
    CREATE TABLE dwh.dim_date (
        date_key     INT NOT NULL,
        full_date    DATE NOT NULL,
        [year]       INT NOT NULL,
        [month]      INT NOT NULL,
        month_name   NVARCHAR(20) NOT NULL,
        [day]        INT NOT NULL,
        day_of_week  INT NOT NULL,
        day_name     NVARCHAR(20) NOT NULL,
        quarter      INT NOT NULL,
        CONSTRAINT pk_dim_date PRIMARY KEY NONCLUSTERED (date_key) NOT ENFORCED
    )
    WITH (DISTRIBUTION = REPLICATE);
END;

-- Fact Table
IF OBJECT_ID('dwh.fact_sales', 'U') IS NULL
BEGIN
    CREATE TABLE dwh.fact_sales (
        order_id      INT NOT NULL,
        customer_key  INT NOT NULL,
        product_key   INT NOT NULL,
        date_key      INT NOT NULL,
        unit_price    DECIMAL(10,2) NOT NULL,
        quantity      INT NOT NULL,
        discount      DECIMAL(4,2) NOT NULL,
        total_amount  DECIMAL(12,2) NOT NULL
    )
    WITH (DISTRIBUTION = ROUND_ROBIN);
END;