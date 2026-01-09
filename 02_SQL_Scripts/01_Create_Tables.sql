-- =====================================================================
-- InkWave Publishing Data Warehouse - Fact Tables DDL
-- Oracle 19c+ Compatible
-- Author: Data Engineering Team
-- Date: 2025-12-07
-- Purpose: Create fact tables with partitioning and optimization
-- =====================================================================

-- =====================================================================
-- 1. FACT_SALES - Transaction-Level Sales Fact Table
-- =====================================================================
CREATE TABLE FACT_SALES (
    sales_fact_key         NUMBER(15) NOT NULL,           -- Surrogate Key
    -- Foreign Keys to Dimensions
    time_key               NUMBER(8) NOT NULL,            -- FK to DIM_TIME
    product_key            NUMBER(10) NOT NULL,           -- FK to DIM_PRODUCT
    author_key             NUMBER(10) NOT NULL,           -- FK to DIM_AUTHOR
    dc_key                 NUMBER(10) NOT NULL,           -- FK to DIM_DISTRIBUTION_CENTER
    vendor_key             NUMBER(10) NOT NULL,           -- FK to DIM_VENDOR
    channel_key            NUMBER(10) NOT NULL,           -- FK to DIM_CHANNEL
    product_type_key       NUMBER(10) NOT NULL,           -- FK to DIM_PRODUCT_TYPE
    currency_key           NUMBER(10) NOT NULL,           -- FK to DIM_CURRENCY (original)
    -- Degenerate Dimensions (Transaction identifiers)
    sale_number            VARCHAR2(20) NOT NULL,         -- Natural transaction ID: S9612, S3981
    -- Measures (Additive)
    quantity_sold          NUMBER(10,2) NOT NULL,         -- TQty from source
    unit_price_original    NUMBER(12,2) NOT NULL,         -- Original currency
    unit_price_gbp         NUMBER(12,2) NOT NULL,         -- Converted to GBP
    discount_rate          NUMBER(5,4),                    -- 0.0000 to 0.2500
    discount_amount_gbp    NUMBER(12,2),                   -- Calculated discount in GBP
    gross_amount_gbp       NUMBER(12,2) NOT NULL,         -- Before discount
    net_amount_gbp         NUMBER(12,2) NOT NULL,         -- After discount
    -- Cost Measures
    print_run_qty          NUMBER(10) NOT NULL,           -- PD - Print run quantity
    binding_cost_gbp       NUMBER(12,2) NOT NULL,         -- BC - Binding cost
    unit_cost_gbp          NUMBER(12,2) NOT NULL,         -- Calculated: binding_cost / print_run
    total_cost_gbp         NUMBER(12,2) NOT NULL,         -- unit_cost * quantity_sold
    -- Profitability Measures
    gross_profit_gbp       NUMBER(12,2) NOT NULL,         -- net_amount - total_cost
    gross_margin_pct       NUMBER(5,2),                    -- (gross_profit / net_amount) * 100
    -- Quality Dimensions
    vendor_score           NUMBER(3,1),                    -- VScr from source (0.0-10.0)
    -- Audit columns
    source_system          VARCHAR2(20) DEFAULT 'RAW_SALES' NOT NULL,
    source_record_id       VARCHAR2(50),                   -- For data lineage
    load_date              DATE DEFAULT SYSDATE NOT NULL,
    load_batch_id          NUMBER(10),
    created_date           DATE DEFAULT SYSDATE NOT NULL,
    created_by             VARCHAR2(50) DEFAULT USER NOT NULL,
    CONSTRAINT pk_fact_sales PRIMARY KEY (sales_fact_key),
    CONSTRAINT uk_fact_sales_trans UNIQUE (sale_number),
    -- Foreign Key Constraints
    CONSTRAINT fk_sales_time FOREIGN KEY (time_key) REFERENCES DIM_TIME(time_key),
    CONSTRAINT fk_sales_product FOREIGN KEY (product_key) REFERENCES DIM_PRODUCT(product_key),
    CONSTRAINT fk_sales_author FOREIGN KEY (author_key) REFERENCES DIM_AUTHOR(author_key),
    CONSTRAINT fk_sales_dc FOREIGN KEY (dc_key) REFERENCES DIM_DISTRIBUTION_CENTER(dc_key),
    CONSTRAINT fk_sales_vendor FOREIGN KEY (vendor_key) REFERENCES DIM_VENDOR(vendor_key),
    CONSTRAINT fk_sales_channel FOREIGN KEY (channel_key) REFERENCES DIM_CHANNEL(channel_key),
    CONSTRAINT fk_sales_product_type FOREIGN KEY (product_type_key) REFERENCES DIM_PRODUCT_TYPE(product_type_key),
    CONSTRAINT fk_sales_currency FOREIGN KEY (currency_key) REFERENCES DIM_CURRENCY(currency_key),
    -- Business Rule Constraints
    CONSTRAINT ck_sales_qty_positive CHECK (quantity_sold > 0),
    CONSTRAINT ck_sales_price_positive CHECK (unit_price_gbp > 0),
    CONSTRAINT ck_sales_discount CHECK (discount_rate BETWEEN 0 AND 0.25)
) TABLESPACE USERS
  PCTFREE 10
  COMPRESS FOR OLTP
  PARTITION BY RANGE (time_key)
  INTERVAL (NUMTOYMINTERVAL(1,'MONTH'))
  (
    PARTITION p_sales_2018 VALUES LESS THAN (20190101),
    PARTITION p_sales_2019 VALUES LESS THAN (20200101),
    PARTITION p_sales_2020 VALUES LESS THAN (20210101),
    PARTITION p_sales_2021 VALUES LESS THAN (20220101),
    PARTITION p_sales_2022 VALUES LESS THAN (20230101),
    PARTITION p_sales_2023 VALUES LESS THAN (20240101),
    PARTITION p_sales_2024 VALUES LESS THAN (20250101)
  );

-- Indexes for FACT_SALES
CREATE INDEX idx_fact_sales_time ON FACT_SALES(time_key) LOCAL;
CREATE INDEX idx_fact_sales_product ON FACT_SALES(product_key) LOCAL;
CREATE INDEX idx_fact_sales_author ON FACT_SALES(author_key) LOCAL;
CREATE INDEX idx_fact_sales_dc ON FACT_SALES(dc_key) LOCAL;
CREATE INDEX idx_fact_sales_channel ON FACT_SALES(channel_key) LOCAL;
CREATE INDEX idx_fact_sales_vendor ON FACT_SALES(vendor_key) LOCAL;
CREATE INDEX idx_fact_sales_composite ON FACT_SALES(time_key, product_key, channel_key) LOCAL;
CREATE INDEX idx_fact_sales_profitability ON FACT_SALES(time_key, gross_margin_pct) LOCAL;

-- Bitmap indexes for low-cardinality columns
CREATE BITMAP INDEX idx_fact_sales_ptype ON FACT_SALES(product_type_key) LOCAL;
CREATE BITMAP INDEX idx_fact_sales_curr ON FACT_SALES(currency_key) LOCAL;

COMMENT ON TABLE FACT_SALES IS 'Transaction-level sales fact table with profitability metrics and partitioning by time';

-- =====================================================================
-- 2. FACT_DAILY_OPERATIONS - Daily Aggregated Operations Fact Table
-- =====================================================================
CREATE TABLE FACT_DAILY_OPERATIONS (
    daily_ops_fact_key     NUMBER(15) NOT NULL,           -- Surrogate Key
    -- Foreign Keys to Dimensions
    time_key               NUMBER(8) NOT NULL,            -- FK to DIM_TIME
    product_key            NUMBER(10) NOT NULL,           -- FK to DIM_PRODUCT (via EdID mapping)
    dc_key                 NUMBER(10) NOT NULL,           -- FK to DIM_DISTRIBUTION_CENTER
    vendor_key             NUMBER(10) NOT NULL,           -- FK to DIM_VENDOR
    product_type_key       NUMBER(10) NOT NULL,           -- FK to DIM_PRODUCT_TYPE
    -- Operational Measures (Additive)
    print_run_qty          NUMBER(10) NOT NULL,           -- PD - Daily print run
    binding_cost_gbp       NUMBER(12,2) NOT NULL,         -- BC - Daily binding cost
    units_sold             NUMBER(10) NOT NULL,           -- US - Units sold
    returns_qty            NUMBER(10) NOT NULL,           -- RV - Returns (can be negative in source)
    net_units              NUMBER(10) NOT NULL,           -- units_sold - returns_qty
    revenue_gbp            NUMBER(12,2) NOT NULL,         -- Rev - Daily revenue
    -- Cost Measures
    unit_binding_cost      NUMBER(12,4) NOT NULL,         -- binding_cost / print_run
    cost_of_goods_sold     NUMBER(12,2) NOT NULL,         -- unit_binding_cost * units_sold
    -- Profitability Measures
    gross_profit_gbp       NUMBER(12,2) NOT NULL,         -- revenue - cost_of_goods_sold
    gross_margin_pct       NUMBER(5,2),                    -- (gross_profit / revenue) * 100
    -- Inventory Measures
    print_run_utilization  NUMBER(5,2),                    -- (units_sold / print_run) * 100
    return_rate_pct        NUMBER(5,2),                    -- (returns / units_sold) * 100
    -- Environmental Measures (Non-Additive)
    temperature_celsius    NUMBER(4,1),                    -- Tmp - Temperature
    humidity_pct           NUMBER(3),                      -- Hmd - Humidity percentage
    -- Quality Measures
    vendor_score           NUMBER(3,1),                    -- VScr - Vendor score (0.0-10.0)
    -- Degenerate Dimensions
    notes                  VARCHAR2(500),                  -- Optional notes from source
    -- Audit columns
    source_system          VARCHAR2(20) DEFAULT 'RAW_DAILY' NOT NULL,
    source_record_id       VARCHAR2(50),                   -- For data lineage
    load_date              DATE DEFAULT SYSDATE NOT NULL,
    load_batch_id          NUMBER(10),
    created_date           DATE DEFAULT SYSDATE NOT NULL,
    created_by             VARCHAR2(50) DEFAULT USER NOT NULL,
    CONSTRAINT pk_fact_daily_ops PRIMARY KEY (daily_ops_fact_key),
    CONSTRAINT uk_fact_daily_ops UNIQUE (time_key, dc_key, product_type_key),
    -- Foreign Key Constraints
    CONSTRAINT fk_daily_time FOREIGN KEY (time_key) REFERENCES DIM_TIME(time_key),
    CONSTRAINT fk_daily_product FOREIGN KEY (product_key) REFERENCES DIM_PRODUCT(product_key),
    CONSTRAINT fk_daily_dc FOREIGN KEY (dc_key) REFERENCES DIM_DISTRIBUTION_CENTER(dc_key),
    CONSTRAINT fk_daily_vendor FOREIGN KEY (vendor_key) REFERENCES DIM_VENDOR(vendor_key),
    CONSTRAINT fk_daily_product_type FOREIGN KEY (product_type_key) REFERENCES DIM_PRODUCT_TYPE(product_type_key),
    -- Business Rule Constraints
    CONSTRAINT ck_daily_print_run CHECK (print_run_qty >= 0),
    CONSTRAINT ck_daily_units_sold CHECK (units_sold >= 0),
    CONSTRAINT ck_daily_revenue CHECK (revenue_gbp >= 0),
    CONSTRAINT ck_daily_temp CHECK (temperature_celsius BETWEEN -50 AND 60),
    CONSTRAINT ck_daily_humidity CHECK (humidity_pct BETWEEN 0 AND 100),
    CONSTRAINT ck_daily_vendor_score CHECK (vendor_score BETWEEN 0.0 AND 10.0)
) TABLESPACE USERS
  PCTFREE 10
  COMPRESS FOR OLTP
  PARTITION BY RANGE (time_key)
  INTERVAL (NUMTOYMINTERVAL(1,'MONTH'))
  (
    PARTITION p_daily_2018 VALUES LESS THAN (20190101),
    PARTITION p_daily_2019 VALUES LESS THAN (20200101),
    PARTITION p_daily_2020 VALUES LESS THAN (20210101),
    PARTITION p_daily_2021 VALUES LESS THAN (20220101),
    PARTITION p_daily_2022 VALUES LESS THAN (20230101),
    PARTITION p_daily_2023 VALUES LESS THAN (20240101),
    PARTITION p_daily_2024 VALUES LESS THAN (20250101)
  );

-- Indexes for FACT_DAILY_OPERATIONS
CREATE INDEX idx_fact_daily_time ON FACT_DAILY_OPERATIONS(time_key) LOCAL;
CREATE INDEX idx_fact_daily_dc ON FACT_DAILY_OPERATIONS(dc_key) LOCAL;
CREATE INDEX idx_fact_daily_product ON FACT_DAILY_OPERATIONS(product_key) LOCAL;
CREATE INDEX idx_fact_daily_vendor ON FACT_DAILY_OPERATIONS(vendor_key) LOCAL;
CREATE INDEX idx_fact_daily_composite ON FACT_DAILY_OPERATIONS(time_key, dc_key, product_type_key) LOCAL;
CREATE INDEX idx_fact_daily_returns ON FACT_DAILY_OPERATIONS(time_key, return_rate_pct) LOCAL;

-- Bitmap indexes
CREATE BITMAP INDEX idx_fact_daily_ptype ON FACT_DAILY_OPERATIONS(product_type_key) LOCAL;

COMMENT ON TABLE FACT_DAILY_OPERATIONS IS 'Daily aggregated operations fact table with inventory and environmental metrics';

-- =====================================================================
-- 3. Create Sequences for Fact Tables
-- =====================================================================
CREATE SEQUENCE seq_sales_fact_key START WITH 100000 INCREMENT BY 1 CACHE 1000;
CREATE SEQUENCE seq_daily_ops_fact_key START WITH 100000 INCREMENT BY 1 CACHE 1000;
CREATE SEQUENCE seq_load_batch_id START WITH 1 INCREMENT BY 1 NOCACHE;

-- =====================================================================
-- 4. Create Statistics Preferences for Fact Tables
-- =====================================================================
BEGIN
    -- Enable incremental statistics for large partitioned tables
    DBMS_STATS.SET_TABLE_PREFS(NULL, 'FACT_SALES', 'INCREMENTAL', 'TRUE');
    DBMS_STATS.SET_TABLE_PREFS(NULL, 'FACT_SALES', 'PUBLISH', 'TRUE');
    DBMS_STATS.SET_TABLE_PREFS(NULL, 'FACT_DAILY_OPERATIONS', 'INCREMENTAL', 'TRUE');
    DBMS_STATS.SET_TABLE_PREFS(NULL, 'FACT_DAILY_OPERATIONS', 'PUBLISH', 'TRUE');
    
    -- Set degree of parallelism for statistics gathering
    DBMS_STATS.SET_TABLE_PREFS(NULL, 'FACT_SALES', 'DEGREE', '4');
    DBMS_STATS.SET_TABLE_PREFS(NULL, 'FACT_DAILY_OPERATIONS', 'DEGREE', '4');
END;
/

-- =====================================================================
-- 5. Create Materialized Views for Common Aggregations
-- =====================================================================

-- MV 1: Monthly Sales Summary by Product and Channel
CREATE MATERIALIZED VIEW MV_MONTHLY_SALES_SUMMARY
BUILD IMMEDIATE
REFRESH COMPLETE ON DEMAND
ENABLE QUERY REWRITE
AS
SELECT 
    t.year,
    t.month_number,
    t.month_name,
    p.edition_id,
    p.product_title,
    c.channel_name,
    pt.product_type_name,
    COUNT(DISTINCT f.sales_fact_key) as transaction_count,
    SUM(f.quantity_sold) as total_quantity,
    SUM(f.net_amount_gbp) as total_revenue_gbp,
    SUM(f.total_cost_gbp) as total_cost_gbp,
    SUM(f.gross_profit_gbp) as total_profit_gbp,
    AVG(f.gross_margin_pct) as avg_margin_pct,
    AVG(f.discount_rate) as avg_discount_rate
FROM FACT_SALES f
JOIN DIM_TIME t ON f.time_key = t.time_key
JOIN DIM_PRODUCT p ON f.product_key = p.product_key AND p.is_current = 'Y'
JOIN DIM_CHANNEL c ON f.channel_key = c.channel_key
JOIN DIM_PRODUCT_TYPE pt ON f.product_type_key = pt.product_type_key
GROUP BY 
    t.year, t.month_number, t.month_name,
    p.edition_id, p.product_title,
    c.channel_name, pt.product_type_name;

CREATE INDEX idx_mv_monthly_sales ON MV_MONTHLY_SALES_SUMMARY(year, month_number, edition_id);

-- MV 2: Daily Operations Summary by Distribution Center
CREATE MATERIALIZED VIEW MV_DAILY_DC_PERFORMANCE
BUILD IMMEDIATE
REFRESH COMPLETE ON DEMAND
ENABLE QUERY REWRITE
AS
SELECT 
    t.full_date,
    t.year,
    t.month_number,
    dc.station_code,
    dc.station_name,
    dc.station_region,
    pt.product_type_name,
    SUM(f.units_sold) as total_units_sold,
    SUM(f.returns_qty) as total_returns,
    SUM(f.net_units) as net_units,
    SUM(f.revenue_gbp) as total_revenue_gbp,
    SUM(f.gross_profit_gbp) as total_profit_gbp,
    AVG(f.return_rate_pct) as avg_return_rate,
    AVG(f.print_run_utilization) as avg_utilization,
    AVG(f.vendor_score) as avg_vendor_score,
    AVG(f.temperature_celsius) as avg_temperature,
    AVG(f.humidity_pct) as avg_humidity
FROM FACT_DAILY_OPERATIONS f
JOIN DIM_TIME t ON f.time_key = t.time_key
JOIN DIM_DISTRIBUTION_CENTER dc ON f.dc_key = dc.dc_key AND dc.is_current = 'Y'
JOIN DIM_PRODUCT_TYPE pt ON f.product_type_key = pt.product_type_key
GROUP BY 
    t.full_date, t.year, t.month_number,
    dc.station_code, dc.station_name, dc.station_region,
    pt.product_type_name;

CREATE INDEX idx_mv_daily_dc ON MV_DAILY_DC_PERFORMANCE(full_date, station_code);

-- MV 3: Quarterly Profitability by Author and Region
CREATE MATERIALIZED VIEW MV_QUARTERLY_AUTHOR_PROFIT
BUILD IMMEDIATE
REFRESH COMPLETE ON DEMAND
ENABLE QUERY REWRITE
AS
SELECT 
    t.year,
    t.quarter_number,
    t.quarter_name,
    a.author_id,
    a.full_name as author_name,
    a.primary_genre,
    dc.station_region,
    COUNT(DISTINCT f.sales_fact_key) as transaction_count,
    SUM(f.quantity_sold) as total_quantity,
    SUM(f.net_amount_gbp) as total_revenue_gbp,
    SUM(f.total_cost_gbp) as total_cost_gbp,
    SUM(f.gross_profit_gbp) as total_profit_gbp,
    AVG(f.gross_margin_pct) as avg_margin_pct,
    SUM(f.net_amount_gbp) / NULLIF(COUNT(DISTINCT p.edition_id), 0) as revenue_per_edition
FROM FACT_SALES f
JOIN DIM_TIME t ON f.time_key = t.time_key
JOIN DIM_AUTHOR a ON f.author_key = a.author_key AND a.is_current = 'Y'
JOIN DIM_PRODUCT p ON f.product_key = p.product_key AND p.is_current = 'Y'
JOIN DIM_DISTRIBUTION_CENTER dc ON f.dc_key = dc.dc_key AND dc.is_current = 'Y'
GROUP BY 
    t.year, t.quarter_number, t.quarter_name,
    a.author_id, a.full_name, a.primary_genre,
    dc.station_region;

CREATE INDEX idx_mv_qtr_author ON MV_QUARTERLY_AUTHOR_PROFIT(year, quarter_number, author_id);

-- MV 4: Vendor Cost Analysis
CREATE MATERIALIZED VIEW MV_VENDOR_COST_ANALYSIS
BUILD IMMEDIATE
REFRESH COMPLETE ON DEMAND
ENABLE QUERY REWRITE
AS
SELECT 
    t.year,
    t.quarter_number,
    v.vendor_id,
    v.vendor_name,
    COUNT(DISTINCT f.daily_ops_fact_key) as operation_days,
    SUM(f.print_run_qty) as total_print_run,
    SUM(f.binding_cost_gbp) as total_binding_cost,
    AVG(f.unit_binding_cost) as avg_unit_cost,
    AVG(f.vendor_score) as avg_vendor_score,
    STDDEV(f.unit_binding_cost) as cost_stddev,
    (STDDEV(f.unit_binding_cost) / NULLIF(AVG(f.unit_binding_cost), 0)) * 100 as cost_variance_pct
FROM FACT_DAILY_OPERATIONS f
JOIN DIM_TIME t ON f.time_key = t.time_key
JOIN DIM_VENDOR v ON f.vendor_key = v.vendor_key AND v.is_current = 'Y'
GROUP BY 
    t.year, t.quarter_number,
    v.vendor_id, v.vendor_name;

CREATE INDEX idx_mv_vendor_cost ON MV_VENDOR_COST_ANALYSIS(year, quarter_number, vendor_id);

-- MV 5: Channel Performance Comparison
CREATE MATERIALIZED VIEW MV_CHANNEL_PERFORMANCE
BUILD IMMEDIATE
REFRESH COMPLETE ON DEMAND
ENABLE QUERY REWRITE
AS
SELECT 
    t.year,
    t.month_number,
    c.channel_code,
    c.channel_name,
    c.channel_type,
    pt.product_type_name,
    COUNT(DISTINCT f.sales_fact_key) as transaction_count,
    SUM(f.quantity_sold) as total_quantity,
    SUM(f.gross_amount_gbp) as gross_revenue_gbp,
    SUM(f.discount_amount_gbp) as total_discount_gbp,
    SUM(f.net_amount_gbp) as net_revenue_gbp,
    SUM(f.gross_profit_gbp) as total_profit_gbp,
    AVG(f.gross_margin_pct) as avg_margin_pct,
    AVG(f.discount_rate) as avg_discount_rate,
    SUM(f.net_amount_gbp) / NULLIF(COUNT(DISTINCT f.sales_fact_key), 0) as avg_transaction_value
FROM FACT_SALES f
JOIN DIM_TIME t ON f.time_key = t.time_key
JOIN DIM_CHANNEL c ON f.channel_key = c.channel_key
JOIN DIM_PRODUCT_TYPE pt ON f.product_type_key = pt.product_type_key
GROUP BY 
    t.year, t.month_number,
    c.channel_code, c.channel_name, c.channel_type,
    pt.product_type_name;

CREATE INDEX idx_mv_channel_perf ON MV_CHANNEL_PERFORMANCE(year, month_number, channel_code);

-- =====================================================================
-- 6. Grant Permissions
-- =====================================================================
-- GRANT SELECT ON FACT_SALES TO role_bi_users;
-- GRANT SELECT ON FACT_DAILY_OPERATIONS TO role_bi_users;
-- GRANT SELECT ON MV_MONTHLY_SALES_SUMMARY TO role_bi_users;
-- GRANT SELECT ON MV_DAILY_DC_PERFORMANCE TO role_bi_users;
-- GRANT SELECT ON MV_QUARTERLY_AUTHOR_PROFIT TO role_bi_users;
-- GRANT SELECT ON MV_VENDOR_COST_ANALYSIS TO role_bi_users;
-- GRANT SELECT ON MV_CHANNEL_PERFORMANCE TO role_bi_users;

COMMIT;

-- =====================================================================
-- End of Fact Tables DDL
-- =====================================================================