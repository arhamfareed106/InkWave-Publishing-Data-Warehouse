-- =====================================================================
-- InkWave Publishing Data Warehouse - Dimension Tables DDL
-- Oracle 19c+ Compatible
-- Author: Data Engineering Team
-- Date: 2025-12-07
-- Purpose: Create all dimension tables with SCD Type 2 support
-- =====================================================================

-- =====================================================================
-- 1. DIM_TIME - Complete Calendar Intelligence
-- =====================================================================
CREATE TABLE DIM_TIME (
    time_key                NUMBER(8) NOT NULL,           -- Surrogate Key: YYYYMMDD
    full_date              DATE NOT NULL,                 -- Natural key
    day_of_week            NUMBER(1) NOT NULL,           -- 1=Monday, 7=Sunday
    day_of_week_name       VARCHAR2(10) NOT NULL,        -- Monday, Tuesday, etc.
    day_of_month           NUMBER(2) NOT NULL,           -- 1-31
    day_of_year            NUMBER(3) NOT NULL,           -- 1-366
    week_of_year           NUMBER(2) NOT NULL,           -- 1-53 (ISO week)
    month_number           NUMBER(2) NOT NULL,           -- 1-12
    month_name             VARCHAR2(10) NOT NULL,        -- January, February, etc.
    month_abbr             VARCHAR2(3) NOT NULL,         -- Jan, Feb, etc.
    quarter_number         NUMBER(1) NOT NULL,           -- 1-4
    quarter_name           VARCHAR2(2) NOT NULL,         -- Q1, Q2, Q3, Q4
    year                   NUMBER(4) NOT NULL,           -- 2018-2025
    fiscal_year            NUMBER(4) NOT NULL,           -- UK fiscal year (Apr-Mar)
    fiscal_quarter         NUMBER(1) NOT NULL,           -- Fiscal Q1-Q4
    fiscal_month           NUMBER(2) NOT NULL,           -- Fiscal month 1-12
    is_weekend             CHAR(1) NOT NULL,             -- Y/N
    is_uk_holiday          CHAR(1) NOT NULL,             -- Y/N
    uk_holiday_name        VARCHAR2(50),                 -- Name of holiday
    is_month_start         CHAR(1) NOT NULL,             -- Y/N
    is_month_end           CHAR(1) NOT NULL,             -- Y/N
    is_quarter_start       CHAR(1) NOT NULL,             -- Y/N
    is_quarter_end         CHAR(1) NOT NULL,             -- Y/N
    is_year_start          CHAR(1) NOT NULL,             -- Y/N
    is_year_end            CHAR(1) NOT NULL,             -- Y/N
    week_start_date        DATE NOT NULL,                 -- Monday of the week
    week_end_date          DATE NOT NULL,                 -- Sunday of the week
    month_year_name        VARCHAR2(20) NOT NULL,        -- Jan 2023, Feb 2023
    quarter_year_name      VARCHAR2(10) NOT NULL,        -- Q1 2023, Q2 2023
    days_in_month          NUMBER(2) NOT NULL,           -- 28-31
    season                 VARCHAR2(10) NOT NULL,         -- Spring, Summer, Autumn, Winter
    CONSTRAINT pk_dim_time PRIMARY KEY (time_key),
    CONSTRAINT uk_dim_time_date UNIQUE (full_date),
    CONSTRAINT ck_dim_time_dow CHECK (day_of_week BETWEEN 1 AND 7),
    CONSTRAINT ck_dim_time_month CHECK (month_number BETWEEN 1 AND 12),
    CONSTRAINT ck_dim_time_quarter CHECK (quarter_number BETWEEN 1 AND 4)
) TABLESPACE USERS
  PCTFREE 5
  COMPRESS FOR OLTP;

-- Create indexes for common query patterns
CREATE INDEX idx_dim_time_year_month ON DIM_TIME(year, month_number);
CREATE INDEX idx_dim_time_fiscal ON DIM_TIME(fiscal_year, fiscal_quarter);
CREATE INDEX idx_dim_time_quarter ON DIM_TIME(year, quarter_number);
CREATE BITMAP INDEX idx_dim_time_weekend ON DIM_TIME(is_weekend);
CREATE BITMAP INDEX idx_dim_time_holiday ON DIM_TIME(is_uk_holiday);

COMMENT ON TABLE DIM_TIME IS 'Date dimension with comprehensive calendar intelligence for UK fiscal year analysis';

-- =====================================================================
-- 2. DIM_PRODUCT - Publication/Edition Dimension
-- =====================================================================
CREATE TABLE DIM_PRODUCT (
    product_key            NUMBER(10) NOT NULL,           -- Surrogate Key
    edition_id             VARCHAR2(10) NOT NULL,         -- Natural key: E121, E122, etc.
    product_title          VARCHAR2(200) NOT NULL,        
    product_category       VARCHAR2(50) NOT NULL,         -- Science, Arts, etc.
    product_language       VARCHAR2(10) NOT NULL,         -- EN, FR, etc.
    author_key             NUMBER(10) NOT NULL,           -- FK to DIM_AUTHOR
    vendor_key             NUMBER(10) NOT NULL,           -- FK to DIM_VENDOR
    -- SCD Type 2 columns
    effective_date         DATE NOT NULL,
    expiry_date            DATE,
    is_current             CHAR(1) DEFAULT 'Y' NOT NULL,
    record_version         NUMBER(5) DEFAULT 1 NOT NULL,
    -- Audit columns
    created_date           DATE DEFAULT SYSDATE NOT NULL,
    created_by             VARCHAR2(50) DEFAULT USER NOT NULL,
    modified_date          DATE,
    modified_by            VARCHAR2(50),
    CONSTRAINT pk_dim_product PRIMARY KEY (product_key),
    CONSTRAINT ck_dim_product_current CHECK (is_current IN ('Y', 'N'))
) TABLESPACE USERS
  PCTFREE 10
  COMPRESS FOR OLTP;

CREATE UNIQUE INDEX uk_dim_product_scd ON DIM_PRODUCT(edition_id, effective_date);
CREATE INDEX idx_dim_product_current ON DIM_PRODUCT(edition_id, is_current);
CREATE INDEX idx_dim_product_category ON DIM_PRODUCT(product_category);
CREATE INDEX idx_dim_product_author ON DIM_PRODUCT(author_key);
CREATE INDEX idx_dim_product_vendor ON DIM_PRODUCT(vendor_key);

COMMENT ON TABLE DIM_PRODUCT IS 'Product/Publication dimension with SCD Type 2 for tracking changes over time';

-- =====================================================================
-- 3. DIM_AUTHOR - Author Dimension
-- =====================================================================
CREATE TABLE DIM_AUTHOR (
    author_key             NUMBER(10) NOT NULL,           -- Surrogate Key
    author_id              VARCHAR2(10) NOT NULL,         -- Natural key: A001, A002, etc.
    first_name             VARCHAR2(50) NOT NULL,
    last_name              VARCHAR2(50) NOT NULL,
    full_name              VARCHAR2(100) NOT NULL,        -- Computed: first + last
    country                VARCHAR2(50) NOT NULL,         -- UK, US, etc.
    primary_genre          VARCHAR2(50) NOT NULL,         -- Fiction, Non-Fiction, Poetry
    -- SCD Type 2 columns
    effective_date         DATE NOT NULL,
    expiry_date            DATE,
    is_current             CHAR(1) DEFAULT 'Y' NOT NULL,
    record_version         NUMBER(5) DEFAULT 1 NOT NULL,
    -- Audit columns
    created_date           DATE DEFAULT SYSDATE NOT NULL,
    created_by             VARCHAR2(50) DEFAULT USER NOT NULL,
    modified_date          DATE,
    modified_by            VARCHAR2(50),
    CONSTRAINT pk_dim_author PRIMARY KEY (author_key),
    CONSTRAINT ck_dim_author_current CHECK (is_current IN ('Y', 'N'))
) TABLESPACE USERS
  PCTFREE 10
  COMPRESS FOR OLTP;

CREATE UNIQUE INDEX uk_dim_author_scd ON DIM_AUTHOR(author_id, effective_date);
CREATE INDEX idx_dim_author_current ON DIM_AUTHOR(author_id, is_current);
CREATE INDEX idx_dim_author_country ON DIM_AUTHOR(country);
CREATE INDEX idx_dim_author_genre ON DIM_AUTHOR(primary_genre);
CREATE INDEX idx_dim_author_name ON DIM_AUTHOR(last_name, first_name);

COMMENT ON TABLE DIM_AUTHOR IS 'Author dimension with SCD Type 2 for tracking author profile changes';

-- =====================================================================
-- 4. DIM_DISTRIBUTION_CENTER - Distribution Center Dimension
-- =====================================================================
CREATE TABLE DIM_DISTRIBUTION_CENTER (
    dc_key                 NUMBER(10) NOT NULL,           -- Surrogate Key
    station_code           VARCHAR2(10) NOT NULL,         -- Natural key: DC001, DC002, etc.
    station_name           VARCHAR2(100) NOT NULL,
    station_region         VARCHAR2(50) NOT NULL,         -- Flintshire, Leicestershire, etc.
    station_country        VARCHAR2(50) DEFAULT 'UK' NOT NULL,
    manager_id             VARCHAR2(10) NOT NULL,         -- MGR001, MGR002, etc.
    station_address        VARCHAR2(200) NOT NULL,
    postcode               VARCHAR2(10),
    -- Geographic hierarchy
    region_code            VARCHAR2(10),                   -- Region grouping code
    region_name            VARCHAR2(50),                   -- Full region name
    -- SCD Type 2 columns
    effective_date         DATE NOT NULL,
    expiry_date            DATE,
    is_current             CHAR(1) DEFAULT 'Y' NOT NULL,
    record_version         NUMBER(5) DEFAULT 1 NOT NULL,
    -- Audit columns
    created_date           DATE DEFAULT SYSDATE NOT NULL,
    created_by             VARCHAR2(50) DEFAULT USER NOT NULL,
    modified_date          DATE,
    modified_by            VARCHAR2(50),
    CONSTRAINT pk_dim_dc PRIMARY KEY (dc_key),
    CONSTRAINT ck_dim_dc_current CHECK (is_current IN ('Y', 'N'))
) TABLESPACE USERS
  PCTFREE 10
  COMPRESS FOR OLTP;

CREATE UNIQUE INDEX uk_dim_dc_scd ON DIM_DISTRIBUTION_CENTER(station_code, effective_date);
CREATE INDEX idx_dim_dc_current ON DIM_DISTRIBUTION_CENTER(station_code, is_current);
CREATE INDEX idx_dim_dc_region ON DIM_DISTRIBUTION_CENTER(station_region);
CREATE INDEX idx_dim_dc_manager ON DIM_DISTRIBUTION_CENTER(manager_id);

COMMENT ON TABLE DIM_DISTRIBUTION_CENTER IS 'Distribution center dimension with geographic hierarchy and SCD Type 2';

-- =====================================================================
-- 5. DIM_VENDOR - Vendor Dimension
-- =====================================================================
CREATE TABLE DIM_VENDOR (
    vendor_key             NUMBER(10) NOT NULL,           -- Surrogate Key
    vendor_id              VARCHAR2(10) NOT NULL,         -- Natural key: V001, V002, etc.
    vendor_name            VARCHAR2(100) NOT NULL,
    vendor_score           NUMBER(3,1) NOT NULL,          -- 0.0 to 10.0
    service_type_1         VARCHAR2(50),                   -- Paper, Ink, etc.
    service_quality_1      VARCHAR2(10),                   -- A, B, C
    service_type_2         VARCHAR2(50),
    service_quality_2      VARCHAR2(10),
    -- Performance metrics
    avg_delivery_days      NUMBER(5,2),                    -- Average delivery time
    on_time_percentage     NUMBER(5,2),                    -- % of on-time deliveries
    cost_variance_pct      NUMBER(5,2),                    -- Cost variance from contract
    -- SCD Type 2 columns
    effective_date         DATE NOT NULL,
    expiry_date            DATE,
    is_current             CHAR(1) DEFAULT 'Y' NOT NULL,
    record_version         NUMBER(5) DEFAULT 1 NOT NULL,
    -- Audit columns
    created_date           DATE DEFAULT SYSDATE NOT NULL,
    created_by             VARCHAR2(50) DEFAULT USER NOT NULL,
    modified_date          DATE,
    modified_by            VARCHAR2(50),
    CONSTRAINT pk_dim_vendor PRIMARY KEY (vendor_key),
    CONSTRAINT ck_dim_vendor_current CHECK (is_current IN ('Y', 'N')),
    CONSTRAINT ck_dim_vendor_score CHECK (vendor_score BETWEEN 0.0 AND 10.0)
) TABLESPACE USERS
  PCTFREE 10
  COMPRESS FOR OLTP;

CREATE UNIQUE INDEX uk_dim_vendor_scd ON DIM_VENDOR(vendor_id, effective_date);
CREATE INDEX idx_dim_vendor_current ON DIM_VENDOR(vendor_id, is_current);
CREATE INDEX idx_dim_vendor_score ON DIM_VENDOR(vendor_score);
CREATE INDEX idx_dim_vendor_service ON DIM_VENDOR(service_type_1);

COMMENT ON TABLE DIM_VENDOR IS 'Vendor dimension with performance metrics and SCD Type 2';

-- =====================================================================
-- 6. DIM_CHANNEL - Sales Channel Dimension
-- =====================================================================
CREATE TABLE DIM_CHANNEL (
    channel_key            NUMBER(10) NOT NULL,           -- Surrogate Key
    channel_code           VARCHAR2(10) NOT NULL,         -- Natural key: CH001, CH002, CH003
    channel_name           VARCHAR2(50) NOT NULL,         -- Amazon, Barnes & Noble, In_Store
    channel_type           VARCHAR2(20) NOT NULL,         -- Online, Physical
    commission_rate        NUMBER(5,2),                    -- Commission percentage
    average_delivery_days  NUMBER(3),                      -- Avg days to customer
    -- Audit columns
    created_date           DATE DEFAULT SYSDATE NOT NULL,
    created_by             VARCHAR2(50) DEFAULT USER NOT NULL,
    modified_date          DATE,
    modified_by            VARCHAR2(50),
    CONSTRAINT pk_dim_channel PRIMARY KEY (channel_key),
    CONSTRAINT uk_dim_channel_code UNIQUE (channel_code)
) TABLESPACE USERS
  PCTFREE 5
  COMPRESS FOR OLTP;

CREATE INDEX idx_dim_channel_type ON DIM_CHANNEL(channel_type);
CREATE INDEX idx_dim_channel_name ON DIM_CHANNEL(channel_name);

COMMENT ON TABLE DIM_CHANNEL IS 'Sales channel dimension for Amazon, Barnes & Noble, and In-Store sales';

-- =====================================================================
-- 7. DIM_PRODUCT_TYPE - Product Type Dimension
-- =====================================================================
CREATE TABLE DIM_PRODUCT_TYPE (
    product_type_key       NUMBER(10) NOT NULL,           -- Surrogate Key
    product_type_code      VARCHAR2(20) NOT NULL,         -- Natural key: HRD, PBK, EBK
    product_type_name      VARCHAR2(50) NOT NULL,         -- Hardcover, Paperback, e-Book
    is_physical            CHAR(1) NOT NULL,              -- Y for physical, N for digital
    weight_kg              NUMBER(5,3),                    -- Typical weight for shipping
    typical_margin_pct     NUMBER(5,2),                    -- Typical profit margin %
    -- Audit columns
    created_date           DATE DEFAULT SYSDATE NOT NULL,
    created_by             VARCHAR2(50) DEFAULT USER NOT NULL,
    modified_date          DATE,
    modified_by            VARCHAR2(50),
    CONSTRAINT pk_dim_product_type PRIMARY KEY (product_type_key),
    CONSTRAINT uk_dim_product_type_code UNIQUE (product_type_code),
    CONSTRAINT ck_dim_product_type_physical CHECK (is_physical IN ('Y', 'N'))
) TABLESPACE USERS
  PCTFREE 5
  COMPRESS FOR OLTP;

CREATE BITMAP INDEX idx_dim_product_type_physical ON DIM_PRODUCT_TYPE(is_physical);

COMMENT ON TABLE DIM_PRODUCT_TYPE IS 'Product type dimension for Hardcover, Paperback, and e-Book classification';

-- =====================================================================
-- 8. DIM_CURRENCY - Currency Dimension
-- =====================================================================
CREATE TABLE DIM_CURRENCY (
    currency_key           NUMBER(10) NOT NULL,           -- Surrogate Key
    currency_code          VARCHAR2(3) NOT NULL,          -- Natural key: GBP, USD, EUR
    currency_name          VARCHAR2(50) NOT NULL,         -- British Pound, US Dollar, Euro
    currency_symbol        VARCHAR2(5) NOT NULL,          -- £, $, €
    is_base_currency       CHAR(1) DEFAULT 'N' NOT NULL,  -- Y for GBP (base)
    decimal_places         NUMBER(1) DEFAULT 2 NOT NULL,
    -- Audit columns
    created_date           DATE DEFAULT SYSDATE NOT NULL,
    created_by             VARCHAR2(50) DEFAULT USER NOT NULL,
    modified_date          DATE,
    modified_by            VARCHAR2(50),
    CONSTRAINT pk_dim_currency PRIMARY KEY (currency_key),
    CONSTRAINT uk_dim_currency_code UNIQUE (currency_code),
    CONSTRAINT ck_dim_currency_base CHECK (is_base_currency IN ('Y', 'N'))
) TABLESPACE USERS
  PCTFREE 5
  COMPRESS FOR OLTP;

COMMENT ON TABLE DIM_CURRENCY IS 'Currency dimension for multi-currency support with GBP as base currency';

-- =====================================================================
-- 9. FACT_EXCHANGE_RATE - Currency Exchange Rate Bridge Table
-- =====================================================================
CREATE TABLE FACT_EXCHANGE_RATE (
    exchange_rate_key      NUMBER(10) NOT NULL,           -- Surrogate Key
    from_currency_key      NUMBER(10) NOT NULL,           -- FK to DIM_CURRENCY
    to_currency_key        NUMBER(10) NOT NULL,           -- FK to DIM_CURRENCY (always GBP)
    time_key               NUMBER(8) NOT NULL,            -- FK to DIM_TIME
    exchange_rate          NUMBER(12,6) NOT NULL,         -- Rate: from_curr * rate = to_curr
    rate_date              DATE NOT NULL,
    rate_source            VARCHAR2(50),                   -- BOE, ECB, etc.
    -- Audit columns
    created_date           DATE DEFAULT SYSDATE NOT NULL,
    created_by             VARCHAR2(50) DEFAULT USER NOT NULL,
    CONSTRAINT pk_fact_exchange_rate PRIMARY KEY (exchange_rate_key),
    CONSTRAINT uk_fact_exch_rate UNIQUE (from_currency_key, to_currency_key, time_key),
    CONSTRAINT fk_exch_from_curr FOREIGN KEY (from_currency_key) REFERENCES DIM_CURRENCY(currency_key),
    CONSTRAINT fk_exch_to_curr FOREIGN KEY (to_currency_key) REFERENCES DIM_CURRENCY(currency_key),
    CONSTRAINT fk_exch_time FOREIGN KEY (time_key) REFERENCES DIM_TIME(time_key)
) TABLESPACE USERS
  PCTFREE 10
  COMPRESS FOR OLTP;

CREATE INDEX idx_fact_exch_from ON FACT_EXCHANGE_RATE(from_currency_key, time_key);
CREATE INDEX idx_fact_exch_to ON FACT_EXCHANGE_RATE(to_currency_key, time_key);

COMMENT ON TABLE FACT_EXCHANGE_RATE IS 'Historical currency exchange rates for multi-currency conversion to GBP';

-- =====================================================================
-- Add Foreign Keys to Product Dimension
-- =====================================================================
ALTER TABLE DIM_PRODUCT ADD CONSTRAINT fk_product_author 
    FOREIGN KEY (author_key) REFERENCES DIM_AUTHOR(author_key);

ALTER TABLE DIM_PRODUCT ADD CONSTRAINT fk_product_vendor 
    FOREIGN KEY (vendor_key) REFERENCES DIM_VENDOR(vendor_key);

-- =====================================================================
-- Create Sequences for Surrogate Keys
-- =====================================================================
CREATE SEQUENCE seq_product_key START WITH 1000 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE seq_author_key START WITH 1000 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE seq_dc_key START WITH 1000 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE seq_vendor_key START WITH 1000 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE seq_channel_key START WITH 1000 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE seq_product_type_key START WITH 1000 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE seq_currency_key START WITH 1000 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE seq_exchange_rate_key START WITH 1000 INCREMENT BY 1 NOCACHE;

-- =====================================================================
-- Create Statistics
-- =====================================================================
BEGIN
    DBMS_STATS.SET_TABLE_PREFS(NULL, 'DIM_TIME', 'INCREMENTAL', 'FALSE');
    DBMS_STATS.SET_TABLE_PREFS(NULL, 'DIM_PRODUCT', 'INCREMENTAL', 'FALSE');
    DBMS_STATS.SET_TABLE_PREFS(NULL, 'DIM_AUTHOR', 'INCREMENTAL', 'FALSE');
    DBMS_STATS.SET_TABLE_PREFS(NULL, 'DIM_DISTRIBUTION_CENTER', 'INCREMENTAL', 'FALSE');
    DBMS_STATS.SET_TABLE_PREFS(NULL, 'DIM_VENDOR', 'INCREMENTAL', 'FALSE');
    DBMS_STATS.SET_TABLE_PREFS(NULL, 'DIM_CHANNEL', 'INCREMENTAL', 'FALSE');
    DBMS_STATS.SET_TABLE_PREFS(NULL, 'DIM_PRODUCT_TYPE', 'INCREMENTAL', 'FALSE');
    DBMS_STATS.SET_TABLE_PREFS(NULL, 'DIM_CURRENCY', 'INCREMENTAL', 'FALSE');
END;
/

-- =====================================================================
-- Grant Permissions
-- =====================================================================
-- GRANT SELECT ON DIM_TIME TO role_bi_users;
-- GRANT SELECT ON DIM_PRODUCT TO role_bi_users;
-- GRANT SELECT ON DIM_AUTHOR TO role_bi_users;
-- GRANT SELECT ON DIM_DISTRIBUTION_CENTER TO role_bi_users;
-- GRANT SELECT ON DIM_VENDOR TO role_bi_users;
-- GRANT SELECT ON DIM_CHANNEL TO role_bi_users;
-- GRANT SELECT ON DIM_PRODUCT_TYPE TO role_bi_users;
-- GRANT SELECT ON DIM_CURRENCY TO role_bi_users;

COMMIT;

-- =====================================================================
-- End of Dimension Tables DDL
-- =====================================================================
