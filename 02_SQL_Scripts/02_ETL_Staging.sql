-- =====================================================================
-- InkWave Publishing Data Warehouse - Staging Tables DDL
-- Oracle 19c+ Compatible
-- Author: Data Engineering Team
-- Date: 2025-12-07
-- Purpose: Staging tables for raw data validation and ETL tracking
-- =====================================================================

-- =====================================================================
-- 1. STG_RAW_DAILY - Staging table for daily operations data
-- =====================================================================
CREATE TABLE STG_RAW_DAILY (
    stg_daily_id           NUMBER GENERATED ALWAYS AS IDENTITY,
    date_stamp             VARCHAR2(20),
    date_parsed            DATE,
    stn_code               VARCHAR2(10),
    pd                     NUMBER,
    bc                     NUMBER,
    us                     NUMBER,
    rv                     NUMBER,
    rev                    NUMBER,
    tmp                    NUMBER,
    hmd                    NUMBER,
    vscr                   NUMBER,
    typ                    VARCHAR2(20),
    notes                  VARCHAR2(500),
    -- ETL Tracking Columns
    source_filename        VARCHAR2(255),
    load_date              DATE DEFAULT SYSDATE,
    validation_status      VARCHAR2(20) DEFAULT 'PENDING',
    validation_errors      VARCHAR2(4000),
    processed_flag         CHAR(1) DEFAULT 'N',
    processed_date         DATE,
    CONSTRAINT pk_stg_daily PRIMARY KEY (stg_daily_id),
    CONSTRAINT ck_stg_daily_processed CHECK (processed_flag IN ('Y', 'N')),
    CONSTRAINT ck_stg_daily_validation CHECK (validation_status IN ('PENDING', 'VALID', 'INVALID'))
) TABLESPACE USERS
  PCTFREE 10;

CREATE INDEX idx_stg_daily_date ON STG_RAW_DAILY(date_parsed);
CREATE INDEX idx_stg_daily_station ON STG_RAW_DAILY(stn_code);
CREATE INDEX idx_stg_daily_validation ON STG_RAW_DAILY(validation_status);
CREATE INDEX idx_stg_daily_processed ON STG_RAW_DAILY(processed_flag);

COMMENT ON TABLE STG_RAW_DAILY IS 'Staging table for raw daily operations data with validation and tracking';

-- =====================================================================
-- 2. STG_RAW_SALES - Staging table for sales transaction data
-- =====================================================================
CREATE TABLE STG_RAW_SALES (
    stg_sales_id           NUMBER GENERATED ALWAYS AS IDENTITY,
    sale_num               VARCHAR2(20),
    date_stamp             VARCHAR2(20),
    date_parsed            DATE,
    ed_id                  VARCHAR2(10),
    chnl                   VARCHAR2(10),
    tqty                   NUMBER,
    uprice                 NUMBER,
    curr                   VARCHAR2(3),
    dscnt                  NUMBER,
    discount_imputed       NUMBER,  -- Imputed discount when missing
    pd                     NUMBER,
    bc                     NUMBER,
    vscr                   NUMBER,
    typ                    VARCHAR2(20),
    -- ETL Tracking Columns
    source_filename        VARCHAR2(255),
    load_date              DATE DEFAULT SYSDATE,
    validation_status      VARCHAR2(20) DEFAULT 'PENDING',
    validation_errors      VARCHAR2(4000),
    processed_flag         CHAR(1) DEFAULT 'N',
    processed_date         DATE,
    CONSTRAINT pk_stg_sales PRIMARY KEY (stg_sales_id),
    CONSTRAINT ck_stg_sales_processed CHECK (processed_flag IN ('Y', 'N')),
    CONSTRAINT ck_stg_sales_validation CHECK (validation_status IN ('PENDING', 'VALID', 'INVALID'))
) TABLESPACE USERS
  PCTFREE 10;

CREATE INDEX idx_stg_sales_date ON STG_RAW_SALES(date_parsed);
CREATE INDEX idx_stg_sales_edition ON STG_RAW_SALES(ed_id);
CREATE INDEX idx_stg_sales_channel ON STG_RAW_SALES(chnl);
CREATE INDEX idx_stg_sales_validation ON STG_RAW_SALES(validation_status);
CREATE INDEX idx_stg_sales_processed ON STG_RAW_SALES(processed_flag);

COMMENT ON TABLE STG_RAW_SALES IS 'Staging table for raw sales transaction data with validation and tracking';

-- =====================================================================
-- 3. STG_RAW_META - Staging table for metadata/reference data
-- =====================================================================
CREATE TABLE STG_RAW_META (
    stg_meta_id            NUMBER GENERATED ALWAYS AS IDENTITY,
    station_id             VARCHAR2(10),
    station_name           VARCHAR2(100),
    station_region         VARCHAR2(50),
    station_mgr            VARCHAR2(10),
    station_address        VARCHAR2(200),
    vendor_id              VARCHAR2(10),
    vendor_name            VARCHAR2(100),
    vendor_score           NUMBER,
    vendor_svc_type_1      VARCHAR2(50),
    vendor_svc_qual_1      VARCHAR2(10),
    vendor_svc_type_2      VARCHAR2(50),
    vendor_svc_qual_2      VARCHAR2(10),
    publication_ed         VARCHAR2(10),
    publication_title      VARCHAR2(200),
    publication_cat        VARCHAR2(50),
    publication_lang       VARCHAR2(10),
    publication_author     VARCHAR2(100),
    author_id              VARCHAR2(10),
    author_first_name      VARCHAR2(50),
    author_last_name       VARCHAR2(50),
    author_country         VARCHAR2(50),
    author_primary_genre   VARCHAR2(50),
    -- ETL Tracking Columns
    source_filename        VARCHAR2(255),
    load_date              DATE DEFAULT SYSDATE,
    validation_status      VARCHAR2(20) DEFAULT 'PENDING',
    validation_errors      VARCHAR2(4000),
    processed_flag         CHAR(1) DEFAULT 'N',
    processed_date         DATE,
    CONSTRAINT pk_stg_meta PRIMARY KEY (stg_meta_id),
    CONSTRAINT ck_stg_meta_processed CHECK (processed_flag IN ('Y', 'N')),
    CONSTRAINT ck_stg_meta_validation CHECK (validation_status IN ('PENDING', 'VALID', 'INVALID'))
) TABLESPACE USERS
  PCTFREE 10;

CREATE INDEX idx_stg_meta_station ON STG_RAW_META(station_id);
CREATE INDEX idx_stg_meta_vendor ON STG_RAW_META(vendor_id);
CREATE INDEX idx_stg_meta_publication ON STG_RAW_META(publication_ed);
CREATE INDEX idx_stg_meta_author ON STG_RAW_META(author_id);
CREATE INDEX idx_stg_meta_validation ON STG_RAW_META(validation_status);
CREATE INDEX idx_stg_meta_processed ON STG_RAW_META(processed_flag);

COMMENT ON TABLE STG_RAW_META IS 'Staging table for raw metadata/reference data with validation and tracking';

-- =====================================================================
-- 4. ETL_BATCH_LOG - Tracking ETL batch executions
-- =====================================================================
CREATE TABLE ETL_BATCH_LOG (
    batch_id               NUMBER GENERATED ALWAYS AS IDENTITY,
    batch_name             VARCHAR2(100),
    start_time             TIMESTAMP,
    end_time               TIMESTAMP,
    status                 VARCHAR2(20),
    records_processed      NUMBER,
    records_failed         NUMBER,
    error_message          VARCHAR2(4000),
    created_by             VARCHAR2(50) DEFAULT USER,
    created_date           DATE DEFAULT SYSDATE,
    CONSTRAINT pk_etl_batch PRIMARY KEY (batch_id),
    CONSTRAINT ck_etl_batch_status CHECK (status IN ('STARTED', 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED'))
) TABLESPACE USERS
  PCTFREE 10;

CREATE INDEX idx_etl_batch_name ON ETL_BATCH_LOG(batch_name);
CREATE INDEX idx_etl_batch_status ON ETL_BATCH_LOG(status);
CREATE INDEX idx_etl_batch_time ON ETL_BATCH_LOG(start_time, end_time);

COMMENT ON TABLE ETL_BATCH_LOG IS 'Tracking table for ETL batch executions';

-- =====================================================================
-- 5. ETL_STEP_LOG - Tracking individual ETL steps within batches
-- =====================================================================
CREATE TABLE ETL_STEP_LOG (
    step_id                NUMBER GENERATED ALWAYS AS IDENTITY,
    batch_id               NUMBER,
    step_name              VARCHAR2(100),
    start_time             TIMESTAMP,
    end_time               TIMESTAMP,
    status                 VARCHAR2(20),
    records_processed      NUMBER,
    records_failed         NUMBER,
    error_message          VARCHAR2(4000),
    CONSTRAINT pk_etl_step PRIMARY KEY (step_id),
    CONSTRAINT fk_etl_step_batch FOREIGN KEY (batch_id) REFERENCES ETL_BATCH_LOG(batch_id),
    CONSTRAINT ck_etl_step_status CHECK (status IN ('STARTED', 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED'))
) TABLESPACE USERS
  PCTFREE 10;

CREATE INDEX idx_etl_step_batch ON ETL_STEP_LOG(batch_id);
CREATE INDEX idx_etl_step_name ON ETL_STEP_LOG(step_name);
CREATE INDEX idx_etl_step_status ON ETL_STEP_LOG(status);

COMMENT ON TABLE ETL_STEP_LOG IS 'Tracking table for individual ETL steps within batches';

-- =====================================================================
-- 6. STG_DATA_QUALITY_LOG - Data quality validation results
-- =====================================================================
CREATE TABLE STG_DATA_QUALITY_LOG (
    dq_log_id              NUMBER GENERATED ALWAYS AS IDENTITY,
    source_table           VARCHAR2(50),
    validation_rule        VARCHAR2(100),
    validation_date        DATE DEFAULT SYSDATE,
    total_records          NUMBER,
    valid_records          NUMBER,
    invalid_records        NUMBER,
    error_details          CLOB,
    batch_id               NUMBER,
    CONSTRAINT pk_dq_log PRIMARY KEY (dq_log_id),
    CONSTRAINT fk_dq_log_batch FOREIGN KEY (batch_id) REFERENCES ETL_BATCH_LOG(batch_id)
) TABLESPACE USERS
  PCTFREE 10;

CREATE INDEX idx_dq_log_table ON STG_DATA_QUALITY_LOG(source_table);
CREATE INDEX idx_dq_log_date ON STG_DATA_QUALITY_LOG(validation_date);
CREATE INDEX idx_dq_log_batch ON STG_DATA_QUALITY_LOG(batch_id);

COMMENT ON TABLE STG_DATA_QUALITY_LOG IS 'Data quality validation results log';

-- =====================================================================
-- 7. Stored Procedures for ETL Operations
-- =====================================================================

-- Procedure to truncate staging tables
CREATE OR REPLACE PROCEDURE truncate_staging_tables AS
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE STG_RAW_DAILY';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE STG_RAW_SALES';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE STG_RAW_META';
    
    INSERT INTO ETL_BATCH_LOG(batch_name, start_time, status)
    VALUES ('TRUNCATE_STAGING', SYSTIMESTAMP, 'COMPLETED');
    
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        INSERT INTO ETL_BATCH_LOG(batch_name, start_time, status, error_message)
        VALUES ('TRUNCATE_STAGING', SYSTIMESTAMP, 'FAILED', SQLERRM);
        COMMIT;
        RAISE;
END;
/

-- Procedure to archive processed staging records
CREATE OR REPLACE PROCEDURE archive_processed_staging AS
    v_batch_id NUMBER;
BEGIN
    -- Create batch log entry
    INSERT INTO ETL_BATCH_LOG(batch_name, start_time, status)
    VALUES ('ARCHIVE_STAGING', SYSTIMESTAMP, 'STARTED')
    RETURNING batch_id INTO v_batch_id;
    
    -- Archive processed daily records
    INSERT INTO STG_RAW_DAILY_ARCHIVE
    SELECT * FROM STG_RAW_DAILY WHERE processed_flag = 'Y';
    
    -- Archive processed sales records
    INSERT INTO STG_RAW_SALES_ARCHIVE
    SELECT * FROM STG_RAW_SALES WHERE processed_flag = 'Y';
    
    -- Archive processed meta records
    INSERT INTO STG_RAW_META_ARCHIVE
    SELECT * FROM STG_RAW_META WHERE processed_flag = 'Y';
    
    -- Update batch log
    UPDATE ETL_BATCH_LOG 
    SET end_time = SYSTIMESTAMP, 
        status = 'COMPLETED',
        records_processed = (SELECT COUNT(*) FROM STG_RAW_DAILY WHERE processed_flag = 'Y') +
                           (SELECT COUNT(*) FROM STG_RAW_SALES WHERE processed_flag = 'Y') +
                           (SELECT COUNT(*) FROM STG_RAW_META WHERE processed_flag = 'Y')
    WHERE batch_id = v_batch_id;
    
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        UPDATE ETL_BATCH_LOG 
        SET end_time = SYSTIMESTAMP, 
            status = 'FAILED',
            error_message = SQLERRM
        WHERE batch_id = v_batch_id;
        COMMIT;
        RAISE;
END;
/

-- =====================================================================
-- 8. Archive Tables (Structure matches staging tables)
-- =====================================================================

-- Archive table for daily data
CREATE TABLE STG_RAW_DAILY_ARCHIVE AS SELECT * FROM STG_RAW_DAILY WHERE 1=0;
ALTER TABLE STG_RAW_DAILY_ARCHIVE ADD CONSTRAINT pk_stg_daily_arch PRIMARY KEY (stg_daily_id);

-- Archive table for sales data
CREATE TABLE STG_RAW_SALES_ARCHIVE AS SELECT * FROM STG_RAW_SALES WHERE 1=0;
ALTER TABLE STG_RAW_SALES_ARCHIVE ADD CONSTRAINT pk_stg_sales_arch PRIMARY KEY (stg_sales_id);

-- Archive table for meta data
CREATE TABLE STG_RAW_META_ARCHIVE AS SELECT * FROM STG_RAW_META WHERE 1=0;
ALTER TABLE STG_RAW_META_ARCHIVE ADD CONSTRAINT pk_stg_meta_arch PRIMARY KEY (stg_meta_id);

COMMENT ON TABLE STG_RAW_DAILY_ARCHIVE IS 'Archive table for processed daily operations data';
COMMENT ON TABLE STG_RAW_SALES_ARCHIVE IS 'Archive table for processed sales transaction data';
COMMENT ON TABLE STG_RAW_META_ARCHIVE IS 'Archive table for processed metadata/reference data';

-- =====================================================================
-- Grant Permissions
-- =====================================================================
-- GRANT SELECT, INSERT, UPDATE, DELETE ON STG_RAW_DAILY TO role_etl_user;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON STG_RAW_SALES TO role_etl_user;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON STG_RAW_META TO role_etl_user;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ETL_BATCH_LOG TO role_etl_user;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ETL_STEP_LOG TO role_etl_user;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON STG_DATA_QUALITY_LOG TO role_etl_user;

COMMIT;

-- =====================================================================
-- End of Staging Tables DDL
-- =====================================================================