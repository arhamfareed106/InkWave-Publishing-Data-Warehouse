-- =====================================================================
-- InkWave Publishing Data Warehouse - DIM_TIME Cursor-Based Load
-- Oracle 19c+ Compatible
-- Author: Data Engineering Team
-- Date: 2025-12-07
-- Purpose: Populate DIM_TIME with complete calendar intelligence using cursor
-- =====================================================================

SET SERVEROUTPUT ON SIZE UNLIMITED;

DECLARE
    -- Cursor to generate date range (2018-01-01 to 2026-12-31)
    CURSOR c_date_range IS
        SELECT 
            TO_DATE('2018-01-01', 'YYYY-MM-DD') + (LEVEL - 1) AS full_date
        FROM DUAL
        CONNECT BY LEVEL <= (TO_DATE('2026-12-31', 'YYYY-MM-DD') - TO_DATE('2018-01-01', 'YYYY-MM-DD') + 1);
    
    -- Variables for time dimension attributes
    v_time_key             NUMBER(8);
    v_full_date            DATE;
    v_day_of_week          NUMBER(1);
    v_day_of_week_name     VARCHAR2(10);
    v_day_of_month         NUMBER(2);
    v_day_of_year          NUMBER(3);
    v_week_of_year         NUMBER(2);
    v_month_number         NUMBER(2);
    v_month_name           VARCHAR2(10);
    v_month_abbr           VARCHAR2(3);
    v_quarter_number       NUMBER(1);
    v_quarter_name         VARCHAR2(2);
    v_year                 NUMBER(4);
    v_fiscal_year          NUMBER(4);
    v_fiscal_quarter       NUMBER(1);
    v_fiscal_month         NUMBER(2);
    v_is_weekend           CHAR(1);
    v_is_uk_holiday        CHAR(1);
    v_uk_holiday_name      VARCHAR2(50);
    v_is_month_start       CHAR(1);
    v_is_month_end         CHAR(1);
    v_is_quarter_start     CHAR(1);
    v_is_quarter_end       CHAR(1);
    v_is_year_start        CHAR(1);
    v_is_year_end          CHAR(1);
    v_week_start_date      DATE;
    v_week_end_date        DATE;
    v_month_year_name      VARCHAR2(20);
    v_quarter_year_name    VARCHAR2(10);
    v_days_in_month        NUMBER(2);
    v_season               VARCHAR2(10);
    
    -- Counter variables
    v_record_count         NUMBER := 0;
    v_commit_threshold     NUMBER := 1000;
    
    -- Start time
    v_start_time           TIMESTAMP := SYSTIMESTAMP;
    
BEGIN
    DBMS_OUTPUT.PUT_LINE('=============================================================');
    DBMS_OUTPUT.PUT_LINE('Starting DIM_TIME Population with Cursor');
    DBMS_OUTPUT.PUT_LINE('Start Time: ' || TO_CHAR(v_start_time, 'YYYY-MM-DD HH24:MI:SS'));
    DBMS_OUTPUT.PUT_LINE('=============================================================');
    
    -- Open cursor and iterate through each date
    FOR date_rec IN c_date_range LOOP
        v_full_date := date_rec.full_date;
        
        -- Calculate time_key (YYYYMMDD format)
        v_time_key := TO_NUMBER(TO_CHAR(v_full_date, 'YYYYMMDD'));
        
        -- Basic date attributes
        v_day_of_week := TO_NUMBER(TO_CHAR(v_full_date, 'D')); -- 1=Sunday in Oracle, adjust to Monday=1
        v_day_of_week := CASE WHEN v_day_of_week = 1 THEN 7 ELSE v_day_of_week - 1 END;
        v_day_of_week_name := TO_CHAR(v_full_date, 'Day');
        v_day_of_week_name := RTRIM(v_day_of_week_name); -- Remove trailing spaces
        v_day_of_month := TO_NUMBER(TO_CHAR(v_full_date, 'DD'));
        v_day_of_year := TO_NUMBER(TO_CHAR(v_full_date, 'DDD'));
        v_week_of_year := TO_NUMBER(TO_CHAR(v_full_date, 'IW')); -- ISO week
        
        -- Month attributes
        v_month_number := TO_NUMBER(TO_CHAR(v_full_date, 'MM'));
        v_month_name := TO_CHAR(v_full_date, 'Month');
        v_month_name := RTRIM(v_month_name);
        v_month_abbr := TO_CHAR(v_full_date, 'Mon');
        v_days_in_month := TO_NUMBER(TO_CHAR(LAST_DAY(v_full_date), 'DD'));
        
        -- Quarter attributes
        v_quarter_number := TO_NUMBER(TO_CHAR(v_full_date, 'Q'));
        v_quarter_name := 'Q' || v_quarter_number;
        
        -- Year attributes
        v_year := TO_NUMBER(TO_CHAR(v_full_date, 'YYYY'));
        
        -- UK Fiscal Year (April to March)
        IF v_month_number >= 4 THEN
            v_fiscal_year := v_year + 1;
            v_fiscal_month := v_month_number - 3;
        ELSE
            v_fiscal_year := v_year;
            v_fiscal_month := v_month_number + 9;
        END IF;
        
        -- Fiscal Quarter (based on fiscal month)
        v_fiscal_quarter := CEIL(v_fiscal_month / 3);
        
        -- Weekend flag
        v_is_weekend := CASE WHEN v_day_of_week IN (6, 7) THEN 'Y' ELSE 'N' END;
        
        -- UK Holiday determination (simplified - major holidays only)
        v_is_uk_holiday := 'N';
        v_uk_holiday_name := NULL;
        
        -- New Year's Day
        IF v_month_number = 1 AND v_day_of_month = 1 THEN
            v_is_uk_holiday := 'Y';
            v_uk_holiday_name := 'New Year''s Day';
        END IF;
        
        -- Christmas Day
        IF v_month_number = 12 AND v_day_of_month = 25 THEN
            v_is_uk_holiday := 'Y';
            v_uk_holiday_name := 'Christmas Day';
        END IF;
        
        -- Boxing Day
        IF v_month_number = 12 AND v_day_of_month = 26 THEN
            v_is_uk_holiday := 'Y';
            v_uk_holiday_name := 'Boxing Day';
        END IF;
        
        -- Good Friday (approximation - 2 days before Easter Sunday)
        -- Easter calculation is complex, simplified here
        
        -- Bank Holidays (first and last Monday of May, last Monday of August)
        IF v_month_number = 5 AND v_day_of_week = 1 AND v_day_of_month <= 7 THEN
            v_is_uk_holiday := 'Y';
            v_uk_holiday_name := 'Early May Bank Holiday';
        END IF;
        
        IF v_month_number = 5 AND v_day_of_week = 1 AND v_day_of_month >= 25 THEN
            v_is_uk_holiday := 'Y';
            v_uk_holiday_name := 'Spring Bank Holiday';
        END IF;
        
        IF v_month_number = 8 AND v_day_of_week = 1 AND v_day_of_month >= 25 THEN
            v_is_uk_holiday := 'Y';
            v_uk_holiday_name := 'Summer Bank Holiday';
        END IF;
        
        -- Month/Quarter/Year boundary flags
        v_is_month_start := CASE WHEN v_day_of_month = 1 THEN 'Y' ELSE 'N' END;
        v_is_month_end := CASE WHEN v_full_date = LAST_DAY(v_full_date) THEN 'Y' ELSE 'N' END;
        v_is_quarter_start := CASE WHEN v_day_of_month = 1 AND v_month_number IN (1, 4, 7, 10) THEN 'Y' ELSE 'N' END;
        v_is_quarter_end := CASE WHEN v_is_month_end = 'Y' AND v_month_number IN (3, 6, 9, 12) THEN 'Y' ELSE 'N' END;
        v_is_year_start := CASE WHEN v_month_number = 1 AND v_day_of_month = 1 THEN 'Y' ELSE 'N' END;
        v_is_year_end := CASE WHEN v_month_number = 12 AND v_day_of_month = 31 THEN 'Y' ELSE 'N' END;
        
        -- Week start and end dates (Monday to Sunday)
        v_week_start_date := TRUNC(v_full_date, 'IW'); -- ISO week Monday
        v_week_end_date := v_week_start_date + 6;      -- Sunday
        
        -- Formatted names
        v_month_year_name := v_month_abbr || ' ' || v_year;
        v_quarter_year_name := v_quarter_name || ' ' || v_year;
        
        -- Season (Northern Hemisphere)
        v_season := CASE
            WHEN v_month_number IN (3, 4, 5) THEN 'Spring'
            WHEN v_month_number IN (6, 7, 8) THEN 'Summer'
            WHEN v_month_number IN (9, 10, 11) THEN 'Autumn'
            ELSE 'Winter'
        END;
        
        -- Insert record
        INSERT INTO DIM_TIME (
            time_key, full_date, day_of_week, day_of_week_name, 
            day_of_month, day_of_year, week_of_year,
            month_number, month_name, month_abbr,
            quarter_number, quarter_name,
            year, fiscal_year, fiscal_quarter, fiscal_month,
            is_weekend, is_uk_holiday, uk_holiday_name,
            is_month_start, is_month_end,
            is_quarter_start, is_quarter_end,
            is_year_start, is_year_end,
            week_start_date, week_end_date,
            month_year_name, quarter_year_name,
            days_in_month, season
        ) VALUES (
            v_time_key, v_full_date, v_day_of_week, v_day_of_week_name,
            v_day_of_month, v_day_of_year, v_week_of_year,
            v_month_number, v_month_name, v_month_abbr,
            v_quarter_number, v_quarter_name,
            v_year, v_fiscal_year, v_fiscal_quarter, v_fiscal_month,
            v_is_weekend, v_is_uk_holiday, v_uk_holiday_name,
            v_is_month_start, v_is_month_end,
            v_is_quarter_start, v_is_quarter_end,
            v_is_year_start, v_is_year_end,
            v_week_start_date, v_week_end_date,
            v_month_year_name, v_quarter_year_name,
            v_days_in_month, v_season
        );
        
        v_record_count := v_record_count + 1;
        
        -- Commit every threshold records
        IF MOD(v_record_count, v_commit_threshold) = 0 THEN
            COMMIT;
            DBMS_OUTPUT.PUT_LINE('Processed ' || v_record_count || ' records...');
        END IF;
        
    END LOOP;
    
    -- Final commit
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE('=============================================================');
    DBMS_OUTPUT.PUT_LINE('DIM_TIME Population Complete');
    DBMS_OUTPUT.PUT_LINE('Total Records Inserted: ' || v_record_count);
    DBMS_OUTPUT.PUT_LINE('Execution Time: ' || 
        TO_CHAR(EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)), '999.99') || ' seconds');
    DBMS_OUTPUT.PUT_LINE('=============================================================');
    
    -- Gather statistics
    DBMS_STATS.GATHER_TABLE_STATS(
        ownname => USER,
        tabname => 'DIM_TIME',
        estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE,
        method_opt => 'FOR ALL COLUMNS SIZE AUTO',
        cascade => TRUE
    );
    
    DBMS_OUTPUT.PUT_LINE('Statistics gathered successfully');
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('ERROR: ' || SQLERRM);
        DBMS_OUTPUT.PUT_LINE('Error at record count: ' || v_record_count);
        RAISE;
END;
/

-- Verify the load
SELECT 
    'Total Records' as metric, COUNT(*) as value FROM DIM_TIME
UNION ALL
SELECT 
    'Date Range' as metric, 
    TO_CHAR(MIN(full_date), 'YYYY-MM-DD') || ' to ' || TO_CHAR(MAX(full_date), 'YYYY-MM-DD') as value 
FROM DIM_TIME
UNION ALL
SELECT 
    'Weekends' as metric, COUNT(*) as value FROM DIM_TIME WHERE is_weekend = 'Y'
UNION ALL
SELECT 
    'UK Holidays' as metric, COUNT(*) as value FROM DIM_TIME WHERE is_uk_holiday = 'Y'
UNION ALL
SELECT 
    'Fiscal Years' as metric, COUNT(DISTINCT fiscal_year) as value FROM DIM_TIME;

-- Sample records
SELECT * FROM DIM_TIME WHERE ROWNUM <= 5 ORDER BY time_key;

-- =====================================================================
-- End of DIM_TIME Cursor-Based Load
-- =====================================================================
