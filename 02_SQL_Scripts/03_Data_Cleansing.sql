-- =====================================================================
-- InkWave Publishing Data Warehouse - Fact Tables Cursor-Based Load
-- Oracle 19c+ Compatible
-- Author: Data Engineering Team
-- Date: 2025-12-07
-- Purpose: Load FACT_SALES and FACT_DAILY_OPERATIONS using cursors
-- =====================================================================

SET SERVEROUTPUT ON SIZE UNLIMITED;

-- =====================================================================
-- Part 1: Load FACT_SALES from STG_RAW_SALES
-- =====================================================================
DECLARE
    -- Cursor to fetch staging sales records
    CURSOR c_sales IS
        SELECT 
            s.stg_sales_id,
            s.sale_num,
            s.date_parsed,
            s.ed_id,
            s.chnl,
            s.tqty,
            s.uprice,
            s.curr,
            NVL(s.dscnt, s.discount_imputed) as discount_rate,
            s.pd,
            s.bc,
            s.vscr,
            s.typ
        FROM STG_RAW_SALES s
        WHERE s.validation_status = 'VALID'
          AND s.processed_flag = 'N'
        ORDER BY s.date_parsed, s.stg_sales_id;
    
    -- Variables for fact table columns
    v_sales_fact_key       NUMBER;
    v_time_key             NUMBER(8);
    v_product_key          NUMBER(10);
    v_author_key           NUMBER(10);
    v_dc_key               NUMBER(10);
    v_vendor_key           NUMBER(10);
    v_channel_key          NUMBER(10);
    v_product_type_key     NUMBER(10);
    v_currency_key         NUMBER(10);
    
    -- Calculated measures
    v_unit_price_gbp       NUMBER(12,2);
    v_exchange_rate        NUMBER(12,6);
    v_discount_amount_gbp  NUMBER(12,2);
    v_gross_amount_gbp     NUMBER(12,2);
    v_net_amount_gbp       NUMBER(12,2);
    v_unit_cost_gbp        NUMBER(12,2);
    v_total_cost_gbp       NUMBER(12,2);
    v_gross_profit_gbp     NUMBER(12,2);
    v_gross_margin_pct     NUMBER(5,2);
    
    -- Counters
    v_record_count         NUMBER := 0;
    v_error_count          NUMBER := 0;
    v_commit_threshold     NUMBER := 500;
    v_batch_id             NUMBER;
    v_start_time           TIMESTAMP := SYSTIMESTAMP;
    
BEGIN
    DBMS_OUTPUT.PUT_LINE('=============================================================');
    DBMS_OUTPUT.PUT_LINE('Starting FACT_SALES Load with Cursor');
    DBMS_OUTPUT.PUT_LINE('Start Time: ' || TO_CHAR(v_start_time, 'YYYY-MM-DD HH24:MI:SS'));
    DBMS_OUTPUT.PUT_LINE('=============================================================');
    
    -- Get next batch ID
    SELECT NVL(MAX(load_batch_id), 0) + 1 INTO v_batch_id FROM FACT_SALES;
    
    -- Open cursor and process each record
    FOR sales_rec IN c_sales LOOP
        BEGIN
            -- Lookup time_key
            BEGIN
                SELECT time_key INTO v_time_key
                FROM DIM_TIME
                WHERE full_date = sales_rec.date_parsed;
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    DBMS_OUTPUT.PUT_LINE('WARNING: No time_key for date ' || 
                        TO_CHAR(sales_rec.date_parsed, 'YYYY-MM-DD') || 
                        ' (Sale: ' || sales_rec.sale_num || ')');
                    v_error_count := v_error_count + 1;
                    CONTINUE;
            END;
            
            -- Lookup product_key and related keys (author, vendor)
            BEGIN
                SELECT 
                    p.product_key,
                    p.author_key,
                    p.vendor_key
                INTO 
                    v_product_key,
                    v_author_key,
                    v_vendor_key
                FROM DIM_PRODUCT p
                WHERE p.edition_id = sales_rec.ed_id
                  AND p.is_current = 'Y';
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    -- Use default/unknown product if not found
                    v_product_key := -1;
                    v_author_key := -1;
                    v_vendor_key := -1;
                    DBMS_OUTPUT.PUT_LINE('WARNING: No product for edition ' || 
                        sales_rec.ed_id || ' (Sale: ' || sales_rec.sale_num || ')');
            END;
            
            -- Lookup distribution center key (from product metadata or default)
            BEGIN
                SELECT dc_key INTO v_dc_key
                FROM DIM_DISTRIBUTION_CENTER
                WHERE is_current = 'Y'
                  AND ROWNUM = 1; -- Use first DC for sales (no direct mapping in source)
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    v_dc_key := -1;
            END;
            
            -- Lookup channel_key
            BEGIN
                SELECT channel_key INTO v_channel_key
                FROM DIM_CHANNEL
                WHERE channel_code = sales_rec.chnl;
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    v_channel_key := -1;
                    DBMS_OUTPUT.PUT_LINE('WARNING: No channel for code ' || 
                        sales_rec.chnl || ' (Sale: ' || sales_rec.sale_num || ')');
            END;
            
            -- Lookup product_type_key
            BEGIN
                SELECT product_type_key INTO v_product_type_key
                FROM DIM_PRODUCT_TYPE
                WHERE UPPER(product_type_name) = UPPER(REPLACE(sales_rec.typ, '‑', '-'));
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    v_product_type_key := -1;
                    DBMS_OUTPUT.PUT_LINE('WARNING: No product type for ' || 
                        sales_rec.typ || ' (Sale: ' || sales_rec.sale_num || ')');
            END;
            
            -- Lookup currency_key
            BEGIN
                SELECT currency_key INTO v_currency_key
                FROM DIM_CURRENCY
                WHERE currency_code = sales_rec.curr;
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    v_currency_key := -1;
            END;
            
            -- Get exchange rate for currency conversion
            BEGIN
                SELECT exchange_rate INTO v_exchange_rate
                FROM FACT_EXCHANGE_RATE
                WHERE from_currency_key = v_currency_key
                  AND time_key = v_time_key
                  AND ROWNUM = 1;
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    -- Use default rates if not found: USD=1.27, EUR=1.17, GBP=1.0
                    v_exchange_rate := CASE 
                        WHEN sales_rec.curr = 'USD' THEN 0.79  -- 1 USD = 0.79 GBP
                        WHEN sales_rec.curr = 'EUR' THEN 0.85  -- 1 EUR = 0.85 GBP
                        ELSE 1.0  -- GBP
                    END;
            END;
            
            -- Calculate measures
            v_unit_price_gbp := sales_rec.uprice * v_exchange_rate;
            v_gross_amount_gbp := v_unit_price_gbp * sales_rec.tqty;
            v_discount_amount_gbp := v_gross_amount_gbp * NVL(sales_rec.discount_rate, 0);
            v_net_amount_gbp := v_gross_amount_gbp - v_discount_amount_gbp;
            
            -- Calculate unit cost from binding cost and print run
            IF sales_rec.pd > 0 THEN
                v_unit_cost_gbp := sales_rec.bc / sales_rec.pd;
            ELSE
                v_unit_cost_gbp := 0;
            END IF;
            
            v_total_cost_gbp := v_unit_cost_gbp * sales_rec.tqty;
            v_gross_profit_gbp := v_net_amount_gbp - v_total_cost_gbp;
            
            IF v_net_amount_gbp > 0 THEN
                v_gross_margin_pct := (v_gross_profit_gbp / v_net_amount_gbp) * 100;
            ELSE
                v_gross_margin_pct := 0;
            END IF;
            
            -- Get next fact key
            SELECT seq_sales_fact_key.NEXTVAL INTO v_sales_fact_key FROM DUAL;
            
            -- Insert into FACT_SALES
            INSERT INTO FACT_SALES (
                sales_fact_key, time_key, product_key, author_key, dc_key, vendor_key,
                channel_key, product_type_key, currency_key,
                sale_number, quantity_sold, unit_price_original, unit_price_gbp,
                discount_rate, discount_amount_gbp, gross_amount_gbp, net_amount_gbp,
                print_run_qty, binding_cost_gbp, unit_cost_gbp, total_cost_gbp,
                gross_profit_gbp, gross_margin_pct, vendor_score,
                source_system, source_record_id, load_batch_id
            ) VALUES (
                v_sales_fact_key, v_time_key, v_product_key, v_author_key, v_dc_key, v_vendor_key,
                v_channel_key, v_product_type_key, v_currency_key,
                sales_rec.sale_num, sales_rec.tqty, sales_rec.uprice, v_unit_price_gbp,
                sales_rec.discount_rate, v_discount_amount_gbp, v_gross_amount_gbp, v_net_amount_gbp,
                sales_rec.pd, sales_rec.bc, v_unit_cost_gbp, v_total_cost_gbp,
                v_gross_profit_gbp, v_gross_margin_pct, sales_rec.vscr,
                'RAW_SALES', sales_rec.stg_sales_id, v_batch_id
            );
            
            v_record_count := v_record_count + 1;
            
            -- Commit periodically
            IF MOD(v_record_count, v_commit_threshold) = 0 THEN
                COMMIT;
                DBMS_OUTPUT.PUT_LINE('Processed ' || v_record_count || ' sales records...');
            END IF;
            
        EXCEPTION
            WHEN OTHERS THEN
                v_error_count := v_error_count + 1;
                DBMS_OUTPUT.PUT_LINE('ERROR processing sale ' || sales_rec.sale_num || ': ' || SQLERRM);
        END;
    END LOOP;
    
    -- Final commit
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE('=============================================================');
    DBMS_OUTPUT.PUT_LINE('FACT_SALES Load Complete');
    DBMS_OUTPUT.PUT_LINE('Records Loaded: ' || v_record_count);
    DBMS_OUTPUT.PUT_LINE('Errors: ' || v_error_count);
    DBMS_OUTPUT.PUT_LINE('Batch ID: ' || v_batch_id);
    DBMS_OUTPUT.PUT_LINE('Execution Time: ' || 
        TO_CHAR(EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)), '999.99') || ' seconds');
    DBMS_OUTPUT.PUT_LINE('=============================================================');
    
    -- Gather statistics
    DBMS_STATS.GATHER_TABLE_STATS(
        ownname => USER,
        tabname => 'FACT_SALES',
        estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE,
        method_opt => 'FOR ALL COLUMNS SIZE AUTO',
        cascade => TRUE
    );
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('FATAL ERROR: ' || SQLERRM);
        RAISE;
END;
/

-- =====================================================================
-- Part 2: Load FACT_DAILY_OPERATIONS from STG_RAW_DAILY
-- =====================================================================
DECLARE
    -- Cursor to fetch staging daily records
    CURSOR c_daily IS
        SELECT 
            d.stg_daily_id,
            d.date_parsed,
            d.stn_code,
            d.pd,
            d.bc,
            d.us,
            d.rv,
            d.rev,
            d.tmp,
            d.hmd,
            d.vscr,
            d.typ,
            d.notes
        FROM STG_RAW_DAILY d
        WHERE d.validation_status = 'VALID'
          AND d.processed_flag = 'N'
        ORDER BY d.date_parsed, d.stg_daily_id;
    
    -- Variables
    v_daily_ops_fact_key   NUMBER;
    v_time_key             NUMBER(8);
    v_product_key          NUMBER(10);
    v_dc_key               NUMBER(10);
    v_vendor_key           NUMBER(10);
    v_product_type_key     NUMBER(10);
    
    -- Calculated measures
    v_net_units            NUMBER(10);
    v_unit_binding_cost    NUMBER(12,4);
    v_cogs                 NUMBER(12,2);
    v_gross_profit         NUMBER(12,2);
    v_gross_margin_pct     NUMBER(5,2);
    v_utilization          NUMBER(5,2);
    v_return_rate          NUMBER(5,2);
    
    -- Counters
    v_record_count         NUMBER := 0;
    v_error_count          NUMBER := 0;
    v_batch_id             NUMBER;
    v_start_time           TIMESTAMP := SYSTIMESTAMP;
    
BEGIN
    DBMS_OUTPUT.PUT_LINE('=============================================================');
    DBMS_OUTPUT.PUT_LINE('Starting FACT_DAILY_OPERATIONS Load with Cursor');
    DBMS_OUTPUT.PUT_LINE('Start Time: ' || TO_CHAR(v_start_time, 'YYYY-MM-DD HH24:MI:SS'));
    DBMS_OUTPUT.PUT_LINE('=============================================================');
    
    -- Get next batch ID
    SELECT NVL(MAX(load_batch_id), 0) + 1 INTO v_batch_id FROM FACT_DAILY_OPERATIONS;
    
    FOR daily_rec IN c_daily LOOP
        BEGIN
            -- Lookup time_key
            SELECT time_key INTO v_time_key
            FROM DIM_TIME
            WHERE full_date = daily_rec.date_parsed;
            
            -- Lookup distribution center
            SELECT dc_key, vendor_key INTO v_dc_key, v_vendor_key
            FROM DIM_DISTRIBUTION_CENTER
            WHERE station_code = daily_rec.stn_code
              AND is_current = 'Y';
            
            -- Lookup product_type_key
            SELECT product_type_key INTO v_product_type_key
            FROM DIM_PRODUCT_TYPE
            WHERE UPPER(product_type_name) = UPPER(REPLACE(daily_rec.typ, '‑', '-'));
            
            -- Set default product (daily data doesn't have edition ID)
            SELECT product_key INTO v_product_key
            FROM DIM_PRODUCT
            WHERE is_current = 'Y'
              AND ROWNUM = 1;
            
            -- Calculate measures
            v_net_units := daily_rec.us - NVL(daily_rec.rv, 0);
            
            IF daily_rec.pd > 0 THEN
                v_unit_binding_cost := daily_rec.bc / daily_rec.pd;
                v_utilization := (daily_rec.us / daily_rec.pd) * 100;
            ELSE
                v_unit_binding_cost := 0;
                v_utilization := 0;
            END IF;
            
            v_cogs := v_unit_binding_cost * daily_rec.us;
            v_gross_profit := daily_rec.rev - v_cogs;
            
            IF daily_rec.rev > 0 THEN
                v_gross_margin_pct := (v_gross_profit / daily_rec.rev) * 100;
            ELSE
                v_gross_margin_pct := 0;
            END IF;
            
            IF daily_rec.us > 0 THEN
                v_return_rate := (NVL(daily_rec.rv, 0) / daily_rec.us) * 100;
            ELSE
                v_return_rate := 0;
            END IF;
            
            -- Get next fact key
            SELECT seq_daily_ops_fact_key.NEXTVAL INTO v_daily_ops_fact_key FROM DUAL;
            
            -- Insert into FACT_DAILY_OPERATIONS
            INSERT INTO FACT_DAILY_OPERATIONS (
                daily_ops_fact_key, time_key, product_key, dc_key, vendor_key, product_type_key,
                print_run_qty, binding_cost_gbp, units_sold, returns_qty, net_units, revenue_gbp,
                unit_binding_cost, cost_of_goods_sold, gross_profit_gbp, gross_margin_pct,
                print_run_utilization, return_rate_pct,
                temperature_celsius, humidity_pct, vendor_score, notes,
                source_system, source_record_id, load_batch_id
            ) VALUES (
                v_daily_ops_fact_key, v_time_key, v_product_key, v_dc_key, v_vendor_key, v_product_type_key,
                daily_rec.pd, daily_rec.bc, daily_rec.us, NVL(daily_rec.rv, 0), v_net_units, daily_rec.rev,
                v_unit_binding_cost, v_cogs, v_gross_profit, v_gross_margin_pct,
                v_utilization, v_return_rate,
                daily_rec.tmp, daily_rec.hmd, daily_rec.vscr, daily_rec.notes,
                'RAW_DAILY', daily_rec.stg_daily_id, v_batch_id
            );
            
            v_record_count := v_record_count + 1;
            
            IF MOD(v_record_count, 100) = 0 THEN
                COMMIT;
                DBMS_OUTPUT.PUT_LINE('Processed ' || v_record_count || ' daily records...');
            END IF;
            
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                v_error_count := v_error_count + 1;
                DBMS_OUTPUT.PUT_LINE('ERROR: Missing dimension data for daily record ' || daily_rec.stg_daily_id);
            WHEN OTHERS THEN
                v_error_count := v_error_count + 1;
                DBMS_OUTPUT.PUT_LINE('ERROR processing daily record ' || daily_rec.stg_daily_id || ': ' || SQLERRM);
        END;
    END LOOP;
    
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE('=============================================================');
    DBMS_OUTPUT.PUT_LINE('FACT_DAILY_OPERATIONS Load Complete');
    DBMS_OUTPUT.PUT_LINE('Records Loaded: ' || v_record_count);
    DBMS_OUTPUT.PUT_LINE('Errors: ' || v_error_count);
    DBMS_OUTPUT.PUT_LINE('Batch ID: ' || v_batch_id);
    DBMS_OUTPUT.PUT_LINE('Execution Time: ' || 
        TO_CHAR(EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_time)), '999.99') || ' seconds');
    DBMS_OUTPUT.PUT_LINE('=============================================================');
    
    DBMS_STATS.GATHER_TABLE_STATS(USER, 'FACT_DAILY_OPERATIONS', 
        estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE, cascade => TRUE);
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('FATAL ERROR: ' || SQLERRM);
        RAISE;
END;
/

-- =====================================================================
-- End of Fact Tables Cursor-Based Load
-- =====================================================================
