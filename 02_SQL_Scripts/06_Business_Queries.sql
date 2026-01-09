-- =====================================================================
-- InkWave Publishing Data Warehouse - Profitability Analysis Queries
-- Oracle 19c+ Compatible
-- Author: Data Engineering Team
-- Date: 2025-12-07
-- Purpose: Business intelligence queries for profitability analysis
-- =====================================================================

-- =====================================================================
-- Query 1: Top 5 Editions by Profit Margin per Region (Rolling 12 Months)
-- =====================================================================
WITH rolling_12m_sales AS (
    SELECT 
        p.edition_id,
        p.product_title,
        dc.station_region,
        SUM(f.net_amount_gbp) as total_revenue,
        SUM(f.total_cost_gbp) as total_cost,
        SUM(f.gross_profit_gbp) as total_profit,
        AVG(f.gross_margin_pct) as avg_margin_pct,
        COUNT(DISTINCT f.time_key) as days_sold
    FROM FACT_SALES f
    JOIN DIM_TIME t ON f.time_key = t.time_key
    JOIN DIM_PRODUCT p ON f.product_key = p.product_key AND p.is_current = 'Y'
    JOIN DIM_DISTRIBUTION_CENTER dc ON f.dc_key = dc.dc_key AND dc.is_current = 'Y'
    WHERE t.full_date >= ADD_MONTHS(SYSDATE, -12)
    GROUP BY p.edition_id, p.product_title, dc.station_region
),
ranked_editions AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY station_region ORDER BY avg_margin_pct DESC) as rank_in_region
    FROM rolling_12m_sales
)
SELECT 
    station_region as "Region",
    edition_id as "Edition ID",
    product_title as "Product Title",
    TO_CHAR(total_revenue, 'FM£999,999,990.00') as "Total Revenue",
    TO_CHAR(total_cost, 'FM£999,999,990.00') as "Total Cost",
    TO_CHAR(total_profit, 'FM£999,999,990.00') as "Gross Profit",
    TO_CHAR(avg_margin_pct, 'FM990.00') || '%' as "Avg Margin %",
    days_sold as "Days Sold"
FROM ranked_editions
WHERE rank_in_region <= 5
ORDER BY station_region, rank_in_region;

-- =====================================================================
-- Query 2: Author ROI Analysis (Total revenue - advances) / number of books
-- Note: Advances not in source data, using proxy calculation
-- =====================================================================
WITH author_performance AS (
    SELECT 
        a.author_id,
        a.full_name as author_name,
        a.primary_genre,
        COUNT(DISTINCT p.edition_id) as number_of_editions,
        SUM(f.net_amount_gbp) as total_revenue,
        SUM(f.total_cost_gbp) as total_costs,
        SUM(f.gross_profit_gbp) as total_profit,
        AVG(f.gross_margin_pct) as avg_margin_pct,
        COUNT(DISTINCT f.sales_fact_key) as total_transactions,
        SUM(f.quantity_sold) as total_units_sold
    FROM FACT_SALES f
    JOIN DIM_AUTHOR a ON f.author_key = a.author_key AND a.is_current = 'Y'
    JOIN DIM_PRODUCT p ON f.product_key = p.product_key AND p.is_current = 'Y'
    JOIN DIM_TIME t ON f.time_key = t.time_key
    WHERE t.full_date >= ADD_MONTHS(SYSDATE, -12)
    GROUP BY a.author_id, a.full_name, a.primary_genre
)
SELECT 
    author_id as "Author ID",
    author_name as "Author Name",
    primary_genre as "Genre",
    number_of_editions as "# Editions",
    TO_CHAR(total_revenue, 'FM£999,999,990.00') as "Total Revenue",
    TO_CHAR(total_profit, 'FM£999,999,990.00') as "Total Profit",
    TO_CHAR(total_revenue / NULLIF(number_of_editions, 0), 'FM£999,999,990.00') as "Revenue per Edition",
    TO_CHAR(total_profit / NULLIF(number_of_editions, 0), 'FM£999,999,990.00') as "Profit per Edition",
    TO_CHAR(avg_margin_pct, 'FM990.00') || '%' as "Avg Margin %",
    total_units_sold as "Units Sold"
FROM author_performance
ORDER BY total_profit DESC;

-- =====================================================================
-- Query 3: Channel Efficiency - Revenue per Marketing Spend
-- Note: Marketing spend not in source data, using discount as proxy
-- =====================================================================
WITH channel_metrics AS (
    SELECT 
        c.channel_code,
        c.channel_name,
        c.channel_type,
        t.year,
        t.quarter_name,
        COUNT(DISTINCT f.sales_fact_key) as transaction_count,
        SUM(f.quantity_sold) as total_units,
        SUM(f.gross_amount_gbp) as gross_revenue,
        SUM(f.discount_amount_gbp) as total_discounts,
        SUM(f.net_amount_gbp) as net_revenue,
        SUM(f.gross_profit_gbp) as total_profit,
        AVG(f.gross_margin_pct) as avg_margin_pct,
        AVG(f.discount_rate) * 100 as avg_discount_pct
    FROM FACT_SALES f
    JOIN DIM_CHANNEL c ON f.channel_key = c.channel_key
    JOIN DIM_TIME t ON f.time_key = t.time_key
    WHERE t.year >= EXTRACT(YEAR FROM SYSDATE) - 1
    GROUP BY c.channel_code, c.channel_name, c.channel_type, t.year, t.quarter_name
)
SELECT 
    channel_name as "Channel",
    channel_type as "Type",
    year || ' ' || quarter_name as "Period",
    transaction_count as "Transactions",
    total_units as "Units Sold",
    TO_CHAR(net_revenue, 'FM£999,999,990.00') as "Net Revenue",
    TO_CHAR(total_discounts, 'FM£999,999,990.00') as "Total Discounts",
    TO_CHAR(total_profit, 'FM£999,999,990.00') as "Gross Profit",
    TO_CHAR(avg_margin_pct, 'FM990.00') || '%' as "Avg Margin %",
    TO_CHAR(avg_discount_pct, 'FM990.00') || '%' as "Avg Discount %",
    TO_CHAR(net_revenue / NULLIF(transaction_count, 0), 'FM£999,990.00') as "Revenue per Transaction",
    TO_CHAR(net_revenue / NULLIF(total_discounts, 0), 'FM999.99') as "Revenue per £ Discount"
FROM channel_metrics
ORDER BY year DESC, quarter_name, net_revenue DESC;

-- =====================================================================
-- Query 4: Product Type Profitability Comparison
-- =====================================================================
SELECT 
    pt.product_type_name as "Product Type",
    pt.is_physical as "Physical",
    t.year as "Year",
    COUNT(DISTINCT f.sales_fact_key) as "Transactions",
    SUM(f.quantity_sold) as "Units Sold",
    TO_CHAR(SUM(f.net_amount_gbp), 'FM£999,999,990.00') as "Total Revenue",
    TO_CHAR(SUM(f.total_cost_gbp), 'FM£999,999,990.00') as "Total Cost",
    TO_CHAR(SUM(f.gross_profit_gbp), 'FM£999,999,990.00') as "Gross Profit",
    TO_CHAR(AVG(f.gross_margin_pct), 'FM990.00') || '%' as "Avg Margin %",
    TO_CHAR(SUM(f.net_amount_gbp) / NULLIF(SUM(f.quantity_sold), 0), 'FM£990.00') as "Avg Revenue per Unit",
    TO_CHAR(SUM(f.gross_profit_gbp) / NULLIF(SUM(f.quantity_sold), 0), 'FM£990.00') as "Avg Profit per Unit"
FROM FACT_SALES f
JOIN DIM_PRODUCT_TYPE pt ON f.product_type_key = pt.product_type_key
JOIN DIM_TIME t ON f.time_key = t.time_key
WHERE t.year >= EXTRACT(YEAR FROM SYSDATE) - 2
GROUP BY pt.product_type_name, pt.is_physical, t.year
ORDER BY t.year DESC, SUM(f.net_amount_gbp) DESC;

-- =====================================================================
-- Query 5: Monthly Profitability Trend Analysis
-- =====================================================================
WITH monthly_profit AS (
    SELECT 
        t.year,
        t.month_number,
        t.month_name,
        SUM(f.net_amount_gbp) as revenue,
        SUM(f.total_cost_gbp) as cost,
        SUM(f.gross_profit_gbp) as profit,
        AVG(f.gross_margin_pct) as margin_pct,
        LAG(SUM(f.net_amount_gbp)) OVER (ORDER BY t.year, t.month_number) as prev_month_revenue,
        LAG(SUM(f.gross_profit_gbp)) OVER (ORDER BY t.year, t.month_number) as prev_month_profit
    FROM FACT_SALES f
    JOIN DIM_TIME t ON f.time_key = t.time_key
    WHERE t.year >= EXTRACT(YEAR FROM SYSDATE) - 1
    GROUP BY t.year, t.month_number, t.month_name
)
SELECT 
    year || ' ' || month_name as "Month",
    TO_CHAR(revenue, 'FM£999,999,990.00') as "Revenue",
    TO_CHAR(cost, 'FM£999,999,990.00') as "Cost",
    TO_CHAR(profit, 'FM£999,999,990.00') as "Profit",
    TO_CHAR(margin_pct, 'FM990.00') || '%' as "Margin %",
    TO_CHAR(((revenue - prev_month_revenue) / NULLIF(prev_month_revenue, 0)) * 100, 'FM990.00') || '%' as "Revenue Growth %",
    TO_CHAR(((profit - prev_month_profit) / NULLIF(prev_month_profit, 0)) * 100, 'FM990.00') || '%' as "Profit Growth %",
    CASE 
        WHEN revenue > prev_month_revenue THEN '↑'
        WHEN revenue < prev_month_revenue THEN '↓'
        ELSE '→'
    END as "Trend"
FROM monthly_profit
ORDER BY year, month_number;

-- =====================================================================
-- Query 6: Distribution Center Profitability Ranking
-- =====================================================================
SELECT 
    dc.station_code as "DC Code",
    dc.station_name as "DC Name",
    dc.station_region as "Region",
    COUNT(DISTINCT f.sales_fact_key) as "Transactions",
    SUM(f.quantity_sold) as "Units Sold",
    TO_CHAR(SUM(f.net_amount_gbp), 'FM£999,999,990.00') as "Total Revenue",
    TO_CHAR(SUM(f.gross_profit_gbp), 'FM£999,999,990.00') as "Gross Profit",
    TO_CHAR(AVG(f.gross_margin_pct), 'FM990.00') || '%' as "Avg Margin %",
    RANK() OVER (ORDER BY SUM(f.gross_profit_gbp) DESC) as "Profit Rank",
    TO_CHAR(SUM(f.net_amount_gbp) / NULLIF(COUNT(DISTINCT f.sales_fact_key), 0), 'FM£999,990.00') as "Revenue per Transaction"
FROM FACT_SALES f
JOIN DIM_DISTRIBUTION_CENTER dc ON f.dc_key = dc.dc_key AND dc.is_current = 'Y'
JOIN DIM_TIME t ON f.time_key = t.time_key
WHERE t.full_date >= ADD_MONTHS(SYSDATE, -12)
GROUP BY dc.station_code, dc.station_name, dc.station_region
ORDER BY SUM(f.gross_profit_gbp) DESC;

-- =====================================================================
-- End of Profitability Analysis Queries
-- =====================================================================
