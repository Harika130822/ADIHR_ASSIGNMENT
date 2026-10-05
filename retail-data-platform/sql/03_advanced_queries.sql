-- =====================================================================================
-- Section H: Advanced SQL  (written against the star schema; ANSI SQL that runs on
-- Amazon Redshift and Athena/Trino. Tested locally with DuckDB via tests/run_sql_tests.py)
-- =====================================================================================

-- -------------------------------------------------------------------------------------
-- H1. Top 10 customers by total transaction amount
--     Only COMPLETED sales count as revenue. DENSE_RANK keeps ties honest.
-- -------------------------------------------------------------------------------------
WITH customer_totals AS (
    SELECT c.customer_id,
           c.customer_name,
           COUNT(*)        AS txn_count,
           SUM(f.amount)   AS total_amount
    FROM fact_transactions f
    JOIN dim_customer c ON c.customer_key = f.customer_key
    WHERE f.status = 'COMPLETED'
    GROUP BY c.customer_id, c.customer_name
)
SELECT customer_id, customer_name, txn_count, total_amount,
       DENSE_RANK() OVER (ORDER BY total_amount DESC) AS sales_rank
FROM customer_totals
ORDER BY total_amount DESC
LIMIT 10;

-- -------------------------------------------------------------------------------------
-- H2. Region-wise sales for the last 30 days (including today)
--     Filtering on dim_date.full_date lets Redshift prune using the date_key sort key.
-- -------------------------------------------------------------------------------------
SELECT r.region_name,
       COUNT(*)                                   AS txn_count,
       SUM(f.amount)                              AS total_sales,
       ROUND(100.0 * SUM(f.amount) / SUM(SUM(f.amount)) OVER (), 2) AS pct_of_total
FROM fact_transactions f
JOIN dim_region r ON r.region_key = f.region_key
JOIN dim_date   d ON d.date_key   = f.date_key
WHERE f.status = 'COMPLETED'
  AND d.full_date >= CURRENT_DATE - INTERVAL '30' DAY
  AND d.full_date <  CURRENT_DATE + INTERVAL '1' DAY
GROUP BY r.region_name
ORDER BY total_sales DESC;

-- -------------------------------------------------------------------------------------
-- H3. Month-over-month sales growth %
--     LAG() looks at the previous month's row; NULLIF avoids divide-by-zero.
-- -------------------------------------------------------------------------------------
WITH monthly AS (
    SELECT d.year_number,
           d.month_number,
           SUM(f.amount) AS monthly_sales
    FROM fact_transactions f
    JOIN dim_date d ON d.date_key = f.date_key
    WHERE f.status = 'COMPLETED'
    GROUP BY d.year_number, d.month_number
)
SELECT year_number,
       month_number,
       monthly_sales,
       LAG(monthly_sales) OVER (ORDER BY year_number, month_number) AS prev_month_sales,
       ROUND(100.0 * (monthly_sales - LAG(monthly_sales) OVER (ORDER BY year_number, month_number))
             / NULLIF(LAG(monthly_sales) OVER (ORDER BY year_number, month_number), 0), 2)
                                                                     AS mom_growth_pct
FROM monthly
ORDER BY year_number, month_number;

-- -------------------------------------------------------------------------------------
-- H4. Identify duplicate transactions using the business key (transaction_id)
--     Run against the RAW landing table (raw_transactions) - that is where duplicates
--     live. The fact table should always return 0 rows here (a useful DQ assertion).
-- -------------------------------------------------------------------------------------
-- 4a. Which keys are duplicated and how many times
SELECT transaction_id,
       COUNT(*)                         AS copies,
       COUNT(DISTINCT status)           AS distinct_statuses,   -- >1 means a correction, not a pure copy
       MIN(ingest_ts)                   AS first_ingested,
       MAX(ingest_ts)                   AS last_ingested
FROM raw_transactions
GROUP BY transaction_id
HAVING COUNT(*) > 1
ORDER BY copies DESC, transaction_id;

-- 4b. Every duplicate row, flagging which one the ETL keeps (latest ingest wins)
SELECT *
FROM (
    SELECT t.*,
           ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY ingest_ts DESC) AS rn,
           COUNT(*)     OVER (PARTITION BY transaction_id)                         AS copies
    FROM raw_transactions t
) x
WHERE copies > 1
ORDER BY transaction_id, rn;            -- rn = 1 is kept, rn > 1 are discarded

-- -------------------------------------------------------------------------------------
-- H5. Customers with NO transactions in the last 90 days
--     NOT EXISTS handles NULLs correctly (NOT IN would silently return nothing if any
--     customer_key were NULL). Also returns last purchase date for the CRM team.
-- -------------------------------------------------------------------------------------
SELECT c.customer_id,
       c.customer_name,
       c.customer_email,
       MAX(d.full_date)                         AS last_transaction_date,
       CURRENT_DATE - MAX(d.full_date)          AS days_since_last_txn
FROM dim_customer c
LEFT JOIN dim_customer cv     ON cv.customer_id = c.customer_id      -- all SCD2 versions
LEFT JOIN fact_transactions f ON f.customer_key = cv.customer_key
LEFT JOIN dim_date d          ON d.date_key     = f.date_key
WHERE c.is_current
  AND NOT EXISTS (
        SELECT 1
        FROM fact_transactions f2
        JOIN dim_customer c2 ON c2.customer_key = f2.customer_key
        JOIN dim_date d2     ON d2.date_key     = f2.date_key
        WHERE c2.customer_id = c.customer_id
          AND d2.full_date >= CURRENT_DATE - INTERVAL '90' DAY
  )
GROUP BY c.customer_id, c.customer_name, c.customer_email
ORDER BY last_transaction_date NULLS FIRST;
