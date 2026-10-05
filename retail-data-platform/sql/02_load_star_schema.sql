-- =====================================================================================
-- Load the star schema from the processed layer.
-- stg_transactions = the cleaned, deduplicated Parquet in s3://.../processed/transactions/
-- (exposed as a Redshift Spectrum / Athena external table through the Glue Data Catalog)
-- =====================================================================================

-- ---------- Dim_Date: generate a calendar for the full range ----------
-- (On Redshift, generate_series is leader-node only: generate the calendar once with the
--  Glue job or a small Python script and COPY it in. Athena/Trino: use SEQUENCE().)
INSERT INTO dim_date
SELECT
    CAST(TO_CHAR(d, 'YYYYMMDD') AS INTEGER)       AS date_key,
    d                                             AS full_date,
    EXTRACT(DAY FROM d)                           AS day_of_month,
    TRIM(TO_CHAR(d, 'Day'))                       AS day_name,
    EXTRACT(ISODOW FROM d)                        AS day_of_week,
    EXTRACT(WEEK FROM d)                          AS week_of_year,
    EXTRACT(MONTH FROM d)                         AS month_number,
    TRIM(TO_CHAR(d, 'Month'))                     AS month_name,
    EXTRACT(QUARTER FROM d)                       AS quarter_number,
    EXTRACT(YEAR FROM d)                          AS year_number,
    EXTRACT(ISODOW FROM d) IN (6, 7)              AS is_weekend
FROM (SELECT CAST(generate_series AS DATE) AS d
      FROM generate_series(DATE '2025-01-01', DATE '2027-12-31', INTERVAL 1 DAY)) cal;

-- ---------- Dim_Region ----------
INSERT INTO dim_region (region_key, region_code, region_name, country)
SELECT ROW_NUMBER() OVER (ORDER BY region) AS region_key,
       region                               AS region_code,
       INITCAP(region)                      AS region_name,
       'India'                              AS country
FROM (SELECT DISTINCT region FROM stg_transactions) r;

-- ---------- Dim_Customer (initial load: one current version per customer) ----------
INSERT INTO dim_customer
SELECT ROW_NUMBER() OVER (ORDER BY customer_id) AS customer_key,
       customer_id, customer_name, customer_email, region,
       first_seen                               AS effective_from,
       DATE '9999-12-31'                        AS effective_to,
       TRUE                                     AS is_current
FROM (
    SELECT customer_id, customer_name, customer_email, region,
           MIN(transaction_date) OVER (PARTITION BY customer_id) AS first_seen,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY transaction_ts DESC) AS rn
    FROM stg_transactions
) latest
WHERE rn = 1;

-- Incremental SCD Type 2 (daily), shown for completeness:
--   1) UPDATE dim_customer SET effective_to = CURRENT_DATE - 1, is_current = FALSE
--      WHERE is_current AND customer_id IN (customers whose email/region changed today);
--   2) INSERT a new row with a new customer_key, effective_from = CURRENT_DATE, is_current = TRUE
--      for changed customers and brand-new customers.

-- ---------- Fact_Transactions ----------
INSERT INTO fact_transactions (transaction_id, date_key, customer_key, region_key, store_id,
                               product_id, payment_method, status, quantity, unit_price,
                               amount, transaction_ts)
SELECT s.transaction_id,
       CAST(TO_CHAR(s.transaction_date, 'YYYYMMDD') AS INTEGER),
       c.customer_key,
       r.region_key,
       s.store_id, s.product_id, s.payment_method, s.status,
       s.quantity, s.unit_price, s.amount, s.transaction_ts
FROM stg_transactions s
JOIN dim_customer c
  ON c.customer_id = s.customer_id
 AND s.transaction_date BETWEEN c.effective_from AND c.effective_to   -- SCD2-correct lookup
JOIN dim_region r
  ON r.region_code = s.region;
-- Daily incremental load in Redshift: load the new day into a temp table and use
-- MERGE INTO fact_transactions USING tmp ON transaction_id ... (idempotent on rerun).
