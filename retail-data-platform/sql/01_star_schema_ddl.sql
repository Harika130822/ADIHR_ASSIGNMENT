-- =====================================================================================
-- Section D: Dimensional model (star schema) for retail reporting
-- Target: Amazon Redshift (works with minor changes on Athena/Iceberg or any ANSI DB)
--
--                 Dim_Date
--                    |
--   Dim_Customer -- Fact_Transactions -- Dim_Region
--
-- GRAIN of Fact_Transactions: ONE ROW PER COMPLETED-OR-NOT SALES TRANSACTION
--   (one transaction_id = one row). This is the most atomic level the source gives us,
--   so every report (by day, month, region, customer, store, payment method) can be
--   rolled up from it, and nothing is double counted. Aggregate tables in curated/
--   (daily_region_sales) are derived FROM this grain, never the other way round.
-- =====================================================================================

CREATE TABLE dim_date (
    date_key        INTEGER      NOT NULL,   -- smart key YYYYMMDD, e.g. 20260901
    full_date       DATE         NOT NULL,
    day_of_month    SMALLINT     NOT NULL,
    day_name        VARCHAR(10)  NOT NULL,
    day_of_week     SMALLINT     NOT NULL,   -- 1=Monday .. 7=Sunday
    week_of_year    SMALLINT     NOT NULL,
    month_number    SMALLINT     NOT NULL,
    month_name      VARCHAR(10)  NOT NULL,
    quarter_number  SMALLINT     NOT NULL,
    year_number     SMALLINT     NOT NULL,
    is_weekend      BOOLEAN      NOT NULL,
    CONSTRAINT pk_dim_date PRIMARY KEY (date_key)
)
DISTSTYLE ALL;            -- small table: copy to every node, joins never shuffle

CREATE TABLE dim_region (
    region_key      INTEGER      NOT NULL,   -- surrogate key
    region_code     VARCHAR(20)  NOT NULL,   -- natural/business key: NORTH, SOUTH ...
    region_name     VARCHAR(50)  NOT NULL,
    country         VARCHAR(50)  NOT NULL,
    CONSTRAINT pk_dim_region PRIMARY KEY (region_key),
    CONSTRAINT uq_dim_region_code UNIQUE (region_code)
)
DISTSTYLE ALL;

-- SCD Type 2: if a customer's region/email changes we keep history,
-- so old sales stay attributed to the region the customer was in at the time.
CREATE TABLE dim_customer (
    customer_key    BIGINT       NOT NULL,   -- surrogate key (one per version)
    customer_id     VARCHAR(20)  NOT NULL,   -- business key from source
    customer_name   VARCHAR(200),
    customer_email  VARCHAR(200),            -- PII: column-level access via Lake Formation
    home_region_code VARCHAR(20),
    effective_from  DATE         NOT NULL,
    effective_to    DATE         NOT NULL,   -- 9999-12-31 for the current version
    is_current      BOOLEAN      NOT NULL,
    CONSTRAINT pk_dim_customer PRIMARY KEY (customer_key)
)
DISTKEY (customer_key);

CREATE TABLE fact_transactions (
    transaction_id  VARCHAR(30)  NOT NULL,   -- degenerate dimension / business key
    date_key        INTEGER      NOT NULL,
    customer_key    BIGINT       NOT NULL,
    region_key      INTEGER      NOT NULL,
    store_id        VARCHAR(10),
    product_id      VARCHAR(10),
    payment_method  VARCHAR(20),
    status          VARCHAR(20),
    quantity        INTEGER,
    unit_price      DECIMAL(12,2),
    amount          DECIMAL(14,2),           -- additive measure
    transaction_ts  TIMESTAMP,
    load_ts         TIMESTAMP DEFAULT GETDATE(),
    CONSTRAINT pk_fact_transactions PRIMARY KEY (transaction_id),
    CONSTRAINT fk_fact_date     FOREIGN KEY (date_key)     REFERENCES dim_date (date_key),
    CONSTRAINT fk_fact_customer FOREIGN KEY (customer_key) REFERENCES dim_customer (customer_key),
    CONSTRAINT fk_fact_region   FOREIGN KEY (region_key)   REFERENCES dim_region (region_key)
)
DISTKEY (customer_key)        -- co-located with dim_customer for the most common join
SORTKEY (date_key);           -- most queries filter on a date range -> zone-map pruning

-- Note: Redshift does not ENFORCE PK/FK constraints; the optimizer uses them as hints.
-- Uniqueness is guaranteed upstream by the ETL dedup on transaction_id, and checked by
-- the data-quality query in 03_advanced_queries.sql (Q4).
