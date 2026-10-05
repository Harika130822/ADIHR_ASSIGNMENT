"""
Local test harness for Sections D + H.

Builds the star schema in an in-memory DuckDB database from the ETL output
(samples/data/processed) and runs every query in sql/03_advanced_queries.sql,
printing results you can screenshot and asserting they make sense.

The SQL files are written for Redshift/Athena; this script only adds a few
small compatibility shims (TO_CHAR, INITCAP, GETDATE) and strips Redshift
physical-design clauses (DISTSTYLE / DISTKEY / SORTKEY) so DuckDB accepts them.

Usage:  python tests/run_sql_tests.py
"""
import os
import re
import sys

import duckdb

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SAMPLE = os.path.join(ROOT, "samples", "data")
TODAY = os.environ.get("AS_OF_DATE", "2026-10-01")   # fixed "today" so results are reproducible


def load_sql(name):
    with open(os.path.join(ROOT, "sql", name)) as f:
        sql = f.read()
    sql = re.sub(r"^\s*(DISTSTYLE|DISTKEY|SORTKEY)[^;\n]*", "", sql, flags=re.MULTILINE | re.IGNORECASE)
    sql = sql.replace("CURRENT_DATE", f"DATE '{TODAY}'")
    return sql


def statements(sql):
    no_comments = re.sub(r"--[^\n]*", "", sql)
    return [s.strip() for s in no_comments.split(";") if s.strip()]


def main():
    con = duckdb.connect()
    con.execute("""
        CREATE MACRO to_char(d, f) AS strftime(d, CASE f WHEN 'YYYYMMDD' THEN '%Y%m%d'
                                                          WHEN 'Day' THEN '%A'
                                                          WHEN 'Month' THEN '%B' END);
        CREATE MACRO initcap(s) AS upper(s[1]) || lower(s[2:]);
        CREATE MACRO getdate() AS current_timestamp;
    """)
    con.execute(f"""
        CREATE VIEW stg_transactions AS
        SELECT * FROM read_parquet('{SAMPLE}/processed/transactions/**/*.parquet', hive_partitioning = 1);
        CREATE VIEW raw_transactions AS
        SELECT * FROM read_csv('{SAMPLE}/raw/transactions/*/*.csv', header = true, all_varchar = true);
    """)

    for s in statements(load_sql("01_star_schema_ddl.sql")) + statements(load_sql("02_load_star_schema.sql")):
        con.execute(s)
    for t in ["dim_date", "dim_region", "dim_customer", "fact_transactions"]:
        print(f"{t:<20} {con.sql(f'SELECT COUNT(*) FROM {t}').fetchone()[0]:>8} rows")

    # Referential integrity checks (Redshift won't enforce them, so we test them)
    orphans = con.sql("""SELECT COUNT(*) FROM fact_transactions f
                         LEFT JOIN dim_customer c USING (customer_key)
                         LEFT JOIN dim_region r USING (region_key)
                         LEFT JOIN dim_date d USING (date_key)
                         WHERE c.customer_key IS NULL OR r.region_key IS NULL OR d.date_key IS NULL""").fetchone()[0]
    assert orphans == 0, f"{orphans} fact rows have no matching dimension row"
    assert con.sql("SELECT COUNT(*) - COUNT(DISTINCT transaction_id) FROM fact_transactions").fetchone()[0] == 0
    print("PK/FK integrity checks passed\n")

    titles = ["H1 Top 10 customers", "H2 Region sales, last 30 days", "H3 Month-over-month growth",
              "H4a Duplicate keys in raw", "H4b Duplicate rows (rn=1 kept)", "H5 No txn in 90 days"]
    results = {}
    for title, q in zip(titles, statements(load_sql("03_advanced_queries.sql")), strict=True):
        rel = con.sql(q)
        results[title] = rel.fetchall()
        print(f"=== {title}  ({len(results[title])} rows)")
        rel.limit(10).show(max_width=140)

    assert len(results["H1 Top 10 customers"]) == 10
    assert len(results["H2 Region sales, last 30 days"]) == 5
    assert len(results["H4a Duplicate keys in raw"]) > 0, "generator injects duplicates; H4 should find them"
    dup_in_fact = con.sql("SELECT COUNT(*) FROM (SELECT transaction_id FROM fact_transactions "
                          "GROUP BY 1 HAVING COUNT(*) > 1)").fetchone()[0]
    assert dup_in_fact == 0
    assert len(results["H5 No txn in 90 days"]) > 0, "generator creates dormant customers"
    print("All SQL assertions passed.")


if __name__ == "__main__":
    sys.exit(main())
