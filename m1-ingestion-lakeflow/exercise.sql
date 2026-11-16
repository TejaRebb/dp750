-- MODULE 1: Lakeflow Declarative Pipelines (SQL). Fill blanks __(n)__.

-- BRONZE: incremental JSON from ADLS. New source columns must NOT stop the
-- pipeline and must NOT be added to the schema; capture them in _rescued_data.
CREATE OR REFRESH __(1)__ bronze_orders
AS SELECT *, _metadata.file_path AS src_file
FROM __(2)__(
  "abfss://landing@stdp750.dfs.core.windows.net/orders/",
  format => "json",
  schemaEvolutionMode => "__(3)__"
);

-- SILVER: data quality
CREATE OR REFRESH STREAMING TABLE silver_orders (
  CONSTRAINT valid_id  EXPECT (order_id IS NOT NULL) __(4)__,  -- discard bad rows
  CONSTRAINT valid_amt EXPECT (amount >= 0),                    -- __(5)__ what happens to a violating row?
  CONSTRAINT valid_ts  EXPECT (order_ts IS NOT NULL) __(6)__    -- stop the update entirely
)
AS SELECT * FROM __(7)__ bronze_orders;                         -- this line errors as-is without it

-- SCD TYPE 2 dimension from CDC feed. cdc_customers columns:
-- customer_id, name, tier, operation ('INSERT'|'UPDATE'|'DELETE'), op_ts (source commit time), ingest_ts (landing time)
-- Events arrive OUT OF ORDER.
CREATE OR REFRESH STREAMING TABLE dim_customer;

__(8)__ INTO dim_customer
FROM STREAM(cdc_customers)
KEYS (customer_id)
APPLY AS DELETE WHEN __(9)__
SEQUENCE BY __(10)__
COLUMNS * EXCEPT (operation, ingest_ts)
STORED AS __(11)__;

-- GOLD: daily revenue; must reflect late corrections/deletes in silver, full recompute OK.
CREATE OR REFRESH __(12)__ gold_daily_revenue
AS SELECT date(order_ts) AS d, sum(amount) AS revenue
FROM silver_orders GROUP BY date(order_ts);
