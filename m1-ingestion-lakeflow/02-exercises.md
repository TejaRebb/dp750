# M1 — Exercises (SECOND PASS, guided)

Fill in the numbered blanks `__(n)__` or find the bugs. The hints are directly under each code block. Try first, then open the solution.

---

## Exercise 1 — SQL pipeline, bronze → silver → SCD2 dim → gold (warm-up)

**Scenario.** JSON orders land in ADLS. Bronze must ingest incrementally. If new source columns appear, they must **not** stop the pipeline and must **not** be added to the schema; capture them in `_rescued_data`. Silver applies data-quality rules. A CDC feed of customers (out of order) builds an SCD Type 2 dimension. Gold is a daily revenue table that must reflect late corrections/deletes in silver. A full recompute is fine.

```sql
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
```

**Hints**
- (1) Which object type gives incremental, exactly-once file ingestion?
- (2) What does a streaming table need in front of `read_files` to make it use Auto Loader? Two words.
- (3) Of the four classic modes, which one never fails and never evolves the schema?
- (4) What is the SQL clause that discards violating rows?
- (5) No `ON VIOLATION` clause. What is the default action?
- (6) What is the SQL clause that aborts and rolls back the update?
- (7) A streaming table reading a table needs streaming semantics.
- (8) The legacy keyword pair still works as a standalone statement. The current form wraps it in a flow.
- (9) A boolean condition on the `operation` column.
- (10) Out-of-order events: sort by when the change happened at the source, not when it landed.
- (11) The SCD type that keeps history.
- (12) Which object type always reflects upstream updates/deletes?

<details><summary>Solution</summary>

```sql
CREATE OR REFRESH STREAMING TABLE bronze_orders
AS SELECT *, _metadata.file_path AS src_file
FROM STREAM read_files(
  "abfss://landing@stdp750.dfs.core.windows.net/orders/",
  format => "json",
  schemaEvolutionMode => "rescue"
);

CREATE OR REFRESH STREAMING TABLE silver_orders (
  CONSTRAINT valid_id  EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_amt EXPECT (amount >= 0),
  CONSTRAINT valid_ts  EXPECT (order_ts IS NOT NULL) ON VIOLATION FAIL UPDATE
)
AS SELECT * FROM STREAM bronze_orders;

CREATE OR REFRESH STREAMING TABLE dim_customer;

APPLY CHANGES INTO dim_customer
-- current equivalent:  CREATE FLOW dim_customer_cdc AS AUTO CDC INTO dim_customer
FROM STREAM(cdc_customers)
KEYS (customer_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY op_ts
COLUMNS * EXCEPT (operation, ingest_ts)
STORED AS SCD TYPE 2;

CREATE OR REFRESH MATERIALIZED VIEW gold_daily_revenue
AS SELECT date(order_ts) AS d, sum(amount) AS revenue
FROM silver_orders GROUP BY date(order_ts);
```

1. `STREAMING TABLE`: file ingestion must be incremental and exactly-once. An MV over files would re-read everything.
2. `STREAM read_files`: `read_files` in a streaming table must be preceded by `STREAM`, which makes it use Auto Loader. `schemaLocation` isn't needed here.
3. `rescue`: never fails, never evolves, puts new columns in `_rescued_data`. `addNewColumns` would fail once and evolve; `none` would drop them.
4. `ON VIOLATION DROP ROW`: violating rows are dropped before the write and counted in `dropped_records`.
5. Warn (the default): the violating row **is written** to `silver_orders`, and `failed_records` is incremented in the event log.
6. `ON VIOLATION FAIL UPDATE`: the update fails and rolls back atomically. No metrics are recorded for it.
7. `STREAM`: without it this is a batch read, and a streaming table definition errors.
8. `APPLY CHANGES`: the legacy statement, still valid. The current form is `CREATE FLOW <name> AS AUTO CDC INTO ...`, with otherwise identical clauses.
9. `operation = 'DELETE'`: without this clause, DELETE events would be upserted like normal rows.
10. `op_ts`: the source commit time defines the true order. `ingest_ts` only reflects arrival order and would break out-of-order handling.
11. `SCD TYPE 2`: adds `__START_AT`/`__END_AT`. The default would be SCD TYPE 1.
12. `MATERIALIZED VIEW`: batch semantics, so it reflects late corrections/deletes. An ST would only see new appends, and its stream would fail on update/delete commits.

</details>

---

## Exercise 2 — Python Auto Loader in a Lakeflow Job (no pipeline)

**Scenario.** CSV sales files (with headers) land in `abfss://landing@stdp750.dfs.core.windows.net/sales/`. The landing folder already holds 3 years of history, and the team wants **only files that arrive from now on**. `sale_date` must be a DATE and `amount` a `DECIMAL(18,2)`; every other column should be type-inferred, not strings. The external location has file events enabled. The job runs hourly on serverless, processes everything available, and stops.

```python
(spark.readStream
   .format(__(1)__)
   .option("cloudFiles.format", "csv")
   .option("header", "true")
   .option(__(2)__, "/Volumes/ops/landing/_meta/sales/schema")
   .option("cloudFiles.inferColumnTypes", __(3)__)
   .option("cloudFiles.schemaHints", __(4)__)
   .option("cloudFiles.includeExistingFiles", __(5)__)
   .option("cloudFiles.useManagedFileEvents", __(6)__)
   .load("abfss://landing@stdp750.dfs.core.windows.net/sales/")
 .writeStream
   .option(__(7)__, "/Volumes/ops/landing/_meta/sales/checkpoint")
   .trigger(__(8)__)
   .toTable("main.bronze.sales"))
```

**Hints**
- (1) The Auto Loader source name.
- (2) Outside a pipeline, where does Auto Loader store the inferred schema and its history?
- (3) The default is the opposite of what you need for CSV.
- (4) A DDL string covering only the two columns that need specific types.
- (5) Skip the 3 years of history. When is this option evaluated?
- (6) Use the external location's file events.
- (7) Streaming state for exactly-once.
- (8) Serverless jobs only allow two trigger types. Which one processes everything available, possibly in several batches, and stops?
- (9) **Question:** no `schemaEvolutionMode` is set. Which mode is in effect, and what happens on the next run when a new column `promo_code` appears?
- (10) **Question:** after a week, someone flips `includeExistingFiles` to `true` to backfill history. What happens?

<details><summary>Solution</summary>

```python
(spark.readStream
   .format("cloudFiles")
   .option("cloudFiles.format", "csv")
   .option("header", "true")
   .option("cloudFiles.schemaLocation", "/Volumes/ops/landing/_meta/sales/schema")
   .option("cloudFiles.inferColumnTypes", "true")
   .option("cloudFiles.schemaHints", "sale_date DATE, amount DECIMAL(18,2)")
   .option("cloudFiles.includeExistingFiles", "false")
   .option("cloudFiles.useManagedFileEvents", "true")
   .load("abfss://landing@stdp750.dfs.core.windows.net/sales/")
 .writeStream
   .option("checkpointLocation", "/Volumes/ops/landing/_meta/sales/checkpoint")
   .trigger(availableNow=True)
   .toTable("main.bronze.sales"))
```

1. `"cloudFiles"`: the Auto Loader Structured Streaming source.
2. `"cloudFiles.schemaLocation"`: required outside pipelines for inference and evolution. It stores `_schemas` and can share the checkpoint folder.
3. `"true"`: the Auto Loader default is `false`, which makes every CSV column a STRING.
4. `"sale_date DATE, amount DECIMAL(18,2)"`: hints pin specific columns and the rest stays inferred. Hints apply only because no `.schema()` is passed.
5. `"false"`: only new files are processed. This is honored only on the first start of this checkpoint.
6. `"true"`: file events give notification-level discovery without per-stream queues. The first run still does one listing to sync.
7. `"checkpointLocation"`: required for exactly-once file tracking and stream progress.
8. `availableNow=True`: processes everything available, possibly over several micro-batches, then stops. Serverless supports only `availableNow` and `once`.
9. The mode is `addNewColumns`, the default when no schema is provided (hints don't count as a schema). The run merges `promo_code` into the schema at `schemaLocation`, then **fails** with `UnknownFieldException`. The next run, or an automatic Job retry, picks it up. Configure job retries.
10. Nothing changes. `includeExistingFiles` is evaluated only when the stream first starts. To backfill, use a new checkpoint (a separate backfill stream), or load the history once with `COPY INTO`.

</details>

---

## Exercise 3 — Python pipeline bug hunt (6 bugs)

**Scenario.** A Lakeflow pipeline in Python. Bronze ingests JSON events with Auto Loader. Silver must be a **streaming table** that drops rows failing any of three rules. Rows that fail must also be kept in a quarantine table for review. Gold is daily counts per event type and must stay correct when silver is corrected. The lines marked `# (n)` each contain one bug. Explain each bug and fix it.

```python
from pyspark import pipelines as dp
from pyspark.sql.functions import col, to_date

SRC = "abfss://landing@stdp750.dfs.core.windows.net/events/"

rules = {
    "valid_id":   "event_id IS NOT NULL",
    "valid_type": "event_type IN ('click','view','purchase')",
    "valid_ts":   "event_ts IS NOT NULL",
}

@dp.table(comment="raw events")
def bronze_events():
    return (spark.read.format("cloudFiles")                          # (1)
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .load(SRC))

@dp.table(comment="clean events")
@dp.expect_or_drop(rules)                                            # (2)
def silver_events():
    df = spark.read.table("bronze_events")                           # (3)
    print(f"silver rows: {df.count()}")                              # (4)
    return df

@dp.table(comment="quarantined events")
def quarantine_events():
    return (spark.readStream.table("bronze_events")
            .filter(" AND ".join(rules.values())))                   # (5)

@dp.table(comment="daily counts")                                    # (6)
def gold_daily_counts():
    return (spark.readStream.table("silver_events")
            .groupBy(to_date("event_ts").alias("d"), "event_type")
            .count())
```

**Hints**
- (1) Auto Loader works with only one kind of read.
- (2) What argument type is `rules`, and which decorator accepts it?
- (3) Silver must be a streaming table. What does `@dp.table` create from a batch DataFrame?
- (4) What does the Python coding-rules list say about actions inside dataset functions?
- (5) The quarantine should hold the rows that fail.
- (6) Gold must stay correct when silver is corrected, which is not what an unbounded streaming aggregation gives you. Which decorator and read type fit?

<details><summary>Solution</summary>

```python
from pyspark import pipelines as dp
from pyspark.sql.functions import col, to_date

SRC = "abfss://landing@stdp750.dfs.core.windows.net/events/"

rules = {
    "valid_id":   "event_id IS NOT NULL",
    "valid_type": "event_type IN ('click','view','purchase')",
    "valid_ts":   "event_ts IS NOT NULL",
}

@dp.table(comment="raw events")
def bronze_events():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .load(SRC))

@dp.table(comment="clean events")
@dp.expect_all_or_drop(rules)
def silver_events():
    return spark.readStream.table("bronze_events")

@dp.table(comment="quarantined events")
def quarantine_events():
    return (spark.readStream.table("bronze_events")
            .filter(f"NOT ({' AND '.join(rules.values())})"))

@dp.materialized_view(comment="daily counts")
def gold_daily_counts():
    return (spark.read.table("silver_events")
            .groupBy(to_date("event_ts").alias("d"), "event_type")
            .count())
```

1. `spark.read` → `spark.readStream`: `cloudFiles` is a streaming-only source. In a pipeline you also skip `schemaLocation` and the checkpoint.
2. `expect_or_drop(rules)` → `expect_all_or_drop(rules)`: a dict of rules needs the `expect_all*` family. `expect_or_drop` takes `(name, condition)`.
3. `spark.read.table` → `spark.readStream.table`: with a batch DataFrame, `@dp.table` creates a **materialized view** (legacy behavior), not the required streaming table.
4. Remove `df.count()` / `print`: actions like `count()` and `collect()`, and side effects, are forbidden in dataset functions. The function is evaluated several times during planning and must only return a DataFrame.
5. Add `NOT (...)`: the quarantine must hold the **failing** rows. As written it duplicates the clean rows. This inverse-predicate pattern matches the MS Learn quarantine pattern.
6. `@dp.table` + `readStream` → `@dp.materialized_view` + `spark.read`: an MV recomputes when silver is corrected. An unbounded streaming aggregation needs watermarks/state and would not reflect corrections.

</details>

---

## Exercise 4 — AUTO CDC in Python: SCD2 history + SCD1 current + downstream (hardest)

**Scenario.** A Debezium feed (already flattened) lands in `raw.crm.customers_cdc` with columns `tenant_id, customer_id, name, tier, email, last_login, op ('c','u','d','r','t'), commit_ts, lsn`. A customer is unique by **tenant + customer**. Several events for the same key can share a `commit_ts`; `lsn` breaks the tie.

Requirements:
- **A.** `dim_customer_hist`: SCD2. Changes to `last_login` must **not** create new versions. `op`, `lsn` and `commit_ts` must not appear in the target.
- **B.** `dim_customer_current`: SCD1 from the same feed. `op = 't'` (truncate) must clear the table.
- **C.** You want to declare the SCD2 target schema explicitly.
- **D.** A downstream streaming table `customer_changes_audit` must stream **from** `dim_customer_current`.

```python
from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr, struct

@dp.temporary_view
def customers_cdc():
    return spark.readStream.table("raw.crm.customers_cdc")

# ---------- A: SCD2 history ----------
dp.__(1)__("dim_customer_hist")

dp.__(2)__(
    target = "dim_customer_hist",
    source = "customers_cdc",
    keys = __(3)__,
    sequence_by = __(4)__,
    apply_as_deletes = __(5)__,
    except_column_list = __(6)__,
    stored_as_scd_type = __(7)__,
    __(8)__ = ["last_login"],
)

# ---------- B: SCD1 current ----------
dp.create_streaming_table("dim_customer_current")

dp.create_auto_cdc_flow(
    target = "dim_customer_current",
    source = "customers_cdc",
    keys = ["tenant_id", "customer_id"],
    sequence_by = struct("commit_ts", "lsn"),
    apply_as_deletes = expr("op = 'd'"),
    __(9)__ = expr("op = 't'"),
    except_column_list = ["op", "commit_ts", "lsn"],
    stored_as_scd_type = 1,
)

# ---------- D: downstream stream from the SCD1 target ----------
@dp.table
def customer_changes_audit():
    return (spark.readStream
            .option(__(11)__, "true")
            .table("dim_customer_current"))
```

**Hints**
- (1) The function that declares an empty streaming table to act as a target.
- (2) The current name of `apply_changes`.
- (3) Composite key, as a list.
- (4) Two columns, ordered: the timestamp first, then the tie-breaker. Which Spark function combines them?
- (5) The Debezium delete code.
- (6) The three CDC metadata columns to keep out of the target.
- (7) The history-keeping SCD type (string or int accepted).
- (8) The parameter that stops a column from generating new versions without removing it from the target.
- (9) The truncate parameter. Why is this allowed on B but would be invalid on A?
- (10) **Question (C):** if you declare `dim_customer_hist` with an explicit schema, which extra columns must it include, and of what type here?
- (11) You can't plain-stream from a table that receives upserts and deletes. Which read option streams its changes?
- (12) **Question:** a delete for a key arrives 3 days after later events for that key. Which table property must you raise on the SCD2 target, and why?
- (13) **Question:** write requirement A as SQL.

<details><summary>Solution</summary>

```python
from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr, struct

@dp.temporary_view
def customers_cdc():
    return spark.readStream.table("raw.crm.customers_cdc")

dp.create_streaming_table("dim_customer_hist")

dp.create_auto_cdc_flow(
    target = "dim_customer_hist",
    source = "customers_cdc",
    keys = ["tenant_id", "customer_id"],
    sequence_by = struct("commit_ts", "lsn"),
    apply_as_deletes = expr("op = 'd'"),
    except_column_list = ["op", "commit_ts", "lsn"],
    stored_as_scd_type = "2",
    track_history_except_column_list = ["last_login"],
)

dp.create_streaming_table("dim_customer_current")

dp.create_auto_cdc_flow(
    target = "dim_customer_current",
    source = "customers_cdc",
    keys = ["tenant_id", "customer_id"],
    sequence_by = struct("commit_ts", "lsn"),
    apply_as_deletes = expr("op = 'd'"),
    apply_as_truncates = expr("op = 't'"),
    except_column_list = ["op", "commit_ts", "lsn"],
    stored_as_scd_type = 1,
)

@dp.table
def customer_changes_audit():
    return (spark.readStream
            .option("readChangeFeed", "true")
            .table("dim_customer_current"))
```

1. `create_streaming_table`: AUTO CDC needs a pre-declared streaming-table target.
2. `create_auto_cdc_flow`: replaces `apply_changes` with the same signature. `dlt.apply_changes(...)` would also work.
3. `["tenant_id", "customer_id"]`: a composite key as a list of strings (or `col()` without qualifiers).
4. `struct("commit_ts", "lsn")`: orders by `commit_ts`, then `lsn` on ties. Sequence values must never be NULL.
5. `expr("op = 'd'")`: without it, deletes would be upserted as rows.
6. `["op", "commit_ts", "lsn"]`: removes the CDC metadata from the target. In SCD2, the sequence values still populate `__START_AT`/`__END_AT`.
7. `"2"`: SCD Type 2 (the default is 1).
8. `track_history_except_column_list`: `last_login` stays in the target and is updated in place on the current row, but doesn't create a version. Putting it in `except_column_list` would wrongly drop the column.
9. `apply_as_truncates`: allowed because B is SCD1. Truncate is **SCD1 only**, so adding it to A (SCD2) is invalid.
10. `__START_AT` and `__END_AT`, with the **same type as `sequence_by`**. Here that is the struct `STRUCT<commit_ts: <type>, lsn: <type>>`, not a timestamp.
11. `"readChangeFeed"`: to stream from an AUTO CDC target, read its change data feed. A plain stream fails on the target's update/delete commits.
12. `pipelines.cdc.tombstoneGCThresholdInSeconds` (default 2 days): delete tombstones are kept only that long for out-of-order handling. A delete arriving later than that window can be mis-ordered, so set the threshold above the maximum expected lateness.
13. SQL for A:
    ```sql
    CREATE OR REFRESH STREAMING TABLE dim_customer_hist;
    CREATE FLOW dim_customer_hist_cdc AS AUTO CDC INTO dim_customer_hist
    FROM STREAM(raw.crm.customers_cdc)
    KEYS (tenant_id, customer_id)
    APPLY AS DELETE WHEN op = 'd'
    SEQUENCE BY STRUCT(commit_ts, lsn)
    COLUMNS * EXCEPT (op, commit_ts, lsn)
    STORED AS SCD TYPE 2
    TRACK HISTORY ON * EXCEPT (last_login);
    ```

</details>
