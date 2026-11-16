# M6 — Exercises (SECOND PASS, guided)

Fill in the numbered blanks `__(n)__` or find the bugs. The hints are directly under each code block. Try first, then open the solution. Section references (§) point to `01-qa.md`.

---

## Exercise 1 — SQL/PySpark transform operators (warm-up)

**Scenario.** A retail team has a long `sales(year, quarter, region, amount)` table and a wide `wide_sales(year, q1, q2, q3, q4)` table. They need:
- quarters as columns, **one row per year**;
- the wide table back to rows, **keeping quarters whose value is NULL**;
- orders whose customer does **not** exist in `customers`;
- the rows in today's snapshot that aren't in yesterday's, **respecting duplicate counts**;
- EU and US orders appended with **all duplicates kept**;
- revenue subtotals per region, per (region, product), and a grand total, but **no per-product-only subtotal**;
- the latest row per customer, in SQL without a subquery;
- in PySpark, EU and US DataFrames combined even though US has an extra column and a different column order.

```sql
-- (a) quarters as columns, one row per year
SELECT * FROM (SELECT year, quarter, amount FROM sales)
__(1)__ (sum(amount) FOR quarter IN (1 AS q1, 2 AS q2, 3 AS q3, 4 AS q4));

-- (b) back to rows, keep NULL quarters
SELECT * FROM wide_sales
UNPIVOT __(2)__ (amount FOR quarter IN (q1, q2, q3, q4));

-- (c) orphan orders (FK violations)
SELECT o.* FROM orders o LEFT __(3)__ JOIN customers c ON o.customer_id = c.customer_id;

-- (d) rows new in today's snapshot, keeping multiplicity
SELECT * FROM snap_today __(4)__ SELECT * FROM snap_yesterday;

-- (e) append EU + US, keep duplicates
SELECT order_id, amount FROM eu_orders __(5)__ SELECT order_id, amount FROM us_orders;

-- (f) subtotals: (region, product), (region), grand total
SELECT region, product, sum(amount) AS rev
FROM sales_lines
GROUP BY __(6)__ (region, product);

-- (g) latest row per customer
SELECT * FROM customer_updates
__(7)__ row_number() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) = 1;
```
```python
# (h) US has an extra column 'state' and columns in a different order
combined = eu_df.__(8)__(us_df, allowMissingColumns=True)
```

**Hints**
- (1) The clause that rotates values into columns. Why does (a) project only three columns in the subquery?
- (2) The default drops NULL values.
- (3) Left rows with no match, left columns only.
- (4) Multiset difference. The plain keyword deduplicates.
- (5) `UNION` alone deduplicates.
- (6) Hierarchical subtotals. Which one would also add per-product subtotals?
- (7) Filters on a window function result, like `HAVING` for aggregates.
- (8) Matches by name, not position.

<details><summary>Solution</summary>

```sql
SELECT * FROM (SELECT year, quarter, amount FROM sales)
PIVOT (sum(amount) FOR quarter IN (1 AS q1, 2 AS q2, 3 AS q3, 4 AS q4));

SELECT * FROM wide_sales
UNPIVOT INCLUDE NULLS (amount FOR quarter IN (q1, q2, q3, q4));

SELECT o.* FROM orders o LEFT ANTI JOIN customers c ON o.customer_id = c.customer_id;

SELECT * FROM snap_today EXCEPT ALL SELECT * FROM snap_yesterday;

SELECT order_id, amount FROM eu_orders UNION ALL SELECT order_id, amount FROM us_orders;

SELECT region, product, sum(amount) AS rev
FROM sales_lines
GROUP BY ROLLUP (region, product);

SELECT * FROM customer_updates
QUALIFY row_number() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) = 1;
```
```python
combined = eu_df.unionByName(us_df, allowMissingColumns=True)
```

1. `PIVOT`: the implicit grouping is every column not in the aggregate or the FOR clause. Projecting away `region` is what makes it one row per **year**. Without it you get one row per (year, region).
2. `INCLUDE NULLS`: SQL UNPIVOT defaults to `EXCLUDE NULLS`. (PySpark `unpivot` keeps nulls by default.)
3. `ANTI`: a `LEFT ANTI JOIN` returns left rows with no match. Safer than `NOT IN`, which returns nothing if `c.customer_id` contains a NULL.
4. `EXCEPT ALL`: each row in yesterday removes exactly one matching row from today. `EXCEPT` (DISTINCT) would hide a row that went from 1 to 2 copies.
5. `UNION ALL`: keeps duplicates and needs no dedup shuffle. Plain `UNION` is `UNION DISTINCT`.
6. `ROLLUP`: gives (region, product), (region), (). `CUBE` would add (product) too. Use `grouping()` to tell subtotal NULLs from data NULLs.
7. `QUALIFY`: filters on a window result without a subquery. `row_number` (not `rank`) guarantees exactly one row per key.
8. `unionByName`: `union()` is positional and would put values in the wrong columns. `allowMissingColumns=True` fills `state` with NULL for EU rows.

</details>

---

## Exercise 2 — Key Vault secret + Event Hubs (Kafka endpoint) streaming ingestion

**Scenario.**
- An IoT team streams telemetry into the event hub `telemetry` in the Standard-tier namespace `evhns-iot`.
- The listen-only connection string is managed by the security team in **Azure Key Vault** `kv-iot`, surfaced in Databricks through a **Key Vault-backed scope** also named `kv-iot`.
- The ingestion service principals (group `ingest-sps`) may only **read** secrets in that scope.
- On its **first** start the stream must pick up **all events still retained** by Event Hubs. It runs as an hourly job that processes what's available, then stops.

```bash
# Security team: the secret value is created in ...__(1)__... (where?), name "eh-listen"
# Databricks admin: grant read-only access to the SP group on the scope
databricks secrets put-acl kv-iot ingest-sps __(2)__
```
```python
from pyspark.sql import functions as F
ns, hub = "evhns-iot", "telemetry"
conn = dbutils.secrets.__(3)__(scope="kv-iot", key="eh-listen")

opts = {
  "kafka.bootstrap.servers": f"{ns}.servicebus.windows.net:__(4)__",
  "__(5)__": hub,
  "kafka.security.protocol": "__(6)__",
  "kafka.sasl.mechanism": "__(7)__",
  "kafka.sasl.jaas.config":
     f'__(8)__.org.apache.kafka.common.security.plain.PlainLoginModule required '
     f'username="__(9)__" password="{conn}";',
  "startingOffsets": "__(10)__",
}
schema = "device_id STRING, temp DOUBLE, ts TIMESTAMP"
raw = spark.readStream.format("kafka").options(**opts).load()
parsed = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("e"),
                     "partition", "offset", F.col("timestamp").alias("enqueued_ts"))
             .select("e.*", "partition", "offset", "enqueued_ts"))
(parsed.writeStream
   .option("checkpointLocation", "/Volumes/iot/raw/chk/telemetry")
   .trigger(__(11)__=True)
   .toTable("iot.bronze.telemetry"))
```

**Hints**
- (1) A Key Vault-backed scope is a read-only interface.
- (2) The lowest of the three secret ACL levels.
- (3) Returns the value as a string, redacted in output.
- (4) The Event Hubs Kafka port.
- (5) The Spark Kafka source option naming the topic. A topic is an event hub. No `kafka.` prefix.
- (6) TLS plus SASL.
- (7) The SASL mechanism used with a SAS connection string.
- (8) Databricks bundles shaded Kafka classes. What is the prefix?
- (9) A literal string, not your user name.
- (10) The default would skip everything already in the hub.
- (11) Process all available data in multiple batches, then stop.

<details><summary>Solution</summary>

```bash
# (1) created in Azure Key Vault kv-iot (portal / az keyvault secret set), NOT via databricks secrets put-secret
az keyvault secret set --vault-name kv-iot --name eh-listen --value "Endpoint=sb://evhns-iot.servicebus.windows.net/;SharedAccessKeyName=listen;SharedAccessKey=..."
databricks secrets put-acl kv-iot ingest-sps READ
```
```python
conn = dbutils.secrets.get(scope="kv-iot", key="eh-listen")
opts = {
  "kafka.bootstrap.servers": f"{ns}.servicebus.windows.net:9093",
  "subscribe": hub,
  "kafka.security.protocol": "SASL_SSL",
  "kafka.sasl.mechanism": "PLAIN",
  "kafka.sasl.jaas.config":
     f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
     f'username="$ConnectionString" password="{conn}";',
  "startingOffsets": "earliest",
}
# ...
   .trigger(availableNow=True)
```

1. **Azure Key Vault**: Key Vault-backed scopes are read-only from Databricks, so `put-secret` fails. The secret shows up in the scope automatically.
2. `READ`: read and list only. `WRITE` adds writes and `MANAGE` adds ACL changes. Granting ACLs needs Premium. Note that READ on a Key Vault-backed scope exposes **every** secret in that vault.
3. `get`: the value is redacted when printed. `getBytes` is for binary, and `list` returns key metadata only.
4. `9093`: the Event Hubs Kafka endpoint port (requires Standard tier or higher).
5. `subscribe`: the event hub name is the topic. `EntityPath` in the connection string isn't needed.
6. `SASL_SSL`.
7. `PLAIN`. (`OAUTHBEARER` is for Entra ID client-credentials auth. With a UC service credential you set no SASL options at all.)
8. `kafkashaded`: unshaded `org.apache.kafka…` fails with `RESTRICTED_STREAMING_OPTION_PERMISSION_ENFORCED`.
9. `$ConnectionString`: the literal username Event Hubs expects with a connection-string password.
10. `earliest`: the default `latest` would skip retained history. It applies **only on first start** (new checkpoint). Later runs resume from the checkpoint offsets.
11. `availableNow`: processes the backlog in rate-limited batches and stops, which suits an hourly job.

</details>

---

## Exercise 3 — Data types, constraints, dedup, nulls, profiling (no pipelines)

**Scenario.** A payments silver table is loaded by a notebook job (no Lakeflow pipeline).
- `payment_id` must never be NULL.
- `amount` must be exact to the cent with up to 16 integer digits.
- `paid_at` is an instant from terminals in many time zones.
- `pos_local_dt` is the wall-clock time printed on the receipt and **must never shift** when the session time zone changes.
- Every write must reject statuses outside `NEW/PAID/REFUNDED`.
- Bronze has several versions per `payment_id`: keep the **newest** deterministically.
- Missing `status` becomes `NEW`.
- The team wants quartiles of `amount` in a notebook, and continuous daily drift monitoring of the table.

```sql
CREATE TABLE main.silver.payments (
  payment_id   BIGINT __(1)__,
  amount       __(2)__,
  paid_at      __(3)__,
  pos_local_dt __(4)__,
  status       STRING,
  CONSTRAINT pk_payments PRIMARY KEY (payment_id)      -- (5) does this reject duplicate payment_ids?
);

ALTER TABLE main.silver.payments
  ADD CONSTRAINT valid_status __(6)__ (status IN ('NEW', 'PAID', 'REFUNDED'));
-- (7) On the existing production copy this ALTER fails. Why, and what must happen first?
```
```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("payment_id").orderBy(F.col("updated_at").desc())
latest = (spark.read.table("main.bronze.payments")
            .withColumn("rn", F.__(8)__().over(w))
            .filter("rn = 1").drop("rn"))
clean = latest.na.__(9)__({"status": "NEW"})

clean.__(10)__("count", "min", "25%", "50%", "75%", "max").show()        # quick quartiles
clean.select(F.__(11)__("payment_id").alias("approx_ids")).show()       # cheap cardinality check
# (12) Continuous daily drift/null-rate monitoring on main.silver.payments: which feature and profile type?
```

**Hints**
- (1) Enforced nullability.
- (2) Exact type: precision = integer digits + scale.
- (3) Stored as UTC, displayed in the session time zone.
- (4) No zone conversion ever. What does it do to the table protocol?
- (5) Which constraint kinds are enforced on Delta?
- (6) The enforced constraint keyword for a boolean expression.
- (7) What does `ADD CONSTRAINT` check before succeeding?
- (8) Exactly one row per key, ties broken arbitrarily but only one survives.
- (9) The `na` method that replaces NULLs per column.
- (10) Like `describe()` plus percentiles.
- (11) A HyperLogLog-based distinct count.
- (12) Formerly Lakehouse Monitoring. Pick the profile for a table with a timestamp column.

<details><summary>Solution</summary>

```sql
CREATE TABLE main.silver.payments (
  payment_id   BIGINT NOT NULL,
  amount       DECIMAL(18,2),
  paid_at      TIMESTAMP,
  pos_local_dt TIMESTAMP_NTZ,
  status       STRING,
  CONSTRAINT pk_payments PRIMARY KEY (payment_id)
);
-- fix or quarantine violators first, e.g.:
-- CREATE TABLE ops.payments_bad_status AS SELECT * FROM main.silver.payments WHERE status NOT IN ('NEW','PAID','REFUNDED') OR status IS NULL;
-- UPDATE/DELETE them, then:
ALTER TABLE main.silver.payments
  ADD CONSTRAINT valid_status CHECK (status IN ('NEW', 'PAID', 'REFUNDED'));
```
```python
latest = (spark.read.table("main.bronze.payments")
            .withColumn("rn", F.row_number().over(w))
            .filter("rn = 1").drop("rn"))
clean = latest.na.fill({"status": "NEW"})
clean.summary("count", "min", "25%", "50%", "75%", "max").show()
clean.select(F.approx_count_distinct("payment_id").alias("approx_ids")).show()
```

1. `NOT NULL`: enforced. It's also required for a PRIMARY KEY column.
2. `DECIMAL(18,2)`: 16 integer digits + 2 decimals, exact. `DOUBLE` drifts, and bare `DECIMAL` is `DECIMAL(10,0)`, which drops cents.
3. `TIMESTAMP`: an instant (local-time-zone semantics). The display follows `spark.sql.session.timeZone`.
4. `TIMESTAMP_NTZ`: wall clock, never shifted. It adds the `timestampNtz` table feature (protocol upgrade), so older readers can't read the table.
5. **No.** PK/FK constraints are informational only. Enforce uniqueness in the load (dedup + MERGE) and with test queries.
6. `CHECK`: enforced on every write. A violating write fails the whole transaction.
7. `ADD CONSTRAINT` **verifies all existing rows** and fails if any violates the expression. Fix or quarantine the violators first, then add the constraint. From then on, every write is checked. In this load, NULL statuses are also filled with `NEW` before the write.
8. `row_number`: deterministic "keep newest" with exactly one row per key. `rank` keeps ties, and `dropDuplicates` after `orderBy` keeps an arbitrary row.
9. `fill` (`fillna`): type-matched replacement per column.
10. `summary`: count/mean/stddev/min/max plus approximate percentiles. `describe()` has no percentiles.
11. `approx_count_distinct`: far cheaper than `countDistinct` on large data. Default relative standard deviation is 0.05.
12. **Data quality monitoring → data profiling** with a **Time series** profile on `paid_at` (Snapshot recomputes the whole table and is limited to 4 TB). It writes `payments_profile_metrics` and `payments_drift_metrics`. Alert on the drift table.

Note for (7): to reject NULL statuses explicitly, add `ALTER TABLE … ALTER COLUMN status SET NOT NULL` rather than relying on the CHECK's NULL handling.

</details>

---

## Exercise 4 — Production review: find the 12 bugs (compute, secrets, error handling, ADF, Azure Monitor)

**Scenario.**
- An ADF pipeline calls a Databricks orchestrator notebook (Notebook activity) on an interactive cluster, then branches on the returned row count.
- The orchestrator runs a child load notebook with retries and must make the **ADF activity and the Databricks run fail** when the load ultimately fails.
- The load must be **safe to retry** (no duplicates) and keep the newest version per `order_id`.
- The cluster is used by a team that writes some notebooks in **R** and must terminate quickly when idle.
- Cluster VMs come from pools, and an Event Hubs connection string is passed via Spark config.
- Ops wants an Azure Monitor alert whenever a job run fails.

The review found **12 bugs**. Find them all.

```jsonc
// (A) Cluster spec (Clusters API excerpt)
{
  "cluster_name": "ops-shared",
  "spark_version": "15.4.x-scala2.12",
  "data_security_mode": "USER_ISOLATION",                 // standard access mode
  "autotermination_minutes": 5,
  "instance_pool_id": "pool-d8-all-spot",
  "driver_instance_pool_id": "pool-d8-all-spot",
  "spark_conf": {
    "spark.eh.conn": "{{secret/kv-iot/eh-listen}}"
  }
}
```
```python
# (B) orchestrator notebook
import json, time
run_date = dbutils.widgets.get("run_date")

def run_with_retry(path, timeout_s, args, retries=2):
    for attempt in range(retries + 1):
        try:
            return dbutils.notebook.run(path, timeout_s, args)
        except Exception:
            if attempt == retries:
                raise
            time.sleep(60)

try:
    out = run_with_retry("./load_orders", 3600, {"run_date": run_date, "batch_size": 5000})
    stats = json.loads(out)
    dbutils.notebook.exit(json.dumps(stats))
except Exception as e:
    spark.sql(f"INSERT INTO ops.etl_errors VALUES (current_timestamp(), 'load_orders', '{str(e)[:500]}')")
    dbutils.notebook.exit(json.dumps({"status": "FAILED"}))
```
```python
# (C) child notebook ./load_orders
from pyspark.sql import functions as F
run_date = dbutils.widgets.get("run_date")
src = spark.read.table("main.bronze.orders").filter(F.col("ingest_date") == run_date)
latest = src.orderBy(F.col("updated_at").desc()).dropDuplicates(["order_id"])
latest.write.mode("append").saveAsTable("main.silver.orders")
dbutils.notebook.exit(latest.count())
```
```text
(D) ADF: If Condition activity expression after the Notebook activity "RunOrders"
@greater(int(activity('RunOrders').output.result.rows), 0)

(E) ADF linked service: authentication = System-assigned managed identity.
    The data factory's MI was granted "Reader" on the Databricks workspace resource.
```
```kusto
// (F) Azure Monitor log search alert query (Log Analytics workspace)
DatabricksJobs
| where ActionName == "runFailure"
| extend jobId = RequestParams.jobId
| summarize failures = count() by jobId
```

**Hints**
- (A) Four bugs: the secret reference syntax, the termination bounds, R support per access mode, and what an "All Spot" pool does to the driver.
- (B) Two bugs: the type of every `arguments` value, and what `exit` does to the run's status.
- (C) Three bugs: determinism of the dedup, idempotency under retries, and the type `exit` accepts.
- (D) One bug: the property ADF exposes for the notebook's exit value.
- (E) One bug: the role the MI needs.
- (F) One bug with two parts: the action name, and reading a JSON **string** column.

<details><summary>Solution</summary>

```jsonc
// (A)
{
  "cluster_name": "ops-shared",
  "spark_version": "15.4.x-scala2.12",
  "data_security_mode": "SINGLE_USER",                      // dedicated, needed for R; assign it to the team's group (UI: Advanced > Dedicated > group)
  "autotermination_minutes": 10,                            // valid range 10–10,000 (0 = off)
  "instance_pool_id": "pool-d8-all-spot",                   // workers may be spot
  "driver_instance_pool_id": "pool-d8-ondemand",            // driver from an on-demand pool
  "spark_conf": {
    "spark.eh.conn": "{{secrets/kv-iot/eh-listen}}"
  }
}
```
```python
# (B)
try:
    out = run_with_retry("./load_orders", 3600, {"run_date": run_date, "batch_size": "5000"})
    stats = json.loads(out)
except Exception as e:
    spark.sql("INSERT INTO ops.etl_errors VALUES (current_timestamp(), 'load_orders', :msg)",
              args={"msg": str(e)[:500]})
    raise                                                     # fail the run => ADF activity fails
dbutils.notebook.exit(json.dumps(stats))                      # success path only, outside the try
```
```python
# (C)
from pyspark.sql.window import Window
import json
w = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())
latest = src.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
latest.createOrReplaceTempView("latest")
spark.sql("""
  MERGE INTO main.silver.orders t USING latest s ON t.order_id = s.order_id
  WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *""")
dbutils.notebook.exit(json.dumps({"rows": latest.count()}))
```
```text
(D) @greater(int(activity('RunOrders').output.runOutput.rows), 0)
(E) Grant the data factory's managed identity the Contributor role on the Databricks workspace (Access control (IAM)).
```
```kusto
// (F)
DatabricksJobs
| where ActionName == "runFailed"
| extend jobId = tostring(parse_json(RequestParams).jobId)
| summarize failures = count() by jobId
```

1. (A) `{{secret/…}}` → **`{{secrets/kv-iot/eh-listen}}`**. The owner also needs READ on the scope, and a rotated value needs a cluster restart.
2. (A) `autotermination_minutes: 5` is invalid. The range is **10–10,000** (`0` disables it).
3. (A) **Standard access mode (`USER_ISOLATION`) doesn't support R.** Use dedicated (`SINGLE_USER`), assigned to the team's group (DBR 15.4+), so the team can still share it.
4. (A) The driver comes from an **All Spot** pool: all nodes, driver included, are spot and evictions aren't replaced. Use an **on-demand pool for the driver**.
5. (B) `dbutils.notebook.run` arguments must be **strings** (`"5000"`), because they set widgets.
6. (B) The `except` branch calls `dbutils.notebook.exit(...)`, which **ends the run as Succeeded**, so ADF never sees a failure. **Re-raise** after logging. The fix also moves the success `exit` after the `try/except`, so only the success path returns a value. Bonus fix: use a parameterized `spark.sql(..., args=...)` instead of f-string SQL with error text, which breaks on quotes.
7. (C) `orderBy(...).dropDuplicates([...])` **doesn't guarantee** keeping the newest row. Use `row_number()` over a window.
8. (C) `mode("append")` **isn't idempotent**: the orchestrator's retry (or an ADF retry) re-appends the same rows. Use **MERGE** (or `replaceWhere` on `ingest_date`).
9. (C) `dbutils.notebook.exit(latest.count())` passes an int. `exit` takes a **string**, so return `json.dumps({"rows": n})`, which the orchestrator parses and ADF reads.
10. (D) ADF exposes the exit value as **`output.runOutput`** (≤ 2 MB). Since the value is JSON, `runOutput.rows` works.
11. (E) An ADF managed identity needs the **Contributor** role on the Azure Databricks workspace resource. Reader yields 403.
12. (F) The action is **`runFailed`** (covers failed **and canceled** runs), not `runFailure`. And `RequestParams` is a **string**: use `parse_json(RequestParams).jobId` with `tostring()` before summarizing. Then create a **log search alert rule** (threshold `> 0`, frequency/lookback e.g. 5 min) with an **action group**. This requires diagnostic settings sending the `jobs` category to the workspace (**Premium**).

Not bugs, but review notes:
- Running ADF Notebook activities on an **existing interactive cluster** is discouraged (all-purpose DBUs, shared state). Prefer a job cluster, or the **Databricks Job activity** on serverless.
- The `ops.etl_errors` insert should use a parameterized query, as shown.

</details>
