# Module 4 Exercises (second pass, with guidance)

Four exercises that get harder as you go. Fill each `__(n)__` blank, or answer the numbered question. Hints come right after each code block. Try every blank before you open the solution.
Unity Catalog names are `catalog.schema.table`. Checkpoints live in UC volumes (`/Volumes/...`).

---

## Exercise 1 (warm-up): incremental bronze → silver, hourly, cheap

**Scenario.** A Lakeflow Job runs every hour on job compute. It must move new rows from `main.sales.bronze_orders` (Delta) to `main.sales.silver_orders`, and the cluster must stop when it's done.
- Compliance runs `DELETE FROM bronze_orders WHERE customer_id = ...` several times a week. The table is partitioned by `order_date`, so these are **not** partition drops. The silver stream must not fail because of these deletes. Propagating the deletes is handled by a separate process.
- After an outage, the first run will find about 40,000 new files. The run must process them in chunks of at most 500 files per micro-batch.

```python
silver_q = (spark.readStream
    .option("__(1)__", "true")
    .option("__(2)__", 500)
    .table("main.sales.bronze_orders")
    .filter("order_status IS NOT NULL")
    .writeStream
    .option("__(3)__", "/Volumes/main/sales/chk/silver_orders")
    .trigger(__(4)__)
    .toTable("main.sales.silver_orders"))

silver_q.__(5)__()      # keep the job task alive until the stream finishes
```

**Hints**
- (1) This is the DBR 12.2+ option that ignores commits which rewrite existing files. The legacy option that only handles partition drops is the wrong choice here.
- (2) This is the file-count rate limit for a Delta source. Its default is 1000.
- (3) Every streaming writer needs its own one of these.
- (4) Pick the trigger that processes the backlog in several rate-limited batches and then stops. The older single-batch option is deprecated.
- (5) This is a blocking method on the `StreamingQuery` handle.

<details><summary>Solution</summary>

```python
silver_q = (spark.readStream
    .option("skipChangeCommits", "true")
    .option("maxFilesPerTrigger", 500)
    .table("main.sales.bronze_orders")
    .filter("order_status IS NOT NULL")
    .writeStream
    .option("checkpointLocation", "/Volumes/main/sales/chk/silver_orders")
    .trigger(availableNow=True)
    .toTable("main.sales.silver_orders"))

silver_q.awaitTermination()
```

1. `skipChangeCommits`: ignores UPDATE/DELETE/MERGE commits on the source. `ignoreDeletes` only covers whole-partition drops, and `ignoreChanges` is legacy and re-emits duplicates.
2. `maxFilesPerTrigger`: caps each micro-batch at 500 files (default 1000). `maxBytesPerTrigger` would be the soft byte cap.
3. `checkpointLocation`: one unique checkpoint per query, stored in a UC volume or external location.
4. `availableNow=True`: processes the 40k files in batches of 500, honouring the rate limit, then stops so the job cluster can shut down. `once=True` is deprecated (DBR 11.3+) and runs as one giant batch.
5. `awaitTermination`: blocks until the availableNow query finishes. Without it the task could end before the stream does.
</details>

---

## Exercise 2 (core): sliding-window gold aggregate with late data

**Scenario.** Build `main.sales.gold_store_rev` with revenue per store over **30-minute windows that advance every 10 minutes**.
- Events can arrive up to **15 minutes late**. Anything later may be dropped.
- Each window must be written **exactly once, with its final value**. A downstream process sends a report per row.
- The sink is a UC managed Delta table.
- Classic job compute. Check for new data once a minute to keep storage API costs down.

```python
from pyspark.sql import functions as F

gold = (spark.readStream.table("main.sales.silver_orders")
    .__(1)__("order_ts", "__(2)__")
    .groupBy(F.__(3)__("order_ts", "30 minutes", "__(4)__"), "store_id")
    .agg(F.sum("amount").alias("revenue"))
    .select("store_id",
            F.col("window.start").alias("win_start"),
            F.col("window.end").alias("win_end"),
            "revenue"))

(gold.writeStream
    .outputMode("__(5)__")
    .option("checkpointLocation", "/Volumes/main/sales/chk/gold_store_rev")
    .trigger(processingTime="__(6)__")
    .toTable("main.sales.gold_store_rev"))
```

**Question (7).** The window `[10:00, 10:30)` for store 42 has received all its events. What has to happen before its row is written to the table? Give the condition in terms of event times.

**Bonus question.** A colleague changes `outputMode` to `"update"` "for fresher numbers". What happens, and what would you do instead if fresher numbers really were required?

**Hints**
- (1)–(2) This declares event-time lateness. It must come *before* the aggregation and use the same column as the window.
- (3)–(4) The same function makes tumbling and sliding windows. The third argument is the slide.
- (5) Each row written once and final means this mode. It requires a watermark when there is an aggregation.
- (6) The interval string format is a number plus a unit.
- (7) Watermark = max event time seen − delay. A window is final when the watermark passes its end.

<details><summary>Solution</summary>

```python
gold = (spark.readStream.table("main.sales.silver_orders")
    .withWatermark("order_ts", "15 minutes")
    .groupBy(F.window("order_ts", "30 minutes", "10 minutes"), "store_id")
    .agg(F.sum("amount").alias("revenue"))
    .select("store_id", F.col("window.start").alias("win_start"),
            F.col("window.end").alias("win_end"), "revenue"))

(gold.writeStream
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/main/sales/chk/gold_store_rev")
    .trigger(processingTime="1 minute")
    .toTable("main.sales.gold_store_rev"))
```

1. `withWatermark`: defines the lateness threshold. It is required for an aggregation in append mode and is what lets old window state be evicted.
2. `"15 minutes"`: the allowed lateness. Records within it are guaranteed to be counted; later records may be dropped.
3. `window`: event-time window function. Use `session_window` only for gap-based sessions.
4. `"10 minutes"`: slide duration, which must be ≤ window duration. Each order falls into 3 overlapping windows.
5. `append`: each window is emitted once, after the watermark passes its end. The Delta sink also doesn't support `update`.
6. `"1 minute"`: a processingTime trigger polls once a minute instead of continuously, which reduces storage API calls.
7. The watermark must move past 10:30. That requires the **max `order_ts` seen to exceed 10:45** (10:45 − 15 min = 10:30). The watermark is computed at the end of a micro-batch, so the row is written in a following trigger, at the 1-minute cadence. The output is therefore at least 15 minutes behind event time.

**Bonus.** The query fails, because the Delta sink supports only append and complete. If fresher partial values were required, use `foreachBatch` with a `MERGE INTO gold_store_rev` keyed on `(store_id, win_start)` and `.outputMode("update")`. The downstream "one report per row" consumer would then receive repeated updates, which contradicts the requirement, so append stays correct here.
</details>

---

## Exercise 3 (stateful): orders ↔ payments join, dedup, and a bug hunt

**Scenario.** DBR **15.4 LTS**, classic job compute with 32 worker cores.
- `main.pay.payments` contains producer **retries**. The same `payment_id` arrives several times with *different* `pay_ts` values, up to 90 minutes apart.
- A payment is valid if it arrives within **1 hour after** its order.
- The output must contain **every order**. Orders with no valid payment get a row with NULL payment columns, used for dunning.
- State is large, and the team saw long GC pauses in a prototype.

### Part A: fill the blanks

```python
from pyspark.sql import functions as F

# Must be set BEFORE the first run of this query
spark.conf.set("spark.sql.streaming.stateStore.providerClass", "__(1)__")
spark.conf.set("spark.sql.streaming.stateStore.rocksdb.__(2)__.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "__(3)__")

orders = (spark.readStream.table("main.pay.orders")
          .withWatermark("order_ts", "1 hour"))

payments = (spark.readStream.table("main.pay.payments")
            .withWatermark("pay_ts", "2 hours")
            .__(4)__(["payment_id"]))

joined = orders.alias("o").join(
    payments.alias("p"),
    F.expr("""o.order_id = p.order_id AND
              p.pay_ts >= o.order_ts AND
              p.pay_ts <= o.order_ts + __(5)__"""),
    "__(6)__")

(joined.writeStream
   .outputMode("__(7)__")
   .option("checkpointLocation", "__(8)__")
   .toTable("main.pay.order_payment_status"))
```

**Hints**
- (1) This is the Databricks RocksDB provider class name. RocksDB only became the default in DBR 17.3 LTS.
- (2) This RocksDB option uploads only the changes at each checkpoint (DBR 13.3+).
- (3) Databricks guidance for stateful streams is 1–2× the total cores.
- (4) Pick the dedup operator for duplicates whose timestamps **differ** (DBR 13.3+). Check that the watermark delay covers the 90-minute gap.
- (5) A SQL interval literal.
- (6) "Every order, NULLs when unmatched": orders are on the left.
- (7) The only mode stream-stream joins support.
- (8) A path, plus the rule that applies to it.

### Part B: find the bugs

A colleague's first version is below. List every problem, with the reason and the fix.

```python
payments = spark.readStream.table("main.pay.payments").dropDuplicates(["payment_id"])  # (B1)
orders   = spark.readStream.table("main.pay.orders")                                    # (B2)

joined = orders.join(payments, "order_id", "left_outer")                               # (B3)

(joined.writeStream
   .outputMode("update")                                                                # (B4)
   .option("checkpointLocation", "/Volumes/main/pay/chk/orders_silver")                 # (B5) path already used by the orders_silver stream
   .toTable("main.pay.order_payment_status"))
```

**Question (C).** An order placed at 09:00 never gets paid. Roughly when can its NULL-padded row appear?

<details><summary>Solution, Part A</summary>

```python
spark.conf.set("spark.sql.streaming.stateStore.providerClass",
               "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")
spark.conf.set("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "64")

orders = spark.readStream.table("main.pay.orders").withWatermark("order_ts", "1 hour")
payments = (spark.readStream.table("main.pay.payments")
            .withWatermark("pay_ts", "2 hours")
            .dropDuplicatesWithinWatermark(["payment_id"]))

joined = orders.alias("o").join(
    payments.alias("p"),
    F.expr("""o.order_id = p.order_id AND
              p.pay_ts >= o.order_ts AND
              p.pay_ts <= o.order_ts + interval 1 hour"""),
    "leftOuter")

(joined.writeStream
   .outputMode("append")
   .option("checkpointLocation", "/Volumes/main/pay/chk/order_payment_status")
   .toTable("main.pay.order_payment_status"))
```

1. `com.databricks.sql.streaming.state.RocksDBStateStoreProvider`: moves state off the JVM heap and fixes the GC pauses. It must be set before the first run, because the provider can't change on an existing checkpoint.
2. `changelogCheckpointing`: writes only the changed state at each checkpoint, which shortens checkpoints. It can be toggled later, but needs DBR 13.3+.
3. `64` (anything from 32 to 64): 1–2× cores. It is frozen in the checkpoint after the first run.
4. `dropDuplicatesWithinWatermark`: dedups on `payment_id` even though the retries' `pay_ts` values differ. The 2-hour watermark is larger than the 90-minute max gap, so all duplicates are guaranteed to be removed.
5. `interval 1 hour`: the time-range condition on the watermarked columns. It tells Spark when no further match can arrive, so state can be dropped.
6. `leftOuter` (also `left_outer`/`left`): keeps every order and NULL-pads the unmatched ones. Outer stream-stream joins require watermarks plus the time-range condition.
7. `append`: stream-stream joins support only append.
8. A **new, unique** path, e.g. `/Volumes/main/pay/chk/order_payment_status`. Never share it with another query.
</details>

<details><summary>Solution, Part B and C</summary>

- **B1** `dropDuplicates(["payment_id"])` with no watermark keeps every `payment_id` forever, so state is unbounded. Adding a watermark on `pay_ts` alone wouldn't clean up either, because `pay_ts` isn't a key, and adding `pay_ts` to the keys breaks dedup since retries have different timestamps. Fix: `withWatermark("pay_ts","2 hours").dropDuplicatesWithinWatermark(["payment_id"])`.
- **B2** The orders side has no watermark. Outer stream-stream joins need watermarks, and Databricks recommends one on **both** sides for state cleanup.
- **B3** An equi-join only, with no event-time range condition. A left outer stream-stream join without watermarks plus a time-range condition fails at analysis, and even an inner join would keep unbounded state. Add `p.pay_ts BETWEEN o.order_ts AND o.order_ts + interval 1 hour`.
- **B4** `update` isn't supported for stream-stream joins, which are append only. The Delta sink also lacks update mode.
- **B5** It reuses another query's checkpoint. That corrupts both queries' offsets, commits and state. Every query needs its own checkpoint.

**C.** Not before Spark can prove that no payment can still match, which means after 10:00 (order_ts + 1 h) has fallen behind the watermark. With two inputs, the **global watermark is the minimum** of the two: max(order_ts) − 1 h and max(pay_ts) − 2 h. The slower payments watermark (2 h) usually dominates. The NULL row therefore appears roughly once payment events later than ~12:00 have been seen, which is hours after the order. This output delay is expected behaviour, not a bug.
</details>

---

## Exercise 4 (capstone): CDC upsert with foreachBatch, monitoring, batch tuning, compute

**Scenario.**
- `main.crm.customers_bronze` has **Change Data Feed** enabled and receives inserts, updates **and deletes**. Silver must reflect all three.
- The silver stream was previously run with checkpoint `.../customers_silver_v1` and `txnAppId = "cust_silver_v1"`. That checkpoint was deleted after an incompatible change, and the stream now restarts with a **new** checkpoint.
- Every micro-batch must also be appended to an audit table, **without duplicates** on retry.
- Ops wants an alert whenever the stream processes slower than data arrives.
- A separate nightly batch report joins a 2 TB fact table to a 20 MB `dim_region` (fact on the left, LEFT OUTER) and currently uses a sort-merge join with heavy spill.
- The streaming job runs 24/7 on classic job compute with a 30-second trigger. The budget wants spot workers but **no** risk to the driver. Photon is required.

```python
from pyspark.sql import functions as F, Window
from pyspark.sql.streaming import StreamingQueryListener

APP_ID = "cust_silver_v2"

# ---------- source: CDF ----------
src = (spark.readStream
        .option("__(5)__", "true")
        .table("main.crm.customers_bronze")
        .filter("_change_type != 'update_preimage'")
        .withColumn("op", F.when(F.col("_change_type") == "delete", "DELETE").otherwise("UPSERT"))
        .drop("_change_type", "_commit_version", "_commit_timestamp"))

# ---------- sink logic ----------
def upsert_customers(micro_df, batch_id):
    if micro_df.__(1)__():
        return
    w = Window.partitionBy("customer_id").orderBy(F.col("change_ts").desc())
    latest = micro_df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
    latest.createOrReplaceTempView("cust_updates")

    micro_df.__(2)__.sql("""
      MERGE INTO main.crm.customers_silver t
      USING cust_updates s ON t.customer_id = s.customer_id
      WHEN MATCHED AND s.op = 'DELETE' THEN DELETE
      WHEN MATCHED AND s.change_ts > t.change_ts THEN UPDATE SET *
      WHEN NOT MATCHED AND s.op <> 'DELETE' THEN INSERT *
    """)

    (micro_df.write.format("delta")
        .option("__(3)__", APP_ID)
        .option("__(4)__", batch_id)
        .mode("append")
        .saveAsTable("main.crm.customers_changes_audit"))

q = (src.writeStream
       .__(6)__(upsert_customers)
       .outputMode("update")
       .queryName("customers_silver")
       .option("checkpointLocation", "/Volumes/main/crm/chk/customers_silver_v2")
       .trigger(processingTime="30 seconds")
       .start())

# ---------- monitoring ----------
class LagAlert(StreamingQueryListener):
    def onQueryStarted(self, e): pass
    def onQueryProgress(self, e):
        p = e.progress
        if p.__(7)__ < p.inputRowsPerSecond:
            enqueue_alert(p.name, p.batchId)        # lightweight; a separate thread ships it
    def onQueryIdle(self, e): pass
    def onQueryTerminated(self, e): pass

spark.streams.__(8)__(LagAlert())

# ---------- nightly batch report (separate job) ----------
spark.conf.set("spark.sql.shuffle.partitions", "__(9)__")
report = fact_sales.join(F.__(10)__(dim_region), "region_id", "left")
```

```json
"new_cluster": {
  "spark_version": "17.3.x-scala2.13",
  "node_type_id": "Standard_D8ds_v5",
  "num_workers": 4,
  "runtime_engine": "__(11)__",
  "azure_attributes": {
    "first_on_demand": 1,
    "availability": "__(12)__",
    "spot_bid_max_price": -1
  }
}
```

**Reasoning questions (answer in one line each)**
- R1. Why `APP_ID = "cust_silver_v2"` and not reuse `"cust_silver_v1"`?
- R2. Why the `row_number()` dedup before the MERGE?
- R3. Could this job move to serverless jobs as written?
- R4. Should autoscaling be enabled on the streaming job cluster?
- R5. Will Photon speed up this streaming job, given that Photon "doesn't support stateful streaming"?

**Hints**
- (1) This DataFrame method returns a bool. CDF/OPTIMIZE can produce empty batches.
- (2) Run SQL with the session that owns the micro-batch DataFrame.
- (3)–(4) The two Delta DataFrameWriter options for idempotent writes. One is a stable string, the other is monotonic.
- (5) The reader option that streams row-level changes instead of table appends.
- (6) The writer method that hands each micro-batch to a function.
- (7) The counterpart of `inputRowsPerSecond` in the progress object.
- (8) Registers a listener on the session's StreamingQueryManager.
- (9) The value that lets Databricks size shuffle partitions from the plan and input.
- (10) A DataFrame-side join hint for the small table.
- (11) What the Clusters/Jobs API needs so Photon is actually on.
- (12) Spot workers that fall back to on-demand when spot capacity is unavailable.

<details><summary>Solution</summary>

```python
src = (spark.readStream
        .option("readChangeFeed", "true")
        ...)

def upsert_customers(micro_df, batch_id):
    if micro_df.isEmpty():
        return
    ...
    micro_df.sparkSession.sql("""MERGE INTO ...""")
    (micro_df.write.format("delta")
        .option("txnAppId", APP_ID)
        .option("txnVersion", batch_id)
        .mode("append")
        .saveAsTable("main.crm.customers_changes_audit"))

q = (src.writeStream.foreachBatch(upsert_customers) ... .start())

class LagAlert(StreamingQueryListener):
    def onQueryProgress(self, e):
        p = e.progress
        if p.processedRowsPerSecond < p.inputRowsPerSecond: ...

spark.streams.addListener(LagAlert())

spark.conf.set("spark.sql.shuffle.partitions", "auto")
report = fact_sales.join(F.broadcast(dim_region), "region_id", "left")
```
```json
"runtime_engine": "PHOTON",
"azure_attributes": { "first_on_demand": 1, "availability": "SPOT_WITH_FALLBACK_AZURE", "spot_bid_max_price": -1 }
```

1. `isEmpty`: foreachBatch can receive empty batches (e.g. an OPTIMIZE on the source with nothing to do). Skip them.
2. `sparkSession`: use the micro-batch DataFrame's own session so the temp view `cust_updates` resolves.
3. `txnAppId`: a stable unique string identifying this writer for Delta's idempotency check.
4. `txnVersion`: bound to `batch_id`. On a replay of the same batch, Delta sees `(APP_ID, batch_id)` again and skips the append.
5. `readChangeFeed`: streams inserts, `update_postimage`s and deletes. Needed because silver must reflect updates and deletes. `skipChangeCommits` would drop them.
6. `foreachBatch`: lets you run MERGE, which the Delta streaming sink can't do directly in update mode.
7. `processedRowsPerSecond`: if it stays below `inputRowsPerSecond`, the stream is falling behind.
8. `addListener`: registers the listener for all queries in this Spark session. Keep callbacks light because they run on the driver listener thread.
9. `auto`: auto-optimized shuffle picks the partition count from plan and data size, which helps with spill. AQE coalescing is on by default anyway.
10. `broadcast`: 20 MB is above the static 10 MB threshold, so the planner chose a sort-merge join. Broadcasting the right side of a LEFT OUTER join is allowed, removes the 2 TB shuffle, and removes the spill. AQE might switch at runtime (under 30 MB), but only after shuffling.
11. `PHOTON`: via the API, Photon is enabled only with `runtime_engine = "PHOTON"`. The UI checkbox defaults on, the API does not.
12. `SPOT_WITH_FALLBACK_AZURE`: spot workers with on-demand fallback. `first_on_demand: 1` keeps the driver on-demand.

**R1.** The new checkpoint restarts `batch_id` at 0. Reusing `cust_silver_v1` would make Delta treat batches 0..N as already committed and **silently skip** the audit writes.
**R2.** MERGE fails if several source rows match one target row. It also keeps the update deterministic (latest `change_ts` wins), so a replayed batch produces the same result.
**R3.** No. Serverless jobs support only `availableNow` (and the deprecated `once`). `processingTime="30 seconds"` fails with `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`. The options are availableNow in a continuous job, or a serverless Lakeflow pipeline in continuous mode.
**R4.** No. Classic autoscaling has limitations scaling down Structured Streaming, and Databricks says not to enable it for streaming jobs. Use a fixed size, or Lakeflow pipelines with enhanced autoscaling.
**R5.** Yes. The query has no stateful operators (the dedup is a batch `row_number` inside foreachBatch), and Photon accelerates the MERGE and Delta writes. It would not help Python UDFs or stateful operators.
</details>
