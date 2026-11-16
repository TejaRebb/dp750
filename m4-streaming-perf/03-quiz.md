# Module 4 Quiz (final check)

15 scenario questions in DP-750 style. Some say **choose two**. Answer every question before opening any answer.
The section references (§n) point to `01-qa.md`.

---

### Q1
A streaming query computes revenue per store in 1-hour tumbling windows and writes to a Unity Catalog managed table. A downstream service sends one invoice per row written, so each window must be written once, with its final value. Late events of up to 20 minutes must be counted.

Which configuration meets the requirement?

- A. `outputMode("update")` with `withWatermark("ts", "20 minutes")`
- B. `outputMode("append")` with `withWatermark("ts", "20 minutes")`
- C. `outputMode("complete")` with `withWatermark("ts", "20 minutes")`
- D. `outputMode("append")` with no watermark, `trigger(availableNow=True)`

<details><summary>Answer</summary>

**B.** Append emits each window once, after the watermark passes the window end. The watermark guarantees that events up to 20 minutes late are included.
- A: The Delta sink (every UC managed table) doesn't support update mode. Even on another sink, update would emit the same window several times.
- C: Complete rewrites the entire result every batch and never drops state, so the watermark is ignored for cleanup.
- D: An aggregation in append mode without a watermark fails at analysis. The trigger doesn't change that.
</details>

---

### Q2 (choose two)
You implement a **left outer** stream-stream join between `impressions` and `clicks`, keeping impressions that never got a click. Which **two** are required for the join to run and for state to be cleaned up?

- A. A watermark on both input streams
- B. `outputMode("update")` on the writer
- C. A join condition that includes an event-time range between the two timestamp columns
- D. The RocksDB state store provider
- E. `spark.sql.streaming.multipleWatermarkPolicy = max`

<details><summary>Answer</summary>

**A and C.** Outer stream-stream joins require watermarks and an event-time range condition. Databricks recommends watermarks on both sides. These two things together tell Spark when an unmatched row can be emitted with NULLs and when state can be dropped.
- B: Stream-stream joins support only append mode.
- D: RocksDB is recommended for large state, but it isn't required for the join to work. It is also already the default in DBR 17.3 LTS+.
- E: `max` lowers latency but drops data from the slower stream. It is optional and risky, not required.
</details>

---

### Q3
An hourly Lakeflow Job reads a Delta table as a stream. After a 3-day outage, the next run must process the backlog without one giant micro-batch, respect `maxFilesPerTrigger = 200`, and then terminate so the job cluster shuts down. What should the trigger be?

- A. No trigger (default)
- B. `trigger(processingTime="1 hour")`
- C. `trigger(once=True)`
- D. `trigger(availableNow=True)`

<details><summary>Answer</summary>

**D.** AvailableNow processes everything available at start in multiple batches that honour rate limits, then stops.
- A: The default trigger runs continuously and never terminates, so the cluster keeps running.
- B: A processingTime trigger also never terminates. It only spaces out the micro-batches.
- C: `once` is deprecated (DBR 11.3+) and processes the backlog as a single batch, which is exactly the giant batch to avoid.
</details>

---

### Q4
A silver stream reads `bronze_events` (Delta, partitioned by `event_date`). Legal now runs `DELETE FROM bronze_events WHERE user_id = ...` weekly. Since then the stream fails on every run. Silver must keep processing new appends. Deletes are propagated to silver by a separate batch job. DBR 15.4 LTS. What do you set?

- A. `.option("ignoreDeletes", "true")`
- B. `.option("ignoreChanges", "true")`
- C. `.option("skipChangeCommits", "true")`
- D. `.option("readChangeFeed", "true")`

<details><summary>Answer</summary>

**C.** `skipChangeCommits` (DBR 12.2 LTS+) ignores commits that delete or rewrite existing rows and processes appends only. That matches "deletes are handled separately".
- A: `ignoreDeletes` only tolerates deletes at partition boundaries, i.e. dropping whole partitions. A `WHERE user_id` delete rewrites files inside partitions, so the stream still fails.
- B: `ignoreChanges` is legacy (≤ 11.3, replaced by skipChangeCommits). It re-emits rewritten files, which creates duplicates downstream.
- D: Change data feed would propagate the deletes. That is valid when silver must reflect them, but here the requirement says another job handles that, and it needs CDF enabled plus change-handling logic.
</details>

---

### Q5
Two independent streaming queries read `bronze_orders`. One writes `silver_orders_eu`, the other `silver_orders_us`. Both use `checkpointLocation = /Volumes/main/s/chk/orders`. Users report missing and duplicated rows after restarts. What is the fix?

- A. Give each query its own checkpoint directory and restart both with the new directories
- B. Keep the shared checkpoint and add `txnAppId`/`txnVersion` options to both writers
- C. Keep the shared checkpoint and switch both queries to `trigger(availableNow=True)`
- D. Keep the shared checkpoint and enable RocksDB changelog checkpointing

<details><summary>Answer</summary>

**A.** A checkpoint holds one query's identity, offsets, commits and state. Sharing it corrupts recovery, which causes the duplicates and skipped data. Each query needs its own checkpoint.
- B: `txnAppId`/`txnVersion` make DataFrameWriter writes idempotent inside `foreachBatch`. They don't repair conflicting offset and commit logs.
- C: The trigger type doesn't change checkpoint ownership. The corruption remains.
- D: Changelog checkpointing is a state store optimization, and these queries are stateless. It doesn't fix a shared checkpoint.
</details>

---

### Q6
A stateful streaming aggregation was first started with the default `spark.sql.shuffle.partitions = 200`. The cluster was later shrunk to 16 cores. You set `spark.sql.shuffle.partitions = 32` and restart with the same checkpoint on DBR 16.4 LTS. The Spark UI still shows 200 tasks per stateful stage. Why?

- A. AQE overrides the setting for streaming queries
- B. The setting only applies to SQL warehouses
- C. The setting must be set in the cluster Spark config, not with `spark.conf.set`
- D. The shuffle partition count of a stateful query is fixed in the checkpoint when it is created. Applying a new value requires a new checkpoint

<details><summary>Answer</summary>

**D.** The value is recorded in the checkpoint and reused on restart. To change it, start with a new checkpoint. DBR 18 LTS+ adds on-demand state repartitioning, which doesn't apply on 16.4.
- A: AQE doesn't apply to streaming queries at all.
- B: The setting applies to Spark compute generally.
- C: Session-level `spark.conf.set` works for new queries. Where you set it isn't the issue.
</details>

---

### Q7
A streaming fact table is joined to `dim_product` (Delta) using `spark.read.table("dim_product")`. At 10:00 a batch job updates 500 product categories in `dim_product`. The stream runs with a 1-minute trigger. Which statement is true?

- A. The stream keeps using the dimension snapshot taken when the query started, until the query is restarted
- B. Micro-batches processed after the 10:00 commit join against the updated categories. Fact rows processed before 10:00 keep their old categories
- C. All previously written fact rows are automatically recomputed with the new categories
- D. The query fails because the static side changed. `skipChangeCommits` is required on the static side

<details><summary>Answer</summary>

**B.** A stream-static join is stateless. Each micro-batch joins against the latest valid version of the static Delta table at processing time. Rows already written are not revisited.
- A: That is the common misconception. For Delta, the static side isn't frozen at query start.
- C: Stream-static joins never recompute history. Use a materialized view for always-correct joins.
- D: Changes to the static side don't fail the query. `skipChangeCommits` is a streaming-source option and has no meaning on the static side.
</details>

---

### Q8
IoT devices resend events when they get no acknowledgement. A resend has the same `event_id` but a **new** `device_ts`, up to 30 minutes later than the original. You must drop the duplicates in a stream while keeping state bounded. DBR 15.4 LTS. Which approach works?

- A. `.dropDuplicates(["event_id"])` with no watermark
- B. `.withWatermark("device_ts", "1 hour").dropDuplicates(["event_id", "device_ts"])`
- C. `.withWatermark("device_ts", "1 hour").dropDuplicatesWithinWatermark(["event_id"])`
- D. `.distinct()` with no watermark

<details><summary>Answer</summary>

**C.** `dropDuplicatesWithinWatermark` (DBR 13.3+) dedups on the key even when other fields such as the timestamp differ. A 1-hour delay is larger than the 30-minute max gap, so all duplicates are guaranteed to be removed, and state is bounded by the watermark.
- A: This dedups correctly but keeps every `event_id` in state forever.
- B: State is bounded, but because `device_ts` is part of the key, a resend with a new timestamp counts as a different row and isn't dropped.
- D: `distinct` compares whole rows, so resends with different timestamps survive. Without a watermark, state is also unbounded.
</details>

---

### Q9
A nightly job on classic job compute spends most of its time in a stage that applies a Python UDF to each row and then writes to Delta. After you enable Photon, total runtime drops only about 5%, and the DBU rate went up. What is the most likely explanation, and what fixes it?

- A. Photon doesn't support UDFs, so that stage falls back to the Spark engine. Rewrite the UDF logic with built-in functions
- B. Photon must also be enabled with `spark.databricks.photon.enabled` in the notebook
- C. Photon only accelerates SQL warehouses, so the job must be moved to a SQL warehouse
- D. Photon doesn't support Delta writes. Write Parquet instead

<details><summary>Answer</summary>

**A.** Photon doesn't support UDFs, RDD APIs or typed Dataset APIs. Those parts fall back transparently to Spark. Only the native operators (and the Delta write) benefit, which explains the 5%. Built-in functions let Photon run the whole stage.
- B: On classic compute Photon is enabled by the compute setting (the UI checkbox, or `runtime_engine: PHOTON` through the API), not a notebook config. It is already on here.
- C: Photon runs on classic all-purpose and job compute, serverless, and pipelines too.
- D: Photon accelerates Delta writes, including MERGE, UPDATE and DELETE.
</details>

---

### Q10
A batch query does `big_orders LEFT OUTER JOIN big_customers ON customer_id`. Both sides are large, so the join is a sort-merge join. One `customer_id` holds 40% of the **customers** side (the right side). AQE is enabled with default settings, yet one task runs 30× longer than the others. What explains this, and what is the best fix?

- A. AQE is disabled for LEFT OUTER joins. Enable `spark.sql.adaptive.enabled`
- B. The skewed partition is under 256MB. Increase `spark.sql.shuffle.partitions` to 2000
- C. Broadcast `big_customers` with a hint so AQE can apply skew handling
- D. AQE skew handling can't split the right side of a LEFT OUTER join. Salt the hot key (or restructure the join)

<details><summary>Answer</summary>

**D.** For LEFT OUTER joins, AQE can only split skew on the left side. Skew on the right side isn't handled. Salting spreads the hot key across many partitions.
- A: AQE is on by default and applies to the query. The limit is about which join side it can split.
- B: More shuffle partitions don't help when one key dominates, because the whole key still hashes to a single partition. Nothing in the scenario says the partition is small, and 40% of a large table is far above 256MB.
- C: `big_customers` is large, so broadcasting it risks OOM. AQE also never applies skew handling to broadcast joins.
</details>

---

### Q11 (choose two)
In the Spark UI, the longest stage of a join has 400 tasks. Summary Metrics: median duration 35 s, 75th percentile 40 s, max 18 min. No spill is reported. Which **two** actions address the root cause?

- A. Add more worker nodes
- B. Salt the hot join key(s) to spread them across partitions
- C. Call `.cache()` on both join inputs
- D. Check that AQE skew join handling applies (SMJ/SHJ, the join side allowed for this join type) and lower `skewedPartitionFactor`/`skewedPartitionThresholdInBytes` if the skewed partition sits under the thresholds
- E. Switch the cluster to all-purpose compute

<details><summary>Answer</summary>

**B and D.** The max task is far above the 75th percentile (more than 50% above), which is the documented signal for skew. The fixes are AQE skew handling (checking applicability and tuning the thresholds) and salting.
- A: One straggler task bounds the stage. More cores don't speed up a single task.
- C: The Spark cache doesn't change how keys are distributed.
- E: The compute type has nothing to do with skew, and all-purpose compute costs more for jobs.
</details>

---

### Q12
Analysts run many ad-hoc notebook queries all day against the same few Delta tables on a classic all-purpose cluster. Reads from ADLS dominate the run time, and the tables change several times a day. What should you recommend?

- A. Add `df.cache()` to every notebook after reading the tables
- B. Run `CACHE SELECT * FROM each_table` every morning on DBR 16.4
- C. Use a worker instance type with local SSDs, so the disk cache (enabled by default on those types) serves repeated reads and invalidates automatically when files change
- D. Set `spark.sql.files.ignoreMissingFiles = true` to skip cloud listing

<details><summary>Answer</summary>

**C.** The disk cache (formerly the Delta cache) caches Parquet/Delta files on local SSD automatically. It is enabled by default on SSD/cache-accelerated types and stays consistent when tables change. Databricks recommends it over the Spark cache.
- A: The Spark cache uses executor memory, goes stale when tables change until you unpersist, and requires code changes everywhere. Databricks says to avoid it for this.
- B: `CACHE SELECT` is ignored in DBR 14.2+ and on SQL warehouses.
- D: This setting silently produces incorrect results and has nothing to do with caching.
</details>

---

### Q13
A team wants to move a 24/7 Structured Streaming job (Kafka → Delta, `trigger(processingTime="10 seconds")`, end-to-end SLA under 1 minute) from classic job compute to serverless jobs to reduce operations work. The first serverless run fails with `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`. Which option keeps an always-on, low-latency stream on serverless?

- A. Rebuild it as a serverless Lakeflow pipeline running in continuous pipeline mode
- B. Change the trigger to `trigger(continuous="1 second")`
- C. Remove the trigger so Spark uses its default
- D. Keep `processingTime` and enable autoscaling on the serverless job

<details><summary>Answer</summary>

**A.** Serverless notebooks and jobs support only `availableNow` (and the deprecated `once`). For an always-on stream on serverless, Databricks points to serverless Lakeflow pipelines in continuous mode. Another option is availableNow in a continuous job, but restarting per run makes a strict sub-minute SLA harder.
- B: Continuous processing isn't supported on Azure Databricks, and time-based triggers aren't supported on serverless.
- C: The default trigger is `processingTime("0 seconds")`, which raises the same error on serverless.
- D: Serverless manages its own scaling, and autoscaling doesn't change trigger support.
</details>

---

### Q14
A `foreachBatch` function appends each micro-batch to two Delta tables, `A` and `B`, using `.option("txnAppId", "orders_fanout").option("txnVersion", batch_id)`. After an incompatible change, the team deletes the checkpoint and restarts with a new one. The stream reports progress, but **no new rows** show up in A or B for the first few thousand batches. What went wrong?

- A. `txnVersion` must be set to `epochId`, not `batch_id`
- B. The new checkpoint restarted `batch_id` at 0. Delta saw the same `txnAppId` with versions it had already committed and skipped those writes. A new `txnAppId` was needed
- C. `foreachBatch` can't write to more than one table
- D. Delta tables written from `foreachBatch` need `outputMode("complete")`

<details><summary>Answer</summary>

**B.** Delta uses `(txnAppId, txnVersion)` as a unique key and skips versions it has already seen. A new checkpoint resets batch IDs, so you must provide a different `txnAppId`.
- A: Binding `txnVersion` to the `batch_id` passed to `foreachBatch` is exactly the documented pattern.
- C: Writing to several tables is allowed, though serialized. Databricks recommends separate streams for better parallelism.
- D: The output mode is unrelated, and complete mode only works with aggregations.
</details>

---

### Q15
A batch ETL joins a 3 TB fact table to a 25 MB dimension with an inner join. `df.explain()` shows the **initial** plan as `SortMergeJoin`, so both sides are shuffled. Statistics are current. You want the small side broadcast from the start, without changing global configuration for other queries. What do you do?

- A. Disable AQE so the planner re-evaluates the broadcast threshold
- B. Increase `spark.sql.shuffle.partitions` so each shuffle partition is under 10 MB
- C. Run `SQL EXPLAIN` to force AQE to re-plan the query before it runs
- D. Wrap the dimension in `broadcast(dim)` (or `/*+ BROADCAST(dim) */`) in this query

<details><summary>Answer</summary>

**D.** 25 MB is above the static `spark.sql.autoBroadcastJoinThreshold` (10 MB), so the planner chose a sort-merge join. A broadcast hint forces a broadcast hash join at plan time, which avoids shuffling 3 TB. AQE could switch at runtime (its threshold is 30 MB), but only after the shuffle exchanges have run, which is why Databricks still recommends the hint when you know the sizes.
- A: Disabling AQE removes runtime optimizations and doesn't change the static threshold.
- B: Shuffle partitioning has no effect on the broadcast decision, which depends on the size of the relation.
- C: `SQL EXPLAIN` doesn't run the query. It only shows the initial plan, and AQE doesn't run.
</details>

---

## Scoring

**13+/15: move on. 10–12: reread the sections for the questions you missed. <10: reread `01-qa.md` §2 (output modes), §4 (triggers), §5–§7 (watermarks, windows, state), §8–§11 (joins, foreachBatch, Delta source options), §13–§16 (AQE, shuffle, broadcast, skew), §18–§20 (caching, Photon, compute), then redo Exercises 3–4.**

| Q | Section |
|---|---|
| 1 | §2, §5 |
| 2 | §9 |
| 3 | §4, §11 |
| 4 | §11 |
| 5 | §3 |
| 6 | §3, §14 |
| 7 | §8 |
| 8 | §7 |
| 9 | §19 |
| 10 | §13, §16 |
| 11 | §16, §21 |
| 12 | §18 |
| 13 | §4, §20 |
| 14 | §10 |
| 15 | §13, §15 |
