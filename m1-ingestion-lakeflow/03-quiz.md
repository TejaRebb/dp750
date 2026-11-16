# M1 — Quiz (FINAL CHECK)

15 scenario questions in DP-750 style. Pick one answer unless the question says **(Choose two)**. Commit to an answer before you open it. Section references (§) point to `01-qa.md`.

---

### Q1
An Auto Loader stream ingests JSON from ADLS with schema inference. The source team adds fields without notice. Requirements: the stream must **never stop** because of schema changes, the **target schema must not change**, and the new fields' values must be **kept** for later analysis. Which `cloudFiles.schemaEvolutionMode` should you set?

- A. `addNewColumns`
- B. `rescue`
- C. `none`
- D. `failOnNewColumns`

<details><summary>Answer</summary>

**B.** `rescue` never evolves the schema and never fails. New columns go into `_rescued_data`.
- A: wrong. It evolves the schema and **fails** the stream once with `UnknownFieldException`.
- C: wrong. New columns are **ignored/dropped**. There is no rescue unless `rescuedDataColumn` is set, so the data isn't kept.
- D: wrong. The stream fails and stays failed until you intervene.
(§2)
</details>

---

### Q2 (Choose two)
An Auto Loader stream on CSV files produces a bronze table where every column is `STRING`. You need `order_date` as DATE and the other numeric columns typed automatically. No explicit schema should be maintained. Which **two** actions together meet the requirement?

- A. Set `cloudFiles.inferColumnTypes` to `true`.
- B. Set `cloudFiles.schemaHints` to `"order_date DATE"`.
- C. Add `FORMAT_OPTIONS ('inferSchema' = 'true')`.
- D. Set `cloudFiles.schemaEvolutionMode` to `addNewColumnsWithTypeWidening`.

<details><summary>Answer</summary>

**A and B.** Auto Loader defaults `inferColumnTypes` to `false` (strings for CSV/JSON/XML). Turning it on types the numerics, and the schema hint pins `order_date` to DATE without a full schema.
- C: wrong. `FORMAT_OPTIONS`/`inferSchema` is **COPY INTO** syntax, not an Auto Loader option.
- D: wrong. Type widening (e.g. int→long) only applies to evolution of already-typed columns. It doesn't turn the inferred strings into types.
(§1, §2)
</details>

---

### Q3
A landing container receives ~2 million small files per hour. The Auto Loader stream uses directory listing. Each micro-batch spends most of its time listing, and storage LIST costs are high. The path is in a Unity Catalog external location. What should you do first?

- A. Increase `cloudFiles.maxFilesPerTrigger` to 100000.
- B. Enable file events on the external location and set `cloudFiles.useManagedFileEvents = true`.
- C. Replace Auto Loader with a scheduled `COPY INTO`.
- D. Set `cloudFiles.includeExistingFiles = false`.

<details><summary>Answer</summary>

**B.** File events (the recommended mode) give notification-level discovery with one queue per external location. You avoid repeated LIST calls on a huge directory.
- A: wrong. It's a per-batch rate limit and doesn't fix discovery cost.
- C: wrong. COPY INTO suits thousands of files. Databricks recommends Auto Loader for very large directories and millions of files.
- D: wrong. It's only evaluated on the first start, and it has nothing to do with listing cost.
(§3)
</details>

---

### Q4
A SQL analyst runs a nightly `COPY INTO sales FROM 'abfss://.../daily/' FILEFORMAT = CSV`. The vendor **corrected and re-uploaded** (overwrote) `2026-11-10.csv`, but after the next run the table still holds the old values. What is the minimal fix to load the corrected file?

- A. Add `COPY_OPTIONS ('mergeSchema' = 'true')`.
- B. Run `COPY INTO ... FILES = ('2026-11-10.csv') COPY_OPTIONS ('force' = 'true')` (after deleting the old rows for that day).
- C. Run `REFRESH TABLE sales`.
- D. Add `FORMAT_OPTIONS ('mergeSchema' = 'true')`.

<details><summary>Answer</summary>

**B.** COPY INTO is idempotent and skips files it has already loaded, **even if they were modified**. `force = true` disables idempotency for that run, and `FILES` scopes it to that one file. Remove the old rows first to avoid duplicates.
- A: wrong. That evolves the target schema and doesn't reload files.
- C: wrong. It refreshes cached metadata, not ingestion state.
- D: wrong. It merges schemas across the source files and doesn't affect idempotency.
(§5)
</details>

---

### Q5 (Choose two)
You build a Databricks SQL streaming table that ingests JSON from a UC volume using `read_files`. Which **two** statements are true?

- A. The query must use `FROM STREAM read_files(...)`, or the streaming table definition errors.
- B. You must set `schemaLocation` in the `read_files` options so the inferred schema persists.
- C. By default `read_files` infers typed columns (`inferColumnTypes` = `true`) for JSON.
- D. `SELECT *` includes the `_metadata` column automatically.

<details><summary>Answer</summary>

**A and C.**
- A: correct. In a streaming table, `read_files` uses Auto Loader, and the `STREAM` keyword is required.
- C: correct. The `read_files` default is the opposite of Auto Loader's.
- B: wrong. `schemaLocation` isn't required when `read_files` is used in a streaming table, because the platform manages it.
- D: wrong. `_metadata` is excluded from `SELECT *` and must be selected explicitly.
(§4)
</details>

---

### Q6
The company needs an ongoing, incremental copy of 40 tables from an on-premises SQL Server (CDC enabled) into Unity Catalog, with SCD Type 2 history on some tables. They want as little custom code and maintenance as possible. What should you use?

- A. Lakehouse Federation with a foreign catalog over SQL Server
- B. A Lakeflow Connect managed SQL Server connector (ingestion gateway + ingestion pipeline)
- C. A JDBC `spark.read` in a scheduled notebook followed by `MERGE INTO`
- D. Auto Loader reading the SQL Server transaction log files

<details><summary>Answer</summary>

**B.** The managed database connectors handle CDC reads (via the **ingestion gateway** and staging storage), incremental application, SCD1/SCD2, retries and schema evolution, all UC-governed on serverless.
- A: wrong. Federation queries in place and doesn't produce an incremental Delta copy. It's for ad hoc work and POCs.
- C: wrong. It works, but it's custom code with ordering/dedup risk and the opposite of "minimal maintenance".
- D: wrong. Auto Loader ingests data files from object storage. It isn't a database CDC reader.
(§6)
</details>

---

### Q7
A silver streaming table has this definition:
```sql
CONSTRAINT valid_amount EXPECT (amount >= 0)
```
A batch arrives containing 120 rows with negative amounts. What happens?

- A. The 120 rows are dropped and counted as `dropped_records`.
- B. The update fails and rolls back.
- C. The 120 rows are written to the table, and the event log records them as failed for `valid_amount`.
- D. The 120 rows are written to an automatically created quarantine table.

<details><summary>Answer</summary>

**C.** With no `ON VIOLATION` clause, the action is **warn** (the default). Invalid rows are written and the metrics (`failed_records`) appear under `flow_progress.data_quality.expectations`.
- A: wrong. That needs `ON VIOLATION DROP ROW`.
- B: wrong. That needs `ON VIOLATION FAIL UPDATE`.
- D: wrong. There's no automatic quarantine. You build one yourself with an inverse filter.
(§9)
</details>

---

### Q8 (Choose two)
In a Python pipeline you have `rules = {"valid_id": "id IS NOT NULL", "valid_ts": "ts IS NOT NULL", "valid_qty": "qty > 0"}`. A row that violates **any** rule must be dropped. Which **two** decorators are valid on the dataset function?

- A. `@dp.expect_all_or_drop(rules)`
- B. `@dp.expect_or_drop(rules)`
- C. `@dlt.expect_all_or_drop(rules)` (with `import dlt`)
- D. `@dp.expect_all(rules, on_violation="DROP")`

<details><summary>Answer</summary>

**A and C.** Groups of rules (a dict) use the `expect_all*` family. The legacy `dlt` module is still supported with the same decorator names.
- B: wrong. `expect_or_drop` takes a single `(name, condition)` pair, not a dict.
- D: wrong. There's no `on_violation` parameter. `expect_all` is warn-only.
(§9, §0)
</details>

---

### Q9
A **triggered** pipeline has two independent flows running in parallel, `silver_orders` and `silver_payments`. `silver_orders` has `ON VIOLATION FAIL UPDATE`, and a bad record arrives. Which statement is correct?

- A. Both flows fail, and the pipeline rolls back all tables.
- B. `silver_orders` fails and its transaction is rolled back. `silver_payments` is not failed by it, and no expectation metrics are recorded for the failed `silver_orders` update.
- C. `silver_orders` writes the valid rows and skips the bad one.
- D. The bad record is written, and the update is marked as succeeded with warnings.

<details><summary>Answer</summary>

**B.** FAIL rolls back the failing flow's update atomically. In **triggered** mode it doesn't fail parallel flows (in continuous mode it stops the flow and its dependents). Because the update failed, no metrics are recorded.
- A: wrong. Parallel flows aren't failed in triggered mode.
- C: wrong. That's DROP behavior.
- D: wrong. That's warn behavior.
(§9, §12)
</details>

---

### Q10
A CDC feed has `customer_id, ..., op, change_ts (source commit time), landed_ts (file arrival time), lsn`. Events arrive out of order, and two updates for the same customer can share the same `change_ts`; `lsn` is unique and increasing. Which `SEQUENCE BY` is correct?

- A. `SEQUENCE BY landed_ts`
- B. `SEQUENCE BY change_ts`
- C. `SEQUENCE BY STRUCT(change_ts, lsn)`
- D. `SEQUENCE BY STRUCT(landed_ts, change_ts)`

<details><summary>Answer</summary>

**C.** Order by the source commit time, and break ties with `lsn` through a `STRUCT` (compared field by field). Each sequence value must be distinct per key.
- A: wrong. Arrival time doesn't reflect the real order of changes, which defeats out-of-order handling.
- B: wrong. Ties on `change_ts` make the order ambiguous; AUTO CDC needs one distinct update per key per sequence value.
- D: wrong. It orders by arrival first, which is the same problem as A.
(§10)
</details>

---

### Q11
You maintain `dim_product` with `AUTO CDC ... STORED AS SCD TYPE 2`. Business users want a new history row **only when `price` or `category` changes**. Changes to `description` should overwrite the current row and `description` must remain in the table. What should you add?

- A. `COLUMNS * EXCEPT (description)`
- B. `TRACK HISTORY ON (price, category)`
- C. `IGNORE NULL UPDATES`
- D. Change to `STORED AS SCD TYPE 1` and add `APPLY AS TRUNCATE WHEN op = 'T'`

<details><summary>Answer</summary>

**B.** `TRACK HISTORY ON` restricts which columns create new versions. Changes to other columns update the current version in place. (Python: `track_history_column_list=["price","category"]`.)
- A: wrong. It removes `description` from the target entirely.
- C: wrong. It controls null handling in partial updates, not versioning.
- D: wrong. SCD1 loses all history. (TRUNCATE being SCD1-only is a separate trap.)
(§10, §11)
</details>

---

### Q12
A pipeline on **classic compute** uses the **Pro** edition. A developer adds `CONSTRAINT valid_id EXPECT (id IS NOT NULL) ON VIOLATION DROP ROW` to a table that is already fed by an `AUTO CDC` flow. The next update fails validation. What is the minimal fix?

- A. Switch the edition to Core.
- B. Switch the edition to Advanced (or move the pipeline to serverless).
- C. Rewrite the expectation as `@dp.expect_or_drop` in Python.
- D. Move the `AUTO CDC` flow into a separate pipeline.

<details><summary>Answer</summary>

**B.** Pro supports CDC but **not expectations**. Advanced supports both, and serverless includes everything.
- A: wrong. Core supports neither CDC nor expectations.
- C: wrong. The language doesn't matter; it's an edition feature.
- D: wrong. The pipeline with the expectation would still need Advanced.
(§13)
</details>

---

### Q13
`silver_orders` (a streaming table) receives late corrections through DML `UPDATE` and `DELETE` statements. You need `gold_customer_revenue`, a revenue aggregate per customer joined to `dim_customer`, that is **always correct** after each pipeline update. Cost of recompute is acceptable. How should you define the gold dataset?

- A. `CREATE OR REFRESH STREAMING TABLE gold_customer_revenue AS SELECT ... FROM STREAM silver_orders JOIN dim_customer ...`
- B. `CREATE OR REFRESH MATERIALIZED VIEW gold_customer_revenue AS SELECT ... FROM silver_orders JOIN dim_customer ...`
- C. A streaming table reading `silver_orders` with `skipChangeCommits = true`
- D. A temporary view

<details><summary>Answer</summary>

**B.** MVs have batch semantics: each refresh reflects updates, deletes and dimension changes, and joins recompute. (Incremental refresh happens only on serverless; otherwise it's a full recompute, which the question allows.)
- A: wrong. A streaming read fails on the update/delete commits. Even if it didn't, the ST would only process new rows, and stream-static joins don't recompute.
- C: wrong. `skipChangeCommits` **ignores** the corrections, so the result would be wrong.
- D: wrong. It isn't persisted or published as a table for consumers.
(§7, §8)
</details>

---

### Q14 (Choose two)
`bronze_clicks` is a streaming table that reads from Kafka with a **3-day retention**. A new engineer plans a "Full refresh all" to fix a bug in a downstream silver table. Which **two** statements are correct?

- A. A full refresh of `bronze_clicks` truncates it, clears its checkpoints, and can only re-read what Kafka still retains, so older data is lost.
- B. Setting the table property `pipelines.reset.allowed = false` on `bronze_clicks` prevents it from being fully refreshed.
- C. A full refresh of a streaming table keeps its existing data and only reprocesses new offsets.
- D. Materialized views downstream of `bronze_clicks` are unaffected by a full refresh of it.

<details><summary>Answer</summary>

**A and B.**
- A: correct. A full refresh truncates the ST, removes its checkpoints and replays the source, so anything past retention is gone.
- B: correct. `pipelines.reset.allowed = false` protects the table. (To reprocess without truncating, use *reset streaming flow checkpoints* via the REST API.)
- C: wrong. That describes neither a full refresh nor a default refresh correctly.
- D: wrong. Downstream tables must also be fully refreshed, or the pipeline fails (unless they read with `skipChangeCommits`). MVs must be fully refreshed.
(§14)
</details>

---

### Q15
A developer clicks **Run now** in the Lakeflow Pipelines Editor on a triggered pipeline. A transient cluster-start error isn't retried, and after the update finishes the cluster is still running an hour later. The same pipeline runs nightly as a task in a Lakeflow Job. Which statement is correct?

- A. This is a misconfiguration. Set the pipeline to Continuous so it retries.
- B. This is expected development-style ("fast-start") behavior for UI-triggered runs: cluster reuse (2 h default) and no retries. The Job-triggered nightly run gets production behavior: retries, and the cluster shuts down right after the run.
- C. Both runs behave the same, because run behavior is set only in the pipeline's storage settings.
- D. UI-triggered runs publish tables to a separate development schema.

<details><summary>Answer</summary>

**B.** Update run behavior depends on how the update starts. UI **Run now** for non-continuous pipelines reuses the cluster (`pipelines.clusterShutdown.delay`, default 2 h) and disables retries. Jobs, API and continuous updates retry, restart on recoverable errors, and shut the cluster down after the run. This replaces the old Development/Production toggle.
- A: wrong. Continuous mode adds always-on cost. It's not the way to get retries for a scheduled batch.
- C: wrong. Run behavior is determined by the trigger source, not by storage settings.
- D: wrong. Run behavior never changes the storage location or target schema.
(§12, §13)
</details>

---

## Scoring

**13+/15 = move on. 10–12 = reread the sections tied to the questions you missed. <10 = reread `01-qa.md` sections §1–§5 (Q1–Q5: Auto Loader, schema evolution, discovery, read_files, COPY INTO), §6 (Q6: Lakeflow Connect), §9 (Q7–Q9: expectations), §10–§11 (Q10–Q11: AUTO CDC / SCD), §7–§8 (Q13: ST vs MV) and §12–§14 (Q12, Q14, Q15: modes, editions, full refresh), then redo `02-exercises.md` Ex 1 and Ex 4.**
