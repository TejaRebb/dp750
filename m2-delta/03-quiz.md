# Module 2 — Delta Lake: Quiz (final check)

> 15 scenario questions in DP-750 style. Pick one answer unless the question says **choose two**. Commit to an answer before you expand it. Section references (§) point to `01-qa.md`.

---

### Q1
You have a 400 GB Unity Catalog managed Delta table `sales.orders` in an Azure Databricks workspace. Most queries filter on `customer_id` (about 20 million distinct values), and some also filter on `order_date`. The table is currently unpartitioned. You need the best query performance with the least ongoing tuning. What should you do?

- A. Recreate the table with `PARTITIONED BY (order_date)` and run `OPTIMIZE ... ZORDER BY (customer_id)` nightly.
- B. Recreate the table with `PARTITIONED BY (customer_id)`.
- C. Run `ALTER TABLE sales.orders CLUSTER BY (customer_id, order_date)` and then `OPTIMIZE sales.orders FULL`.
- D. Leave the table unpartitioned and rely on ingestion-time clustering only.

<details><summary>Answer</summary>

**C.** Liquid clustering is the recommended layout for new and existing unpartitioned tables, and it handles high-cardinality keys. `FULL` reclusters the data already in the table.
- A: Microsoft says not to partition tables under 1 TB. Partitioning plus Z-order is the legacy approach and needs nightly re-Z-ordering, which isn't incremental.
- B: Partitioning on a 20M-cardinality column creates millions of tiny partitions. That is the classic anti-pattern.
- D: Ingestion-time clustering only helps filters that correlate with ingestion order. It does nothing for `customer_id`.

</details>

---

### Q2
A liquid-clustered table was clustered by `(region)`. You ran `ALTER TABLE t CLUSTER BY (device_id, event_ts)`. A week later, queries that filter on `device_id` over **old** data are still slow, while queries over newly written data are fast. A scheduled `OPTIMIZE t` runs nightly. What should you do?

- A. Run `OPTIMIZE t FULL`.
- B. Run `OPTIMIZE t ZORDER BY (device_id)`.
- C. Run `VACUUM t` to remove the files clustered by `region`.
- D. Run `ALTER TABLE t CLUSTER BY NONE` and then re-enable clustering.

<details><summary>Answer</summary>

**A.** Changing keys is metadata-only, so existing data isn't rewritten. Plain `OPTIMIZE` is incremental and doesn't recluster files that are already clustered. `OPTIMIZE ... FULL` reclusters all existing records with the new keys.
- B: `ZORDER` can't be used on a liquid-clustered table. It errors.
- C: VACUUM deletes only unreferenced files. The old-layout files are still the current data.
- D: `CLUSTER BY NONE` doesn't rewrite data either. Re-enabling leaves you where you started.

</details>

---

### Q3
Your team wants Databricks to choose clustering keys automatically and to stop running manual OPTIMIZE and VACUUM jobs for table `ext.raw_events`, which is an **external** table in Unity Catalog. `ALTER TABLE ext.raw_events CLUSTER BY AUTO` does not give the expected behaviour. What is the cause?

- A. `CLUSTER BY AUTO` requires Databricks Runtime 13.3 LTS.
- B. `CLUSTER BY AUTO` and predictive optimization apply only to Unity Catalog **managed** tables.
- C. The table must be partitioned before automatic clustering can be enabled.
- D. Automatic clustering requires that the table was previously Z-ordered.

<details><summary>Answer</summary>

**B.** Automatic liquid clustering depends on predictive optimization, and both are limited to UC managed tables. The fix is to convert or migrate the table to a managed table, or to choose keys manually and schedule maintenance.
- A: The requirement is DBR 15.4 LTS+. 13.3 is too old for AUTO, and the version isn't the root cause here.
- C: Clustering is incompatible with partitioning, so a partitioned table is the opposite of what's needed.
- D: There is no Z-order prerequisite. ZORDER and clustering are mutually exclusive.

</details>

---

### Q4 (choose two)
Table `hr.employees` has deletion vectors enabled. A GDPR request requires that the rows of one employee be physically removed from storage. You already ran `DELETE FROM hr.employees WHERE emp_id = 4711`. Which **two** actions are required to complete the erasure?

- A. Run `REORG TABLE hr.employees APPLY (PURGE)`.
- B. Run `VACUUM hr.employees` once the purged files are past the retention threshold.
- C. Run `ALTER TABLE hr.employees SET TBLPROPERTIES ('delta.enableDeletionVectors' = false)`.
- D. Run `DESCRIBE DETAIL hr.employees` to confirm `numFiles` decreased.

<details><summary>Answer</summary>

**A and B.** With DVs, the DELETE only soft-deletes the rows. They still exist in a Parquet file the current version references. `REORG ... APPLY (PURGE)` rewrites those files, and VACUUM then deletes the old files once they are past retention.
- C: Turning the property off affects future writes only. It doesn't rewrite existing files and doesn't even downgrade the protocol (`DROP FEATURE` does that).
- D: DETAIL is a read-only metadata check. It removes nothing, and `numFiles` says nothing about whether specific rows are gone.

</details>

---

### Q5
You run `VACUUM sales.orders RETAIN 24 HOURS` and the command fails. No operation on the table runs longer than one hour, and you accept losing time travel. What must you do so the command succeeds?

- A. Set `delta.logRetentionDuration` to `interval 24 hours`.
- B. Set `spark.databricks.delta.retentionDurationCheck.enabled` to `false`.
- C. Add `DRY RUN` to the command.
- D. Use `VACUUM sales.orders LITE RETAIN 24 HOURS`.

<details><summary>Answer</summary>

**B.** A retention under the 168-hour default trips the safety check. Disabling `retentionDurationCheck` is the documented override, to be used only when no writer runs longer than the retention.
- A: Log retention controls history and log cleanup, not the VACUUM safety check.
- C: `DRY RUN` only lists the files (up to 1000). It still validates the retention and deletes nothing.
- D: LITE changes how files are found (from the log instead of listing storage). It doesn't bypass the retention check.

</details>

---

### Q6
`finance.ledger` uses default table properties, and predictive optimization runs VACUUM on it regularly. Auditors run `SELECT * FROM finance.ledger TIMESTAMP AS OF <10 days ago>` and the query fails. Going forward, you must support time travel for 30 days. What should you change?

- A. Set `delta.logRetentionDuration = 'interval 30 days'`.
- B. Set `delta.deletedFileRetentionDuration = 'interval 30 days'`.
- C. Enable `delta.enableChangeDataFeed`.
- D. Disable checkpointing on the table.

<details><summary>Answer</summary>

**B.** Time travel needs both the log (30 days by default) and the data files. VACUUM removes data files older than `deletedFileRetentionDuration` (7 days by default), so that is the binding limit.
- A: Log retention is **already** 30 days by default, so changing it alone does nothing for the data files.
- C: CDF records row changes from now on. It doesn't keep old versions queryable, and its files follow the same retention.
- D: Checkpointing is managed by Databricks and doesn't control file retention.

</details>

---

### Q7
`ops.inventory` is at version 12. Version 9 was a bad MERGE. You run `RESTORE TABLE ops.inventory TO VERSION AS OF 8`. A Structured Streaming job reads `ops.inventory` as a source. Which statement is true?

- A. Versions 9–12 are removed from the transaction log.
- B. A new version 13 is committed, and the streaming job may process the restored files as new data, which can produce duplicates.
- C. The command fails unless Change Data Feed is enabled.
- D. The command requires `OWNER` on the table. `MODIFY` is not sufficient.

<details><summary>Answer</summary>

**B.** RESTORE is itself a commit with `dataChange = true`. Downstream streams treat the re-added files as new data.
- A: History isn't rewritten. Versions 9–12 stay in the log (and stay time-travelable within retention).
- C: RESTORE has nothing to do with CDF. It needs only that version 8's data files have not been VACUUMed.
- D: The documented requirement is `MODIFY` permission.

</details>

---

### Q8
On `crm.contacts`, `delta.enableChangeDataFeed = true` was set in the commit that created version 20. The table is now at version 35. A developer runs `SELECT * FROM table_changes('crm.contacts', 15, 35)`. What happens?

- A. Rows for versions 20–35 are returned. Earlier versions are skipped silently.
- B. The query returns an error.
- C. An empty result set is returned.
- D. The snapshot at version 15 is returned as `insert` rows, followed by changes through version 35.

<details><summary>Answer</summary>

**B.** CDF captures changes only from the version where it was enabled onward. Specifying a start version before CDF was enabled raises an error.
- A: Nothing is skipped silently. The range is validated.
- C: Empty results come only from the out-of-range tolerance setting (`timestampOutOfRange.enabled`) for starts **beyond the latest** commit. That isn't the case here.
- D: A snapshot-as-inserts only happens when a **stream** starts without `startingVersion`. Batch `table_changes` never does it.

</details>

---

### Q9
`sales.daily` is a liquid-clustered (unpartitioned) Delta table. You must reload all rows for January 2025 from DataFrame `df_jan` in one **atomic** operation, leaving every other month untouched. On classic compute, which write should you use?

- A. `df_jan.write.mode("overwrite").option("partitionOverwriteMode","dynamic").saveAsTable("sales.daily")`
- B. `df_jan.write.mode("overwrite").option("replaceWhere","order_date >= '2025-01-01' AND order_date < '2025-02-01'").saveAsTable("sales.daily")`
- C. A `MERGE` of `df_jan` into `sales.daily` with `WHEN MATCHED THEN UPDATE SET *` and `WHEN NOT MATCHED THEN INSERT *`.
- D. `DELETE FROM sales.daily WHERE order_date BETWEEN ...` followed by `df_jan.write.mode("append")`.

<details><summary>Answer</summary>

**B.** `replaceWhere` atomically replaces exactly the rows that match an arbitrary predicate. The table doesn't need to be partitioned. By default it also fails if `df_jan` contains rows outside the predicate, which is a useful safety net.
- A: Dynamic partition overwrite is legacy and supports only **partitioned** tables. A liquid-clustered table can't be partitioned, so there are no partitions to scope the overwrite to January.
- C: MERGE doesn't delete January rows missing from `df_jan` (you would need `NOT MATCHED BY SOURCE` with a date filter), and it's more expensive.
- D: Two separate commits aren't atomic. Readers can see January empty in between.

</details>

---

### Q10
A nightly `MERGE INTO dim.customer USING staging.customer_changes ON ...` started failing with `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE`. The staging table now receives several change records per customer per day. What should you do?

- A. Set `spark.databricks.delta.schema.autoMerge.enabled = true`.
- B. Deduplicate the source so it contains only the latest record per key (for example `row_number()` over the key ordered by change timestamp) before the MERGE.
- C. Add a `WHEN NOT MATCHED BY SOURCE THEN DELETE` clause.
- D. Change the table isolation level to `Serializable`.

<details><summary>Answer</summary>

**B.** The error means two or more source rows match the same target row for an update, which is ambiguous. The documented fix is to preprocess the source so there is one row per key.
- A: autoMerge is about schema evolution, not row ambiguity.
- C: That clause handles target rows with **no** source match. It doesn't resolve multiple matches.
- D: Isolation level governs concurrent-transaction conflicts, not the semantics of a single MERGE.

</details>

---

### Q11
Each night you receive a **complete** snapshot of active products. In `dim.product` you must update changed products, insert new ones, and **delete** products that no longer appear in the snapshot, all in one statement. Which clause handles the deletions?

- A. `WHEN NOT MATCHED THEN DELETE`
- B. `WHEN NOT MATCHED BY SOURCE THEN DELETE`
- C. `WHEN MATCHED AND s.product_id IS NULL THEN DELETE`
- D. `WHEN NOT MATCHED BY TARGET THEN DELETE`

<details><summary>Answer</summary>

**B.** `WHEN NOT MATCHED BY SOURCE` fires for target rows that have no matching source row, and it allows `DELETE` or `UPDATE`.
- A: `WHEN NOT MATCHED` (= `BY TARGET`) is for source rows with no target row, and it allows only `INSERT`. `DELETE` there is invalid.
- C: A matched row always has a source row, so the condition is never true when the key is in the ON clause.
- D: Same as A. `BY TARGET` is just an alias for `NOT MATCHED`.

</details>

---

### Q12
A daily job runs `df.write.mode("append").saveAsTable("bronze.clicks")`. Upstream added a new column `campaign_id`, and the job now fails with a schema mismatch. You need the column added to the table with the least disruption and without rewriting existing data. What should you do?

- A. Add `.option("overwriteSchema", "true")` to the append.
- B. Add `.option("mergeSchema", "true")` to the append.
- C. Enable column mapping with `delta.columnMapping.mode = 'name'`.
- D. Replace the job with `CREATE OR REPLACE TABLE bronze.clicks AS SELECT ...`.

<details><summary>Answer</summary>

**B.** `mergeSchema` evolves the schema during the write by adding new columns. Existing rows show NULL for the new column, and no rewrite happens.
- A: `overwriteSchema` requires `mode("overwrite")` and rewrites the table's data. It doesn't apply to appends.
- C: Column mapping enables rename and drop. It doesn't make appends accept new columns.
- D: It rewrites the entire table and resets any properties or layout you don't re-specify, which is maximal disruption.

</details>

---

### Q13
You must rename column `cust_nm` to `customer_name` on a 20 TB Delta table **without rewriting data files**. The table currently has `delta.columnMapping.mode = 'none'`. Which sequence is correct?

- A. `ALTER TABLE t RENAME COLUMN cust_nm TO customer_name` only.
- B. `ALTER TABLE t SET TBLPROPERTIES ('delta.columnMapping.mode' = 'id')`, then `RENAME COLUMN`.
- C. `ALTER TABLE t SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')`, then `RENAME COLUMN`.
- D. Read the table, `withColumnRenamed`, and write with `mode("overwrite")` and `overwriteSchema = true`.

<details><summary>Answer</summary>

**C.** `name` mode can be enabled on an existing table. After that, `RENAME COLUMN` is metadata-only. Note the protocol upgrade to reader 2 / writer 5.
- A: Without column mapping, `RENAME COLUMN` isn't supported on Delta.
- B: `id` mode can only be set at table creation, not on an existing table.
- D: This works, but it rewrites all 20 TB, which violates the requirement.

</details>

---

### Q14
A nightly job rebuilds `gold.kpi` and the output **schema changes** between releases. Requirements: the swap must be atomic for concurrent readers, and auditors must still be able to query the previous night's version. What should the job run?

- A. `DROP TABLE gold.kpi`, then `CREATE TABLE gold.kpi AS SELECT ...`
- B. `CREATE OR REPLACE TABLE gold.kpi AS SELECT ...`
- C. `INSERT OVERWRITE gold.kpi SELECT ...`
- D. `TRUNCATE TABLE gold.kpi`, then `INSERT INTO gold.kpi SELECT ...`

<details><summary>Answer</summary>

**B.** `CREATE OR REPLACE` atomically replaces the schema and data as a new version of the **same** table, so the history is preserved and the previous version stays time-travelable.
- A: DROP + CREATE isn't atomic (readers get "table not found"), and history is lost.
- C: `INSERT OVERWRITE` keeps the existing schema and fails on a schema change. SQL has no `overwriteSchema`.
- D: Two commits, so it's not atomic, and INSERT still fails on the changed schema.

</details>

---

### Q15 (choose two)
You need (1) a cheap, fast copy of `prod.orders` that developers can use to test destructive UPDATE statements for a day, and (2) a disaster-recovery copy in another catalog that stays readable if `prod.orders` is VACUUMed and that lets a Structured Streaming writer resume from where it stopped on the source. Which **two** commands meet these requirements?

- A. `CREATE TABLE dev.orders SHALLOW CLONE prod.orders` for requirement (1).
- B. `CREATE TABLE dr.orders DEEP CLONE prod.orders` for requirement (2).
- C. `CREATE TABLE dr.orders SHALLOW CLONE prod.orders` for requirement (2).
- D. `CREATE TABLE dr.orders AS SELECT * FROM prod.orders` for requirement (2).

<details><summary>Answer</summary>

**A and B.** A shallow clone copies only metadata, so it's cheap and fast, and writes to it never affect prod. A deep clone copies the data plus the stream and COPY INTO metadata, so it's independent of the source's VACUUM and a stream can continue on it. Re-running `CREATE OR REPLACE ... CLONE` syncs it incrementally.
- C: A shallow clone references the source's files. For non-UC tables, VACUUM on the source breaks it, and it doesn't carry stream metadata.
- D: CTAS copies only the data and the query schema. It carries no table properties or streaming metadata and can't be synced incrementally.

</details>

---

## Scoring

**13+/15 = move on. 10–12 = review the questions you missed in the matching § of `01-qa.md`. <10 = reread notes sections §3 Liquid clustering, §9 Deletion vectors, §10 VACUUM, §11–12 Time travel/RESTORE, §14 CDF, §15 MERGE, §16–17 Schema evolution and column mapping, §18 overwrite options, §19 CLONE.**

| Q | Section |
|---|---|
| 1, 2 | §3 Liquid clustering, §4 Partitioning |
| 3 | §3, §8 Predictive optimization |
| 4 | §9 Deletion vectors, §10 VACUUM |
| 5, 6 | §10 VACUUM, §11 Time travel |
| 7 | §12 RESTORE |
| 8 | §14 CDF |
| 9, 14 | §18 CTAS / CREATE OR REPLACE / replaceWhere |
| 10, 11 | §15 MERGE |
| 12 | §16 Schema evolution |
| 13 | §17 Column mapping |
| 15 | §19 CLONE |
