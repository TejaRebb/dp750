# Module 2 — Delta Lake: Exercises (second pass)

> Four scenarios in increasing difficulty. Fill each `__(n)__` blank, or fix the marked bug. Hints follow every code block. Solutions are collapsed. Try the exercise before you open them.

---

## Exercise 1 (warm-up): Undo a bad DELETE — 5 blanks

**Scenario.** At 09:14 a junior engineer ran `DELETE FROM main.sales.orders WHERE region = 'EU'` by mistake. The table is at version 42 and the DELETE was version 42. You need to (a) find the last good version, (b) check its row count without changing anything, and (c) roll back.

```sql
-- (a) find the bad commit
DESCRIBE __(1)__ main.sales.orders LIMIT 5;

-- (b) verify the last good state, two equivalent syntaxes
SELECT count(*) FROM main.sales.orders __(2)__ 41;
SELECT count(*) FROM main.sales.orders@__(3)__;

-- (c) roll back
RESTORE TABLE main.sales.orders TO VERSION AS OF __(4)__;

-- (d) After the RESTORE, what version number does DESCRIBE HISTORY show at the top,
--     and what is its operation?  Answer: version __(5)__
```

**Hints**
- (1) The command that lists commits newest first, with `operation` and `operationMetrics`.
- (2) The SQL temporal clause that takes a number.
- (3) The `@` shorthand prefixes the version with a letter.
- (5) RESTORE never rewrites history.

<details><summary>Solution</summary>

```sql
DESCRIBE HISTORY main.sales.orders LIMIT 5;
SELECT count(*) FROM main.sales.orders VERSION AS OF 41;
SELECT count(*) FROM main.sales.orders@v41;
RESTORE TABLE main.sales.orders TO VERSION AS OF 41;
-- top of history: version 43, operation RESTORE
```
1. `HISTORY`: `DESCRIBE HISTORY` shows `version`, `operation` (`DELETE`) and `operationMetrics` (`numDeletedRows`).
2. `VERSION AS OF`: read-only time travel by version. `TIMESTAMP AS OF '...09:13'` would also work.
3. `v41`: `@v<version>`. The timestamp form is `@yyyyMMddHHmmssSSS`.
4. `41`: the last version before the bad DELETE.
5. `43`: RESTORE is itself a commit (operation `RESTORE`). Versions 42 and 41 remain in history. It works only because v41's files have not been VACUUMed (default 7-day retention).

</details>

---

## Exercise 2: Migrate a Z-ordered table to liquid clustering — 6 blanks + 1 bug

**Scenario.** `main.iot.events` is a 3 TB **unpartitioned** UC managed table. A nightly job runs `OPTIMIZE ... ZORDER BY (device_id)`. Analysts filter on `device_id` and `event_ts`. You must move it to liquid clustering, recluster existing data, later hand key selection to Databricks, and stop managing maintenance jobs.

```sql
-- 1. Switch layout on the existing table
ALTER TABLE main.iot.events __(1)__ (device_id, event_ts);

-- 2. Recluster *existing* data, not just new data
OPTIMIZE main.iot.events __(2)__;

-- 3. Nightly job (left over from before). BUG: find it
OPTIMIZE main.iot.events ZORDER BY (device_id);

-- 4. New aggregate table built with CTAS and clustered on site_id
CREATE TABLE main.iot.events_daily __(3)__ (site_id)
AS SELECT site_id, date(event_ts) AS d, count(*) AS n FROM main.iot.events GROUP BY ALL;
```
```python
# 5. Same idea from PySpark for another table (create/overwrite only!)
(df.write
   .__(4)__("site_id")
   .mode("overwrite")
   .saveAsTable("main.iot.sites_clustered"))
```
```sql
-- 6. Let Databricks choose keys from the query workload
ALTER TABLE main.iot.events CLUSTER BY __(5)__;

-- 7. Requirement for (6) + removes the need for scheduled OPTIMIZE/VACUUM
ALTER SCHEMA main.iot __(6)__ PREDICTIVE OPTIMIZATION;
```

**Hints**
- (1) Same keyword as in `CREATE TABLE ... ____ (cols)`. It works because the table is unpartitioned.
- (2) Plain `OPTIMIZE` is incremental and skips files already clustered or not needing it. You want everything reclustered after a key change (DBR 16.0+).
- Bug: can liquid clustering and Z-order coexist?
- (3) In CTAS the clause goes after the table name, before `AS`.
- (4) The DataFrameWriter method, analogous to `partitionBy`.
- (5) One keyword. It needs a UC managed table.

<details><summary>Solution</summary>

```sql
ALTER TABLE main.iot.events CLUSTER BY (device_id, event_ts);
OPTIMIZE main.iot.events FULL;
-- FIX: remove ZORDER, just run:  OPTIMIZE main.iot.events;   (or drop the job once PO is on)
CREATE TABLE main.iot.events_daily CLUSTER BY (site_id)
AS SELECT site_id, date(event_ts) AS d, count(*) AS n FROM main.iot.events GROUP BY ALL;
```
```python
df.write.clusterBy("site_id").mode("overwrite").saveAsTable("main.iot.sites_clustered")
```
```sql
ALTER TABLE main.iot.events CLUSTER BY AUTO;
ALTER SCHEMA main.iot ENABLE PREDICTIVE OPTIMIZATION;
```
1. `CLUSTER BY`: enables liquid clustering on an existing **unpartitioned** table. It's metadata only, so no data is rewritten. (A partitioned table would need `REPLACE PARTITIONED BY WITH CLUSTER BY`.)
2. `FULL`: `OPTIMIZE ... FULL` reclusters all records. Without it, previously written data keeps its old layout.
- **Bug**: `ZORDER BY` on a liquid-clustered table errors, because clustering is incompatible with ZORDER. Use plain `OPTIMIZE`, or let predictive optimization run it and delete the job.
3. `CLUSTER BY`: CTAS puts `CLUSTER BY` after the table name, never inside the SELECT.
4. `clusterBy`: allowed only on create/overwrite. In `append` mode you can't set or change keys, so use `ALTER TABLE` instead.
5. `AUTO`: automatic key selection from the query history. It needs a UC managed table and predictive optimization.
6. `ENABLE`: PO runs OPTIMIZE, VACUUM and ANALYZE on UC managed tables and is inherited by tables in the schema. It never runs ZORDER.

</details>

---

## Exercise 3: Incremental gold build from Change Data Feed — 8 blanks

**Scenario.** `main.crm.customers` (silver) receives MERGEs from a CRM. You maintain `main.crm.customers_gold` with a batch job that processes only the rows changed since the last run. `last_processed` holds the last `_commit_version` you applied (100). The gold table must reflect inserts, updates **and deletes**.

```python
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

# One-time: start capturing changes (only changes AFTER this commit are recorded)
spark.sql("""ALTER TABLE main.crm.customers
             SET TBLPROPERTIES (__(1)__ = true)""")

last_processed = 100

changes = (spark.read
    .option("__(2)__", "true")
    .option("__(3)__", last_processed + 1)       # batch reads REQUIRE this
    .table("main.crm.customers")
    .filter(F.col("_change_type") != "__(4)__"))  # drop the 'before' image of updates

# A key can change several times between runs -> keep only its latest change
w = Window.partitionBy("customer_id").orderBy(F.col("__(5)__").desc())
latest = (changes.withColumn("rn", F.row_number().over(w))
                 .filter("rn = 1").drop("rn"))

(DeltaTable.forName(spark, "main.crm.customers_gold").alias("t")
   .merge(latest.alias("s"), "t.customer_id = s.customer_id")
   .whenMatchedDelete(condition="s._change_type = '__(6)__'")
   .whenMatchedUpdateAll()
   .whenNotMatchedInsertAll(condition="s._change_type != 'delete'")
   .execute())

# Remember where we got to
new_last = latest.agg(F.max("__(7)__")).first()[0]
```
```sql
-- Same change set in SQL for ad-hoc inspection
SELECT customer_id, _change_type, _commit_version, _commit_timestamp
FROM __(8)__('main.crm.customers', 101)
ORDER BY _commit_version;
```

**Hints**
- (1) The table property name for CDF (with the `delta.` prefix).
- (2) The DataFrameReader option that switches to the change feed.
- (3) Start of the range. Its counterpart, `endingVersion`, is optional.
- (4) The four `_change_type` values: `insert`, `delete`, `update_preimage`, `update_postimage`. Which one is the "before" image?
- (5) and (7) The metadata column holding the table version of each change.
- (6) The change type that should remove the gold row.
- (8) The SQL table-valued function.
- Why dedupe at all? Think about what MERGE does when two source rows match one target row.

<details><summary>Solution</summary>

```python
spark.sql("ALTER TABLE main.crm.customers SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
changes = (spark.read
    .option("readChangeFeed", "true")
    .option("startingVersion", last_processed + 1)
    .table("main.crm.customers")
    .filter(F.col("_change_type") != "update_preimage"))
w = Window.partitionBy("customer_id").orderBy(F.col("_commit_version").desc())
...
   .whenMatchedDelete(condition="s._change_type = 'delete'")
...
new_last = latest.agg(F.max("_commit_version")).first()[0]
```
```sql
SELECT ... FROM table_changes('main.crm.customers', 101) ORDER BY _commit_version;
```
1. `delta.enableChangeDataFeed`: enables legacy CDF. Commits before it can't be read, and asking for an earlier version raises an error.
2. `readChangeFeed`: turns a normal read into a change-feed read with the extra `_change_type`, `_commit_version` and `_commit_timestamp` columns.
3. `startingVersion`: **mandatory for batch** CDF reads, inclusive. (`startingTimestamp` is the alternative.) Streaming may omit it and then starts from a full snapshot as inserts.
4. `update_preimage`: an UPDATE emits pre and post images. Keep only `update_postimage` (plus `insert`/`delete`).
5. `_commit_version`: orders the changes per key so the latest one wins.
6. `delete`: a deleted silver row deletes the gold row. The insert clause is guarded so a deleted-but-never-seen key isn't inserted.
7. `_commit_version`: the max processed version becomes the next run's start minus 1.
8. `table_changes`: `table_changes(table_str, start [, end])`. Start and end are inclusive.
- **Why dedupe:** without it, a key updated twice gives two source rows for one target row, and MERGE fails with `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE`.
- **Retention note:** `_change_data` files are removed by VACUUM. If the job is down longer than the retention period (7 days by default), version 101 may no longer be readable.

</details>

---

## Exercise 4 (hard): HR table lifecycle — 12 blanks + 2 bugs

**Scenario.** `main.hr.employees` is a UC managed Delta table with deletion vectors enabled. Each night a **full snapshot** lands in `main.hr.employees_snapshot`. It can contain duplicate `emp_id` rows (re-extracts), and this week it gained a new column `cost_center`. Requirements:
1. Test the new logic on a **zero-copy** copy of prod first.
2. Upsert changed rows, insert new ones, and **soft-delete** (flag) employees missing from the snapshot. The new column must appear in the target automatically.
3. Honour a GDPR erasure request for employee 4711 so their data is **physically** gone from storage.
4. Rename `dept` to `department` without rewriting 2 TB of data.
5. Audit how many rows last night's MERGE updated.
6. Allow 30 days of time travel from now on.

```sql
-- 1. Cheap test copy (references prod files, copies only metadata)
CREATE TABLE main.hr.employees_test __(1)__ CLONE main.hr.employees;

-- 2. Nightly upsert
MERGE __(2)__ INTO main.hr.employees AS t
USING (
  SELECT * FROM main.hr.employees_snapshot
  QUALIFY row_number() OVER (PARTITION BY emp_id ORDER BY __(3)__ DESC) = 1
) AS s
ON t.emp_id = s.emp_id
WHEN MATCHED AND s.row_hash <> t.row_hash THEN UPDATE SET *
WHEN NOT MATCHED THEN __(4)__
WHEN NOT MATCHED BY __(5)__ AND t.is_active = true
  THEN UPDATE SET t.is_active = __(6)__, t.deactivated_at = current_timestamp();

-- BUG A (someone proposed this extra clause to copy the final salary):
-- WHEN NOT MATCHED BY SOURCE THEN UPDATE SET t.salary = s.salary

-- 3. GDPR erasure
DELETE FROM main.hr.employees WHERE emp_id = 4711;
REORG TABLE main.hr.employees __(7)__;
VACUUM main.hr.employees __(8)__ 168 HOURS;

-- BUG B (someone wanted it gone "right now"):
-- VACUUM main.hr.employees RETAIN 0 HOURS;

-- 4. Metadata-only rename
ALTER TABLE main.hr.employees SET TBLPROPERTIES ('__(9)__' = 'name');
ALTER TABLE main.hr.employees __(10)__ COLUMN dept TO department;

-- 5. Audit last night's MERGE (look at operationMetrics.numTargetRowsUpdated)
DESCRIBE __(11)__ main.hr.employees;

-- 6. 30-day time travel window
ALTER TABLE main.hr.employees SET TBLPROPERTIES ('__(12)__' = 'interval 30 days');
```

**Hints**
- (1) "Zero-copy" means metadata only.
- (2) The per-statement clause that lets MERGE add `cost_center` (DBR 15.2+). In Python this is `.withSchemaEvolution()`.
- (3) Pick the snapshot's extraction timestamp column (assume `extracted_at`). Why does this subquery exist at all?
- (4) The insert-all shorthand.
- (5) Target rows with no source row.
- (6) A boolean literal. Note that this clause **can't** reference `s.*`.
- (7) With DVs, `DELETE` only soft-deletes. This rewrites the affected files.
- (8) The VACUUM keyword before a number of hours.
- (9) The column mapping property (`name` mode works on existing tables, `id` doesn't).
- (11) Commit-level audit, newest first.
- (12) Log retention is already 30 days. Which retention actually limits time travel once VACUUM runs?
- Bug A: what source row exists for a target row that is "not matched by source"?
- Bug B: what does the safety check say, and what would it break?

<details><summary>Solution</summary>

```sql
CREATE TABLE main.hr.employees_test SHALLOW CLONE main.hr.employees;

MERGE WITH SCHEMA EVOLUTION INTO main.hr.employees AS t
USING (
  SELECT * FROM main.hr.employees_snapshot
  QUALIFY row_number() OVER (PARTITION BY emp_id ORDER BY extracted_at DESC) = 1
) AS s
ON t.emp_id = s.emp_id
WHEN MATCHED AND s.row_hash <> t.row_hash THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE AND t.is_active = true
  THEN UPDATE SET t.is_active = false, t.deactivated_at = current_timestamp();

DELETE FROM main.hr.employees WHERE emp_id = 4711;
REORG TABLE main.hr.employees APPLY (PURGE);
VACUUM main.hr.employees RETAIN 168 HOURS;

ALTER TABLE main.hr.employees SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE main.hr.employees RENAME COLUMN dept TO department;

DESCRIBE HISTORY main.hr.employees;

ALTER TABLE main.hr.employees SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 30 days');
```
1. `SHALLOW`: copies only the metadata and references prod's files. Writes to the clone never touch prod. (In UC you can't `CREATE OR REPLACE` an existing shallow clone, so drop it first next time.)
2. `WITH SCHEMA EVOLUTION`: adds `cost_center` through `UPDATE SET *` / `INSERT *`. The `mergeSchema` writer option does nothing for MERGE.
3. `extracted_at`: dedupes the source to one row per key. Duplicate `emp_id`s would otherwise raise the multiple-source-rows-match error.
4. `INSERT *`: `WHEN NOT MATCHED` can only INSERT.
5. `SOURCE`: `WHEN NOT MATCHED BY SOURCE` handles target rows absent from the full snapshot (UPDATE or DELETE allowed).
6. `false`: a soft delete by flag. Literals or target columns only.
- **Bug A**: `NOT MATCHED BY SOURCE` clauses **can't reference source columns**, because there is no source row to read `s.salary` from. Remove the clause, or set the column from a literal or target expression. (Also remember: if you stack several `NOT MATCHED BY SOURCE` clauses, every one except the last needs a condition.)
7. `APPLY (PURGE)`: with DVs the DELETE only marked the rows. `REORG ... APPLY (PURGE)` rewrites the files so the current version no longer contains them.
8. `RETAIN`: VACUUM removes the pre-purge files only once they are older than the retention period. So the bytes are physically gone after 7 days plus the next VACUUM (or predictive optimization's VACUUM).
- **Bug B**: `RETAIN 0 HOURS` fails the `retentionDurationCheck` (anything under 168 h). Disabling that check risks corrupting in-flight writers and kills all time travel and RESTORE. The GDPR-compliant path is PURGE + normal-retention VACUUM. (Requirement 6 also extends retention to 30 days, so the physical erasure happens 30 days later. Make sure the compliance SLA allows it.)
9. `delta.columnMapping.mode`: `name` mode can be set on an existing table. It upgrades the protocol to reader 2 / writer 5.
10. `RENAME`: metadata-only with column mapping. Without column mapping it fails. The `overwriteSchema` alternative would rewrite the whole 2 TB.
11. `HISTORY`: `operationMetrics` contains `numTargetRowsUpdated`, `numTargetRowsInserted`, and so on.
12. `delta.deletedFileRetentionDuration`: the default 7 days is the binding limit for time travel after VACUUM. `logRetentionDuration` is already 30 days.

</details>
