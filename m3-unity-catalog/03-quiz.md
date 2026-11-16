# Module 3 — Quiz (final check)

15 DP-750-style scenario questions. Answer before opening each answer section. "Choose two" questions need **both** picks correct to score the point.

---

### Q1
Contoso's account group `bi-analysts` was granted `SELECT ON SCHEMA sales.gold`. Analysts can see `sales.gold.orders` in Catalog Explorer, but `SELECT * FROM sales.gold.orders` returns `PERMISSION_DENIED`. The admin wants the minimal fix. What should the admin do?

A. `GRANT ALL PRIVILEGES ON SCHEMA sales.gold TO bi-analysts`
B. `GRANT USE CATALOG ON CATALOG sales` and `GRANT USE SCHEMA ON SCHEMA sales.gold` to `bi-analysts`
C. `GRANT SELECT ON TABLE sales.gold.orders TO bi-analysts`
D. `GRANT BROWSE ON CATALOG sales TO bi-analysts`

<details><summary>Answer</summary>

**B.** Reading requires `USE CATALOG` + `USE SCHEMA` + `SELECT`. `SELECT` on the schema already covers the table, so only the usage privileges are missing.
- A: over-grants (every write and create privilege) and is not minimal. It would work, but it fails "minimal".
- C: redundant. `SELECT` already inherits from the schema, and usage privileges are still missing.
- D: `BROWSE` is metadata-only. They can already see the table, and `BROWSE` doesn't enable queries.

</details>

---

### Q2
You want every user in the Azure Databricks account to be able to find tables in the `finance` catalog, see their descriptions and tags, and request access, **without reading any data**. Which grant meets the requirement?

A. `GRANT USE CATALOG ON CATALOG finance TO \`account users\``
B. `GRANT BROWSE ON CATALOG finance TO users`
C. `GRANT BROWSE ON CATALOG finance TO \`account users\``
D. `GRANT READ METADATA ON SCHEMA finance.default TO \`account users\``

<details><summary>Answer</summary>

**C.** `BROWSE` on the catalog gives metadata discovery and access requests without data access or usage privileges. `account users` is the account-level group of all users and SPs.
- A: `USE CATALOG` doesn't expose child metadata without `USE SCHEMA`, and it is a usage privilege, not a discovery privilege.
- B: `users` is a **workspace-local** system group and can't receive UC grants.
- D: wrong scope (one schema), and `READ METADATA` (a child of `MANAGE`) exposes permissions, filters, and policies, which goes beyond discovery.

</details>

---

### Q3
A data steward holds `ALL PRIVILEGES` on schema `prod.silver`. They try to run `GRANT SELECT ON TABLE prod.silver.customers TO \`marketing\`` and get an error. They must be able to manage grants on the schema and its objects, **without** becoming the owner. What should you grant them?

A. `CREATE TABLE` on `prod.silver`
B. `MANAGE` on `prod.silver`
C. Ownership of `prod.silver`
D. `ALL PRIVILEGES` on catalog `prod`

<details><summary>Answer</summary>

**B.** `MANAGE` allows granting, revoking, transferring ownership, and dropping. It cascades to child objects. `ALL PRIVILEGES` explicitly **excludes** `MANAGE`.
- A: a create privilege, unrelated to granting.
- C: would work, but violates "without becoming owner". There is also only one owner.
- D: still excludes `MANAGE`, so it still can't grant, and it massively over-grants.

</details>

---

### Q4 (choose two)
A pipeline accidentally ran `DROP TABLE` on `ops.gold.kpis` (UC **managed**) and on `ops.bronze.events` (UC **external** over `abfss://raw@.../events/`) yesterday. The catalog and schema use default settings. Which **two** statements are true?

A. `UNDROP TABLE ops.gold.kpis` can restore the managed table, including its grants.
B. The managed table's data files were deleted immediately at drop time.
C. The external table's data files still exist in the ADLS path.
D. `UNDROP` also restores the managed table's primary and foreign key constraints.

<details><summary>Answer</summary>

**A and C.** A managed table is recoverable within the default 7-day window, and `UNDROP` restores privileges, column spec, and properties. Dropping an external table removes only metadata, so the files remain.
- B: managed data is purged asynchronously **after** the recovery period, not immediately. (The immediate-deletion behavior describes Hive managed tables.)
- D: PK/FK constraints are **not** recovered by `UNDROP` and must be re-created.

</details>

---

### Q5 (choose two)
Storage account `contosolake` has its firewall enabled and allows only trusted Azure services. Security bans client secrets. You need a Unity Catalog storage credential to access it. Which **two** actions should you take?

A. Create a Microsoft Entra ID app registration with a client secret and use it as the storage credential.
B. Create an Access Connector for Azure Databricks with a managed identity, and use its resource ID in the storage credential.
C. Assign the managed identity the **Storage Blob Data Contributor** role on the storage account (or container).
D. Mount the container with `dbutils.fs.mount` using an account key stored in a secret scope.

<details><summary>Answer</summary>

**B and C.** A managed identity through the Access Connector needs no secret and can reach network-restricted storage. It needs a data-plane RBAC role.
- A: uses a secret (banned), and service principals can't use trusted-service access through the firewall.
- D: mounts are legacy and ungoverned, and the account key is a secret.

</details>

---

### Q6
User Ana owns storage credential `lake_cred`. She runs `CREATE EXTERNAL LOCATION raw URL 'abfss://raw@contosolake.dfs.core.windows.net/' WITH (STORAGE CREDENTIAL lake_cred)`, and it fails with a permission error. Later, after that is fixed, engineers granted only `WRITE FILES` on `raw` can't write files. Which pair of fixes is correct?

A. Grant Ana `CREATE EXTERNAL LOCATION` on the metastore. Grant the engineers `READ FILES` in addition to `WRITE FILES`.
B. Grant Ana `CREATE MANAGED STORAGE` on the credential. Grant the engineers `MODIFY` on the location.
C. Make Ana a workspace admin. Grant the engineers `CREATE EXTERNAL TABLE`.
D. Grant Ana `ALL PRIVILEGES` on the credential. Grant the engineers `WRITE VOLUME`.

<details><summary>Answer</summary>

**A.** Creating an external location needs `CREATE EXTERNAL LOCATION` on **both** the metastore and the credential. As the credential owner, Ana has the latter but not the former. `WRITE FILES` requires `READ FILES` as well.
- B: `CREATE MANAGED STORAGE` is an external-location privilege for managed locations. `MODIFY` doesn't apply to external locations.
- C: workspace admin doesn't imply metastore privileges (in non-auto-enabled setups). `CREATE EXTERNAL TABLE` isn't file write access.
- D: `ALL PRIVILEGES` on the credential still lacks the metastore-level privilege. `WRITE VOLUME` applies to volumes, not locations.

</details>

---

### Q7
Vendor CSVs already land in `abfss://raw@contosolake.dfs.core.windows.net/vendor/`. You must replace a legacy DBFS mount with governed access, keep the files in place, and let service principal `ingest-sp` read and write them through a path in Auto Loader. What should you do?

A. `CREATE VOLUME finance.landing.vendor;` then grant `SELECT` on the volume.
B. `CREATE EXTERNAL VOLUME finance.landing.vendor LOCATION 'abfss://raw@.../vendor/';` then grant `READ VOLUME, WRITE VOLUME`, and use `/Volumes/finance/landing/vendor/`.
C. Create an external table over the folder and grant `MODIFY`.
D. Grant `ingest-sp` `READ FILES` and `WRITE FILES` on the storage credential.

<details><summary>Answer</summary>

**B.** An external volume governs existing non-tabular files in place. Access goes through `/Volumes/...` paths using `READ VOLUME`/`WRITE VOLUME`. The creator needs `CREATE EXTERNAL VOLUME` on the external location.
- A: a managed volume lives in managed storage, not the existing path. `SELECT` doesn't exist on volumes.
- C: tables are for tabular, queryable data. They don't govern raw file drops for path-based Auto Loader.
- D: credential-level file privileges are overly broad and bypass path scoping. Grant on locations or volumes instead.

</details>

---

### Q8
A dynamic view uses `CASE WHEN is_member('hr') THEN salary ELSE NULL END`. `hr` is an Entra-synced account group. HR members querying the view through a SQL warehouse see `NULL` salaries. What is the fix?

A. Grant HR members `SELECT` on the underlying table.
B. Replace `is_member('hr')` with `is_account_group_member('hr')`.
C. Replace it with `current_user() = 'hr'`.
D. Switch the warehouse to a dedicated access mode cluster on DBR 13.3.

<details><summary>Answer</summary>

**B.** `is_member()` is the legacy, workspace-scoped check. In UC, account-group membership must be tested with `is_account_group_member()`.
- A: view readers don't need base-table access (the owner's rights resolve it), and that grant would bypass the masking.
- C: `current_user()` returns an email, not a group name.
- D: dedicated mode below DBR 15.4 can't even read dynamic views, and it wouldn't fix the function.

</details>

---

### Q9
Fabrikam has 900 tables across three catalogs. Every column containing a national ID is tagged `pii = 'national_id'`, and new tables arrive weekly. National IDs must be masked for everyone except the `compliance` group, with the least ongoing effort. What should you use?

A. A column mask on each table via `ALTER TABLE … ALTER COLUMN … SET MASK`
B. A dynamic view per table
C. An ABAC policy (`CREATE POLICY … COLUMN MASK … MATCH COLUMNS has_tag_value('pii','national_id')`) attached at catalog level
D. Workspace-catalog binding for the three catalogs

<details><summary>Answer</summary>

**C.** ABAC policies attach at catalog or schema level and apply automatically to every current and new column matching the governed tag. This is Microsoft's recommendation for consistent masking at scale.
- A: works, but needs per-table setup for 900+ tables and every new one. High effort.
- B: a new object per table, and users must change query targets. High effort.
- D: binding controls which workspaces can access a catalog, not column masking.

</details>

---

### Q10 (choose two)
Table `hr.core.employees` has a table-level column mask on `ssn` and a row filter on `dept`. Which **two** operations will fail or are unsupported?

A. `SELECT * FROM hr.core.employees VERSION AS OF 10`
B. `SELECT * FROM hr.core.employees` from a SQL warehouse
C. `CREATE TABLE hr.core.employees_bkp DEEP CLONE hr.core.employees`
D. `ALTER TABLE hr.core.employees ALTER COLUMN ssn DROP MASK` by the table owner

<details><summary>Answer</summary>

**A and C.** Time travel and deep or shallow clones aren't supported on tables with table-level row filters or column masks.
- B: SQL warehouses fully support reading filtered and masked tables.
- D: the owner (or a holder of `MANAGE` + `SELECT`) can drop a mask.

</details>

---

### Q11
Contoso has `dev`, `test`, and `prod` workspaces attached to one metastore. Data engineers have `SELECT` on `prod_catalog` for incident support, but security requires that `prod_catalog` be accessible **only** from the `prod` workspace, even for users with grants. The `test` workspace may read but never write. What should you configure?

A. Revoke `SELECT` from engineers and re-grant it during incidents.
B. Create a second metastore in the same region for prod.
C. Set `prod_catalog` to ISOLATED; bind it to `prod` (read-write) and `test` (read-only).
D. Use a row filter on every prod table with `session_user()` checks.

<details><summary>Answer</summary>

**C.** Workspace-catalog binding overrides grants. Unbound workspaces are denied, and a binding can be read-only.
- A: operationally fragile, and it doesn't block access from `dev` while grants exist.
- B: you can have only one metastore per region.
- D: row filters filter rows by identity. They can't enforce workspace boundaries.

</details>

---

### Q12 (choose two)
A nightly Lakeflow job owned by a developer failed after the developer left the company. The platform team wants production jobs that don't depend on individuals and don't use personal access tokens. Which **two** actions should they take?

A. Set the job's **Run as** to a service principal, after granting yourself the **Service Principal: User** role on it.
B. Authenticate CI/CD and external tools to Databricks with OAuth machine-to-machine (M2M) using the service principal.
C. Transfer job ownership to the workspace-local `admins` group and keep running as the job creator.
D. Generate a PAT for a shared "batch" user account and store it in a secret scope.

<details><summary>Answer</summary>

**A and B.** Running as an SP decouples jobs from people (you need the SP User role to set Run as). OAuth M2M is the recommended non-PAT authentication.
- C: workspace-local groups are legacy and can't receive UC grants. Running as a human creator is the original problem.
- D: a shared human account plus a PAT is exactly the anti-pattern to avoid.

</details>

---

### Q13 (choose two)
Contoso shares `retail.gold.orders` with Fabrikam (a UC-enabled Azure Databricks workspace in another region) using `ALTER SHARE s ADD TABLE retail.gold.orders WITH HISTORY`. Fabrikam can time-travel, but `table_changes()` fails. Contoso must also share with Litware, which uses only pandas. Which **two** actions are correct?

A. Enable `delta.enableChangeDataFeed = true` on `retail.gold.orders` (CDF records changes only from that point on).
B. Re-add the table using `WITH CHANGE DATA FEED` instead of `WITH HISTORY`.
C. Create Litware as `CREATE RECIPIENT litware;` and send them the activation link from `DESCRIBE RECIPIENT`.
D. Create Litware as `CREATE RECIPIENT litware USING ID '<litware sharing identifier>';`

<details><summary>Answer</summary>

**A and C.** `table_changes()` needs CDF enabled on the source table, together with `WITH HISTORY`. A non-Databricks recipient uses token-based open sharing: they download the credential file once from the activation link.
- B: `WITH CHANGE DATA FEED` is deprecated syntax for old runtimes. The missing piece is enabling CDF on the table.
- D: `USING ID` (a sharing identifier from `current_metastore()`) is for Databricks-to-Databricks recipients. Litware has no metastore.

</details>

---

### Q14
An auditor asks three questions:
(1) Who read `finance.gold.payroll` in the last 30 days?
(2) Which downstream tables depend on its `salary` column?
(3) What did the payroll job cost in DBUs last month?
Which sources answer them, in order?

A. `information_schema.table_privileges`; `system.access.audit`; `system.billing.list_prices`
B. `system.access.audit`; `system.access.column_lineage`; `system.billing.usage`
C. `system.access.table_lineage`; `information_schema.columns`; `system.compute.clusters`
D. `system.access.audit`; `system.information_schema.column_tags`; `system.billing.usage`

<details><summary>Answer</summary>

**B.** Audit gives who did what and when. Column lineage gives column-level downstream impact. Billing usage gives DBUs per job (`usage_metadata.job_id`), which you can join to `list_prices` for dollars.
- A: `table_privileges` shows who *can* read (current state), not who *did*. Audit doesn't give dependencies. `list_prices` has no usage.
- C: table lineage isn't access history. `columns` has no dependencies. `clusters` has config, not cost.
- D: column tags give classification, not dependencies.

</details>

---

### Q15
You're migrating `hive_metastore.sales`. `SYNC SCHEMA main.sales FROM hive_metastore.sales DRY RUN` returns `DRY_RUN_SUCCESS` for the external tables, but `DBFS_ROOT_LOCATION` for the managed Delta table `orders`. You want `orders` to become a **UC managed** table. What should you do?

A. Run `SYNC TABLE main.sales.orders AS EXTERNAL FROM hive_metastore.sales.orders`.
B. Run `CREATE TABLE main.sales.orders DEEP CLONE hive_metastore.sales.orders`.
C. Create an HMS-federated foreign catalog. That converts `orders` to managed automatically.
D. Re-run `SYNC` without `DRY RUN`. The status code is only a warning.

<details><summary>Answer</summary>

**B.** Hive managed tables in the DBFS root can't be SYNCed. DEEP CLONE (or CTAS) creates a UC **managed** copy. UCX would do the same thing in bulk.
- A: `AS EXTERNAL` works only for HMS managed tables stored **outside** the DBFS root, and SYNC creates external tables, never managed.
- C: federation governs the table in place as a foreign table. It doesn't convert it to managed.
- D: `DBFS_ROOT_LOCATION` is a blocking condition, not a warning.

</details>

---

## Scoring

**13+/15 = move on. 10–12 = review the misses. <10 = reread these `01-qa.md` sections:**

| Missed questions | Reread |
|---|---|
| Q1–Q3 | §3 Privileges & inheritance, §4 Ownership/MANAGE, §5 Principals |
| Q4 | §6 Managed vs external |
| Q5–Q6 | §7 Storage credentials, §8 External locations |
| Q7 | §9 Volumes |
| Q8–Q10 | §10 Dynamic views, §11 Row filters, §12 Column masks, §13 Tags & ABAC |
| Q11 | §15 Workspace-catalog binding |
| Q12 | §14 Service principals |
| Q13 | §16 Delta Sharing |
| Q14 | §17 Lineage, §18 System tables, §19 information_schema |
| Q15 | §20 HMS federation, §21 UCX, §22 SYNC |
