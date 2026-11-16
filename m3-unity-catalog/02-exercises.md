# Module 3 — Exercises (second pass, guided)

Four realistic tasks of increasing difficulty. Fill each `__(n)__` blank, or fix each numbered `-- BUG (n)`. Hints come right after each code block, and full solutions are inside the collapsible sections. Read §-references in `01-qa.md` if you get stuck.

---

## Exercise 1 — Minimal read access for an analyst group (warm-up, 6 blanks)

**Scenario.** The Entra-synced **account group** `analysts` must read every current **and future** table and view in `sales.gold`, and nothing else. All account users should be able to *discover* (but not read) anything in the `sales` catalog and request access. The analytics lead group `gold-stewards` must be able to grant and revoke access on `sales.gold` without becoming its owner.

```sql
-- 1. Let analysts into the catalog
GRANT __(1)__ ON CATALOG sales TO `analysts`;

-- 2. Let analysts into the schema
GRANT __(2)__ ON SCHEMA sales.gold TO `analysts`;

-- 3. Read every current + future table/view in the schema
GRANT __(3)__ ON SCHEMA sales.gold TO `analysts`;

-- 4. Org-wide discovery without data access
GRANT __(4)__ ON CATALOG sales TO `__(5)__`;

-- 5. Delegate grant management on the schema, no ownership transfer
GRANT __(6)__ ON SCHEMA sales.gold TO `gold-stewards`;

-- verify
SHOW GRANTS `analysts` ON SCHEMA sales.gold;
```

**Hints**
- (1)(2): reading a table needs *three* privileges. Two of them are "usage" privileges on the parents.
- (3): grant it on the schema so future tables are covered.
- (4): metadata-only, grantable at catalog level only, and needs no usage privileges.
- (5): the built-in **account-level** group that contains every user and SP. Not the workspace `users` group.
- (6): owner-like management, but not included in `ALL PRIVILEGES`.

<details><summary>Solution</summary>

```sql
GRANT USE CATALOG ON CATALOG sales TO `analysts`;
GRANT USE SCHEMA ON SCHEMA sales.gold TO `analysts`;
GRANT SELECT ON SCHEMA sales.gold TO `analysts`;
GRANT BROWSE ON CATALOG sales TO `account users`;
GRANT MANAGE ON SCHEMA sales.gold TO `gold-stewards`;
SHOW GRANTS `analysts` ON SCHEMA sales.gold;
```
1. `USE CATALOG`: a prerequisite for touching anything in the catalog. It gives no data access by itself.
2. `USE SCHEMA`: the same for the schema. Without it, `SELECT` alone fails.
3. `SELECT` on the **schema**: inherits to all current and future tables and views. Granting on the catalog would over-grant.
4. `BROWSE`: discover objects and request access without reading data.
5. `account users`: an account-level group. `users` is workspace-local and can't receive UC grants.
6. `MANAGE`: grant, revoke, transfer, and drop without being owner. (`gold-stewards` still needs `USE CATALOG` on `sales` to exercise `MANAGE` on a schema.)

</details>

---

## Exercise 2 — Governed storage on ADLS Gen2 (9 blanks)

**Scenario.** Storage account `contosolake` has a firewall. Security forbids secrets. You must:
- give UC access to container `raw` and container `curated`
- make `curated/finance` the managed storage for a new `finance` catalog
- register existing vendor CSV drops in `raw/vendor/` as governed files that ingestion service principal `ingest-sp` (app ID `5d1e...`) can read and write
- load them with Spark

The storage credential `lake_mi_cred` has already been created in Catalog Explorer.

```text
Azure portal
  a) Create resource: __(1)__   (same region as contosolake; system-assigned MI = On)
  b) contosolake → Access Control (IAM) → assign role __(2)__ to that managed identity
```
```sql
-- c) External locations
CREATE EXTERNAL LOCATION IF NOT EXISTS raw_zone
  __(3)__ 'abfss://raw@contosolake.dfs.core.windows.net/'
  WITH (__(4)__ lake_mi_cred);

CREATE EXTERNAL LOCATION IF NOT EXISTS curated_zone
  URL 'abfss://curated@contosolake.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL lake_mi_cred);

-- d) Catalog with its own managed storage
GRANT __(5)__ ON EXTERNAL LOCATION curated_zone TO `platform-admins`;
CREATE CATALOG finance
  __(6)__ 'abfss://curated@contosolake.dfs.core.windows.net/finance';
CREATE SCHEMA finance.landing;

-- e) Existing files → governed volume
GRANT CREATE EXTERNAL VOLUME ON EXTERNAL LOCATION raw_zone TO `platform-admins`;
CREATE __(7)__ VOLUME finance.landing.vendor_drop
  LOCATION 'abfss://raw@contosolake.dfs.core.windows.net/vendor/';

GRANT USE CATALOG ON CATALOG finance TO `5d1e...`;
GRANT USE SCHEMA ON SCHEMA finance.landing TO `5d1e...`;
GRANT __(8)__ ON VOLUME finance.landing.vendor_drop TO `5d1e...`;
```
```python
# f) Read as the SP (job Run as = ingest-sp)
df = spark.read.option("header", True).csv("__(9)__")
```

**Hints**
- (1): the first-party Azure resource that holds the managed identity for UC.
- (2): the data-plane role that allows read and write on blobs.
- (3)(4): `CREATE EXTERNAL LOCATION name ___ 'url' WITH (___ cred)`.
- (5): the privilege on an external location that allows it to back a catalog or schema's **managed** storage.
- (6): the UC keyword, which differs from the HMS keyword `LOCATION`.
- (7): the files already exist and must stay in place.
- (8): two privileges are needed for read **and** write on a volume.
- (9): the volume path format, not the abfss URI.

<details><summary>Solution</summary>

```text
a) Access Connector for Azure Databricks
b) Storage Blob Data Contributor
```
```sql
CREATE EXTERNAL LOCATION IF NOT EXISTS raw_zone
  URL 'abfss://raw@contosolake.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL lake_mi_cred);
GRANT CREATE MANAGED STORAGE ON EXTERNAL LOCATION curated_zone TO `platform-admins`;
CREATE CATALOG finance
  MANAGED LOCATION 'abfss://curated@contosolake.dfs.core.windows.net/finance';
CREATE EXTERNAL VOLUME finance.landing.vendor_drop
  LOCATION 'abfss://raw@contosolake.dfs.core.windows.net/vendor/';
GRANT READ VOLUME, WRITE VOLUME ON VOLUME finance.landing.vendor_drop TO `5d1e...`;
```
```python
df = spark.read.option("header", True).csv("/Volumes/finance/landing/vendor_drop/")
```
1. **Access Connector for Azure Databricks**: holds the managed identity. No secrets, and it works with firewalled storage (trusted access).
2. **Storage Blob Data Contributor**: read and write on data. (For least privilege: Delegator on the account plus Contributor on the containers.)
3. `URL`: the cloud path of the external location.
4. `STORAGE CREDENTIAL`: links the path to the MI-backed credential. The creator needs `CREATE EXTERNAL LOCATION` on the metastore **and** on the credential.
5. `CREATE MANAGED STORAGE`: required to use the path as a `MANAGED LOCATION`.
6. `MANAGED LOCATION`: the UC syntax for catalog or schema managed storage (overrides the metastore root).
7. `EXTERNAL`: an external volume registered on existing files. Dropping it leaves the files in place.
8. `READ VOLUME, WRITE VOLUME`: volume file privileges. Write access needs read too.
9. `/Volumes/finance/landing/vendor_drop/`: the `/Volumes/<catalog>/<schema>/<volume>/` path. Access is governed by the volume grants, not the location grants.

</details>

---

## Exercise 3 — Fix a broken row filter and column mask (9 bugs)

**Scenario.** On `sales.gold.orders` (columns `order_id BIGINT, region STRING, country STRING, card_number STRING, amount DECIMAL(18,2)`):
- members of account group `global_sales` see all rows; everyone else sees only `region = 'EMEA'`
- `card_number` is visible only to account group `finance`; others see `'****'`
- `analysts` must be able to query the table
- the governance team then wants to share the protected data with a partner, and later remove the row filter

A teammate wrote this. It is full of bugs. Find and fix each `-- BUG (n)`.

```sql
-- Row filter UDF
CREATE OR REPLACE FUNCTION sec.fn.region_filter(region STRING)
RETURNS STRING                                                   -- BUG (1)
RETURN is_member('global_sales') OR region = 'EMEA';             -- BUG (2)

ALTER TABLE sales.gold.orders
  SET ROW FILTER sec.fn.region_filter ON (region, country);      -- BUG (3)

-- Column mask UDF
CREATE OR REPLACE FUNCTION sec.fn.card_mask(card STRING)
RETURN CASE WHEN is_account_group_member('finance') THEN card
            ELSE 0 END;                                          -- BUG (4)

ALTER TABLE sales.gold.orders
  ALTER COLUMN card_number SET ROW FILTER sec.fn.card_mask;      -- BUG (5)

-- Give analysts access
GRANT SELECT ON TABLE sales.gold.orders TO `analysts`;           -- BUG (6)  (query still fails)

-- A data steward (not the table owner) tries to attach the filter and gets PERMISSION_DENIED.
-- They hold: USE CATALOG, USE SCHEMA, EXECUTE on sec.fn.region_filter, SELECT on the table.
-- BUG (7): what privilege is missing?

-- Share protected data with a partner
ALTER SHARE partner_share ADD TABLE sales.gold.orders;           -- BUG (8)

-- Remove the row filter later
DROP FUNCTION sec.fn.region_filter;                              -- BUG (9)
ALTER TABLE sales.gold.orders DROP ROW FILTER;
```

**Hints**
- (1): what must a row filter return?
- (2): which identity function checks **account** groups?
- (3): the `ON (...)` list maps positionally to the UDF's parameters.
- (4): a mask's return type must match the column type.
- (5): row filters are table-level. What does a column get?
- (6): three privileges are needed to read.
- (7): attaching to an existing table requires ownership, or two privileges on the table.
- (8): a table with **table-level** filters or masks can't be shared. What can you share instead?
- (9): the order matters, or the table becomes inaccessible.

<details><summary>Solution</summary>

```sql
CREATE OR REPLACE FUNCTION sec.fn.region_filter(region STRING)
RETURNS BOOLEAN
RETURN is_account_group_member('global_sales') OR region = 'EMEA';

ALTER TABLE sales.gold.orders
  SET ROW FILTER sec.fn.region_filter ON (region);

CREATE OR REPLACE FUNCTION sec.fn.card_mask(card STRING)
RETURN CASE WHEN is_account_group_member('finance') THEN card
            ELSE '****' END;

ALTER TABLE sales.gold.orders
  ALTER COLUMN card_number SET MASK sec.fn.card_mask;

GRANT USE CATALOG ON CATALOG sales TO `analysts`;
GRANT USE SCHEMA ON SCHEMA sales.gold TO `analysts`;
GRANT SELECT ON TABLE sales.gold.orders TO `analysts`;

-- (7) steward needs MANAGE on the table (in addition to SELECT), or must be the owner
GRANT MANAGE ON TABLE sales.gold.orders TO `data-stewards`;

-- (8) don't share the table that carries table-level filter/mask. Options:
--   A) Share a VIEW that encodes the partner's restriction (built over source data
--      that has no table-level filter/mask), e.g.:
CREATE OR REPLACE VIEW sales.share.orders_partner AS
SELECT order_id, region, country, '****' AS card_number, amount
FROM sales.silver.orders            -- illustrative unprotected source the share owner can read
WHERE region = 'EMEA';
ALTER SHARE partner_share ADD VIEW sales.share.orders_partner;
--   B) Replace the table-level filter/mask with an ABAC policy that EXCEPTs the share owner,
--      then ALTER SHARE ... ADD TABLE is allowed.

-- (9) detach first, then drop
ALTER TABLE sales.gold.orders DROP ROW FILTER;
DROP FUNCTION sec.fn.region_filter;
```
1. `RETURNS BOOLEAN`: a row filter must return a boolean. Rows evaluating to false or null are removed.
2. `is_account_group_member`: `is_member` checks workspace-scoped groups and misses account groups in UC.
3. `ON (region)`: the UDF has one parameter, so pass exactly one column (positional mapping).
4. `'****'`: the mask must return `STRING` to match `card_number`. `0` is an INT, a type mismatch.
5. `SET MASK`: columns get masks via `ALTER COLUMN … SET MASK`. `SET ROW FILTER` is table-level.
6. Add `USE CATALOG` and `USE SCHEMA`: `SELECT` alone is insufficient.
7. `MANAGE` on the table: changing filters or masks on an existing table requires ownership, or `MANAGE` **and** `SELECT`.
8. Tables with **table-level** row filters or masks can't be Delta Shared. Either share a view that implements the restriction, or move to ABAC with the share owner exempt, after which the table can be added. *(The source table name in option A is illustrative.)*
9. Run `DROP ROW FILTER` before `DROP FUNCTION`. Otherwise the table references a missing function and becomes inaccessible.

</details>

---

## Exercise 4 — End-to-end Delta Sharing with history and CDF (12 blanks)

**Scenario.** Contoso (provider) shares `retail.gold.orders` with:
- **Fabrikam**, who has a UC-enabled Azure Databricks workspace in another region. They need time travel, streaming, and `table_changes()`. The table must appear to them as `retail.orders`.
- `retail.gold.stock`, filtered so each recipient sees only its own country (Fabrikam = `DE`).
- **Litware**, who uses pandas only, no Databricks.

```sql
-- ===== PROVIDER (Contoso) =====
-- 0. Prereq for table_changes() on the recipient side
ALTER TABLE retail.gold.orders SET TBLPROPERTIES (__(1)__ = true);

-- 1. Share
CREATE SHARE __(2)__ retail_share COMMENT 'Gold retail for partners';

-- 2. Add orders with alias + history
ALTER SHARE retail_share __(3)__ retail.gold.orders
  AS retail.orders __(4)__;

-- 3. Per-recipient partition filtering
ALTER SHARE retail_share ADD TABLE retail.gold.stock
  PARTITION (country = __(5)__.country);

-- 4. Fabrikam ran this in THEIR workspace and emailed the result:
--      SELECT __(6)__();      → 'azure:westeurope:19a84bee-54bc-43a2-87de-023d0ec16016'
CREATE RECIPIENT IF NOT EXISTS fabrikam
  __(7)__ 'azure:westeurope:19a84bee-54bc-43a2-87de-023d0ec16016'
  PROPERTIES ('country' = 'DE');

-- 5. Litware (pandas): token-based open sharing
CREATE RECIPIENT IF NOT EXISTS litware;
DESCRIBE RECIPIENT litware;        -- copy the __(8)__ and send it over a secure channel

-- 6. Entitle
GRANT __(9)__ ON SHARE retail_share TO __(10)__ fabrikam;
GRANT SELECT ON SHARE retail_share TO RECIPIENT litware;

-- ===== RECIPIENT (Fabrikam) =====
CREATE CATALOG IF NOT EXISTS contoso_retail __(11)__ contoso_provider.retail_share;
GRANT USE CATALOG ON CATALOG contoso_retail TO `fabrikam-bi`;

SELECT * FROM contoso_retail.retail.orders VERSION AS OF 42;
SELECT * FROM __(12)__('contoso_retail.retail.orders', 40);
```

**Hints**
- (1): the Delta table property that turns on the change data feed.
- (2): make the statement idempotent.
- (3)(4): the verb for adding a table, and the clause that enables time travel and streaming (the default on DBR 16.2+, but state it explicitly here).
- (5): the function that exposes recipient properties.
- (6): the function the recipient runs to get its sharing identifier.
- (7): the D2D clause that sets auth type `DATABRICKS`.
- (8): what the open-sharing recipient uses to download the credential file (only once).
- (9)(10): the only privilege you can grant on a share, and the principal keyword.
- (11): the clause that mounts a share as a catalog.
- (12): the table-valued function that reads CDF.

<details><summary>Solution</summary>

```sql
ALTER TABLE retail.gold.orders SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
CREATE SHARE IF NOT EXISTS retail_share COMMENT 'Gold retail for partners';
ALTER SHARE retail_share ADD TABLE retail.gold.orders AS retail.orders WITH HISTORY;
ALTER SHARE retail_share ADD TABLE retail.gold.stock PARTITION (country = CURRENT_RECIPIENT().country);
-- recipient ran: SELECT current_metastore();
CREATE RECIPIENT IF NOT EXISTS fabrikam
  USING ID 'azure:westeurope:19a84bee-54bc-43a2-87de-023d0ec16016'
  PROPERTIES ('country' = 'DE');
CREATE RECIPIENT IF NOT EXISTS litware;
DESCRIBE RECIPIENT litware;   -- activation_link
GRANT SELECT ON SHARE retail_share TO RECIPIENT fabrikam;
GRANT SELECT ON SHARE retail_share TO RECIPIENT litware;

CREATE CATALOG IF NOT EXISTS contoso_retail USING SHARE contoso_provider.retail_share;
SELECT * FROM table_changes('contoso_retail.retail.orders', 40);
```
1. `delta.enableChangeDataFeed`: must be on **before** sharing `WITH HISTORY` for recipients to use `table_changes()`. It captures only changes made after it is enabled.
2. `IF NOT EXISTS`: idempotent creation. The creator needs `CREATE SHARE` on the metastore, or must be metastore admin.
3. `ADD TABLE`: the share owner also needs `USE SCHEMA` and `SELECT` on the table, kept for as long as it is shared.
4. `WITH HISTORY`: enables time travel, streaming, and CDF reads, and shares the Delta log for D2D performance. It is the default on DBR 16.2+, but explicit is safer.
5. `CURRENT_RECIPIENT()`: reads the recipient's `PROPERTIES` (here `country`) for per-recipient partition filtering.
6. `current_metastore`: returns the recipient's sharing identifier `<cloud>:<region>:<uuid>`.
7. `USING ID`: Databricks-to-Databricks recipient with auth type `DATABRICKS`. There is no token to manage.
8. `activation_link`: the open-sharing recipient downloads the credential file (bearer token) from it **once**. Rotate the token if it is exposed.
9. `SELECT`: the only privilege grantable on a share.
10. `RECIPIENT`: the syntax is `GRANT SELECT ON SHARE s TO RECIPIENT r`.
11. `USING SHARE`: `CREATE CATALOG … USING SHARE <provider>.<share>` mounts the read-only shared catalog. The recipient then uses normal UC grants on it.
12. `table_changes`: reads the change data feed, which works because CDF was enabled and the table was shared `WITH HISTORY`. Note that the alias `retail.orders` is used, not `gold.orders`.

</details>
