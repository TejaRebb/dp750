# M6 — Quiz (FINAL CHECK)

15 scenario questions in DP-750 style. Pick one answer unless the question says **(Choose two)**. Commit to an answer before you open it. Section references (§) point to `01-qa.md`.

---

### Q1
The security team created a new Azure Key Vault with default settings and stored a SQL password in it. A workspace admin with the Contributor role on the vault opens `https://<workspace-url>#secrets/createScope` and tries to create an Azure Key Vault-backed secret scope, but it fails with a permission error on the vault. What should you do first?

- A. Grant the admin the Key Vault Secrets User role and retry.
- B. Change the key vault's permission model from Azure RBAC to **Vault access policy**, then create the scope.
- C. Create a Databricks-backed scope and run `databricks secrets put-secret` with the same value.
- D. Use the URL `#secrets/createscope` in lowercase.

<details><summary>Answer</summary>

**B.** New key vaults default to Azure RBAC, and Key Vault-backed scopes support only the **Vault access policy** model. Creating the scope then adds a Get/List access policy for the AzureDatabricks app.
- A: wrong. RBAC role assignments don't apply, because the scope mechanism uses access policies.
- C: wrong. It works technically, but it duplicates the secret outside Key Vault and defeats central management and rotation.
- D: wrong. The URL **is** case-sensitive, but the correct form is `createScope`, with a capital S.
(§1)
</details>

---

### Q2 (Choose two)
Two teams, Finance and HR, use one workspace. Each team must be able to **read but not modify** only its own secrets, which are managed in Azure Key Vault. Which **two** actions meet the requirement?

- A. Store both teams' secrets in one key vault behind one Key Vault-backed scope, and grant each group `READ` on that scope.
- B. Use **separate key vaults**, each behind its **own** Key Vault-backed scope.
- C. Grant each team's group **`READ`** on its own scope with `databricks secrets put-acl`.
- D. Grant each team's group `MANAGE` on its own scope so they can rotate secrets from Databricks.

<details><summary>Answer</summary>

**B and C.** Secret ACLs are **scope-level**, and a Key Vault-backed scope exposes every secret in its vault. Isolation therefore needs separate vaults and scopes, with READ (read + list) for each team.
- A: wrong. Each group would read **all** secrets in the shared vault.
- D: wrong. MANAGE lets them change ACLs, which isn't "read but not modify". A Key Vault-backed scope is read-only from Databricks anyway, so rotation happens in Key Vault.
(§1)
</details>

---

### Q3
Your operations center monitors all Azure resources in a Log Analytics workspace. It needs an **Azure Monitor** alert within minutes whenever any Databricks job run fails. The workspace is on the Premium tier. What should you configure?

- A. Job email notifications on every job, sent to the operations center.
- B. A diagnostic setting on the Databricks workspace that sends the **`jobs`** category to the Log Analytics workspace, plus a **log search alert rule** on `DatabricksJobs | where ActionName == "runFailed"` with an action group.
- C. A metric alert on the Databricks workspace resource's "Failed jobs" platform metric.
- D. A SQL alert on `system.lakeflow.job_run_timeline` that emails the operations center.

<details><summary>Answer</summary>

**B.** Diagnostic settings stream audit events, including job run outcomes, to Log Analytics, where a KQL **log search alert** with an action group fires. Expect about 15 minutes of log latency.
- A: wrong. It doesn't use Azure Monitor and has to be maintained per job.
- C: wrong. Job run outcomes aren't exposed as Azure platform metrics. They arrive as diagnostic **logs** (`DatabricksJobs`), so you need a log alert.
- D: wrong. It works within Databricks, but it isn't an Azure Monitor alert, and the requirement names Azure Monitor and the existing Log Analytics practice.
(§3)
</details>

---

### Q4 (Choose two)
You plan to stream Azure Databricks diagnostic logs to Log Analytics. Which **two** statements are true?

- A. Diagnostic log delivery requires the **Premium** plan.
- B. Diagnostic logs include Spark executor memory and task duration metrics.
- C. **Account-level** events aren't included in diagnostic logs. Use the `system.access.audit` system table for those.
- D. Diagnostic logs are guaranteed to arrive within 60 seconds.

<details><summary>Answer</summary>

**A and C.**
- B: wrong. Diagnostic logs are audit (who-did-what) events, not Spark metrics. Use compute metrics, the Spark UI, system tables or log delivery for those.
- D: wrong. Delivery is typically within 15 minutes and not guaranteed.
(§3)
</details>

---

### Q5
Two monthly extract DataFrames, `jan` and `feb`, must be combined. `feb` has the same columns as `jan` but in a different order, plus a new column `promo_code`. Rows from `jan` should get NULL for `promo_code`. Which statement is correct?

- A. `jan.union(feb)`
- B. `jan.unionByName(feb, allowMissingColumns=True)`
- C. `jan.unionAll(feb).distinct()`
- D. `jan.join(feb, how="full")`

<details><summary>Answer</summary>

**B.** It matches columns by **name** and fills missing columns with NULL.
- A: wrong. `union` is positional: values land in the wrong columns, and the column count differs, which errors.
- C: wrong. `unionAll` is a positional alias of `union`, and `distinct` removes legitimate duplicates.
- D: wrong. A join combines columns side by side. It doesn't append rows.
(§4)
</details>

---

### Q6
An analyst runs:
```sql
SELECT * FROM sales
PIVOT (sum(amount) FOR quarter IN (1 AS q1, 2 AS q2, 3 AS q3, 4 AS q4));
```
`sales` has the columns `year, region, quarter, amount`. The report should have **one row per year**, but it returns one row per year **and region**. What is the fix?

- A. Add `GROUP BY year` after the PIVOT clause.
- B. Pivot from a subquery that selects only `year, quarter, amount`.
- C. Replace `sum` with `max`.
- D. Use `UNPIVOT` instead.

<details><summary>Answer</summary>

**B.** PIVOT implicitly groups by **every column not referenced** in the aggregate or the `FOR` clause. `region` is still in the input, so remove it before pivoting.
- A: wrong. PIVOT doesn't take an extra GROUP BY this way. The grouping is implicit.
- C: wrong. Changing the aggregate doesn't change the grain.
- D: wrong. UNPIVOT does the opposite (columns to rows).
(§6)
</details>

---

### Q7
This query returns **zero rows**, although some orders clearly have no matching customer:
```sql
SELECT * FROM orders WHERE customer_id NOT IN (SELECT customer_id FROM customers);
```
What is the most likely cause, and the fix?

- A. `customers.customer_id` contains a NULL. Rewrite it as `orders LEFT ANTI JOIN customers ON …` (or `NOT EXISTS`).
- B. The subquery is too large. Add a broadcast hint.
- C. `NOT IN` is unsupported in Databricks SQL. Use `EXCEPT`.
- D. The data types differ. Cast both sides to STRING.

<details><summary>Answer</summary>

**A.** With a NULL in the subquery, `x NOT IN (…, NULL)` is never TRUE, so no rows qualify. An anti join or `NOT EXISTS` handles it correctly.
- B: wrong. Size affects performance, not correctness.
- C: wrong. `NOT IN` is supported. `EXCEPT` compares entire rows and returns only the selected columns, so it isn't a drop-in replacement.
- D: wrong. Type mismatches cast or error. They don't produce this all-or-nothing pattern.
(§5)
</details>

---

### Q8
A developer pulls `main` into their feature branch in a Databricks Git folder and gets a merge conflict in `transforms.py`. They want to keep the teammate's version of the entire file and finish the merge. What should they do in the Git folders UI?

- A. Click **Abort**, then **Reset** the branch to `main`.
- B. Select **Take all incoming changes** for the file, then click **Continue Merge**.
- C. Select **Keep all current changes**, then push.
- D. Delete the Git folder and clone it again.

<details><summary>Answer</summary>

**B.** "Incoming" is the side being merged in (the teammate's changes from `main`). Continue Merge completes the operation.
- A: wrong. Abort undoes the merge, and a hard reset discards local commits.
- C: wrong. That keeps the developer's own version, not the teammate's.
- D: wrong. Destructive and unnecessary.
(§8)
</details>

---

### Q9 (Choose two)
A team builds transformations as Python functions in `src/` and deploys them with a bundle. They want failures caught **before merge** without waiting for a full job run, and they want proof that the whole flow works against realistic data **before production**. Which **two** practices should they adopt?

- A. Run `pytest` unit tests on the `src/` functions, with small in-memory DataFrames, using **Databricks Connect** (or local Spark) in CI on every pull request.
- B. Run unit tests against the production silver tables to get realistic coverage.
- C. After `databricks bundle deploy -t staging`, run integration/end-to-end tests as jobs in the **staging** workspace, with data-quality assertions.
- D. Skip automated tests and rely on UAT by business users in production.

<details><summary>Answer</summary>

**A and C.** Unit tests on PRs give fast feedback. Integration and E2E tests in staging after deploy validate tables, permissions and orchestration with realistic data.
- B: wrong. Never unit-test against production data.
- D: wrong. UAT complements automated testing and belongs in pre-production. It doesn't replace automated tests.
(§9)
</details>

---

### Q10
Business users of an AI/BI Genie space get inconsistent answers to "what was net revenue last quarter?". Sometimes refunds are included, and the fiscal quarter is wrong. You want consistent logic, and you want answers to this recurring question marked as **verified**. What should you do?

- A. Add the question and its expected SQL as a **benchmark**.
- B. Add a **parameterized example SQL query** (with a `:quarter` parameter) that implements net revenue and fiscal quarters, so it becomes a trusted asset. Also add a short text instruction about the fiscal calendar.
- C. Rename the UC column `amount` to `net_revenue`.
- D. Add 40 more tables to the space so Genie has more context.

<details><summary>Answer</summary>

**B.** Parameterized example queries and UC SQL functions are **trusted assets**: when Genie uses one, the answer is marked trusted. Text instructions add global context such as the fiscal year start.
- A: wrong. Benchmarks only **evaluate**. Genie doesn't learn from them.
- C: wrong. Renaming a column doesn't encode refund exclusion or fiscal logic, and it breaks downstream consumers.
- D: wrong. More tables reduce accuracy. Best practice is a focused space (≈5 tables).
(§10)
</details>

---

### Q11
A Lakeflow pipeline (Unity Catalog) must ingest from the Azure Event Hubs event hub `orders` in the **Standard**-tier namespace `evh-sales`, using a SAS connection string stored in a secret scope. The pipeline update fails with `RESTRICTED_STREAMING_OPTION_PERMISSION_ENFORCED`. The options include:
```
kafka.bootstrap.servers = evh-sales.servicebus.windows.net:9093
subscribe = orders
kafka.security.protocol = SASL_SSL
kafka.sasl.mechanism = PLAIN
kafka.sasl.jaas.config = org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="...";
```
What should you change?

- A. Install the `azure-eventhubs-spark` Maven library on the pipeline and use `format("eventhubs")`.
- B. Prefix the login module class with **`kafkashaded.`** (`kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule`).
- C. Change the port to 5671 (AMQP).
- D. Upgrade the namespace to the Premium tier.

<details><summary>Answer</summary>

**B.** Databricks ships shaded Kafka client classes. Unshaded class names in JAAS or callback-handler options raise this error.
- A: wrong. Pipelines don't allow third-party JVM libraries, and that connector isn't in the runtime.
- C: wrong. The Kafka endpoint is 9093. 5671 is AMQP, which the Kafka connector doesn't speak.
- D: wrong. Standard supports the Kafka endpoint (only Basic doesn't), and the error isn't tier-related.
(§11)
</details>

---

### Q12
An existing Azure Data Factory pipeline must trigger an existing **multi-task Lakeflow Job** that runs on **serverless**, passing `run_date` as a job parameter. No personal access tokens may be stored. What should you configure?

- A. A Databricks Notebook activity pointing at the job's first notebook, with `baseParameters`, and an access-token linked service.
- B. A **Databricks Job activity** with `jobId` and `jobParameters`, and a linked service using the data factory's **managed identity**, granted **Contributor** on the Databricks workspace.
- C. A Copy activity with the Azure Databricks Delta Lake connector.
- D. A Lakehouse Federation connection from Databricks to ADF.

<details><summary>Answer</summary>

**B.** The Job activity runs an existing job (serverless supported) and passes `jobParameters`. Managed-identity auth needs the Contributor role on the workspace resource.
- A: wrong. It runs a single notebook rather than the multi-task job, and stores a PAT.
- C: wrong. Copy moves data. It doesn't orchestrate jobs.
- D: wrong. Federation queries external databases. It isn't an orchestration mechanism.
(§12)
</details>

---

### Q13 (Choose two)
A shared all-purpose cluster serves a data engineering team. Requirements:
1. Team members must be able to **start and restart** the cluster when it has terminated, but **not change its configuration or install libraries**.
2. Cluster start time must drop from ~6 minutes, on classic compute, accepting some idle VM cost.

Which **two** actions meet the requirements?

- A. Grant the team's group **CAN RESTART** on the cluster.
- B. Grant the team's group CAN MANAGE on the cluster.
- C. Attach the cluster to an **instance pool** with **min idle instances > 0** and a preloaded Databricks Runtime.
- D. Enable Photon.

<details><summary>Answer</summary>

**A and C.** CAN RESTART allows start, restart and terminate on top of attach. Editing and library installs need CAN MANAGE. A pool's idle instances (no DBUs while idle, VM cost only) cut start time.
- B: wrong. CAN MANAGE allows editing and library installs, which the team must not do.
- D: wrong. Photon speeds query execution, not VM provisioning.
(§13, §15)
</details>

---

### Q14 (Choose two)
You are designing a silver `customer_accounts` table.
- Auditors need **every change to `segment` and `credit_limit` over 7 years**, queryable "as of" any date.
- Finance reports `credit_limit` totals that must **reconcile to the cent** with the ERP.

Which **two** design choices meet these requirements?

- A. Implement an **SCD Type 2** history table with `valid_from`/`valid_to`/`is_current` (fed by CDC or the Change Data Feed).
- B. Rely on Delta time travel, setting `delta.logRetentionDuration` and `delta.deletedFileRetentionDuration` to 2,555 days.
- C. Store `credit_limit` as **`DECIMAL(18,2)`**.
- D. Store `credit_limit` as `DOUBLE`, and run daily data profiling to detect rounding drift.

<details><summary>Answer</summary>

**A and C.** SCD2 gives permanent, plain-SQL "as-of" history that survives VACUUM. DECIMAL is exact.
- B: wrong. Time travel isn't a long-term history mechanism. Multi-year retention bloats storage and the log, and neuters VACUUM.
- D: wrong. DOUBLE causes floating-point drift. Profiling would only detect the problem, not prevent it.
(§16, §18)
</details>

---

### Q15
A job's notebook task wraps its load in `try/except`. On error it writes a row to `ops.etl_errors` and calls `dbutils.notebook.exit("FAILED")`. Last night the load failed, but the job run shows **Succeeded**, so neither the job's failure notification nor the task retries fired. What should you change?

- A. Increase the task's `max_retries` to 3.
- B. After logging the error, **re-raise the exception** (or `raise` a new one) instead of calling `dbutils.notebook.exit`.
- C. Replace `dbutils.notebook.exit("FAILED")` with `dbutils.notebook.exit(1)`.
- D. Add a `run_if: AT_LEAST_ONE_FAILED` cleanup task.

<details><summary>Answer</summary>

**B.** `dbutils.notebook.exit` ends the notebook **successfully** with a string value. Only an uncaught exception fails the task, which then triggers retries, failure notifications and `run_if` logic.
- A: wrong. Retries only happen when the task fails, and it doesn't.
- C: wrong. `exit` takes a string, and any exit value still means success.
- D: wrong. That task never runs, because nothing failed.
(§21)
</details>

---

## Scoring

**13+/15 = move on. 10–12 = reread the sections tied to the questions you missed. <10 = reread `01-qa.md` §1–§3 (Q1–Q4: secret scopes, ACLs, identities, Azure Monitor), §4–§7 (Q5–Q7: set operators, joins, PIVOT/UNPIVOT, grouping), §8–§10 (Q8–Q10: Git folders, testing/deploy, Genie), §11–§12 (Q11–Q12: Event Hubs, ADF), §13–§15 (Q13: pools, libraries, compute permissions/access modes), §16–§20 (Q14: modeling, formats, data types, profiling, constraints) and §21–§22 (Q15: error handling, federation), then redo `02-exercises.md` Ex 2 and Ex 4.**
