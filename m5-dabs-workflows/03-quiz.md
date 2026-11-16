# Module 5 Quiz: Bundles + Lakeflow Jobs (final check)

15 scenario questions in DP-750 style. Pick one answer unless the question says **choose two**. Each answer block names the `01-qa.md` section to reread.

---

### Q1
A team deploys a bundle with `databricks bundle deploy -t dev`. The `dev` target has `mode: development`. The job YAML contains `schedule: { quartz_cron_expression: '0 0 6 * * ?', timezone_id: UTC }`. The next morning the job has not run. The team wants it to run on schedule in dev **for this job only**. What should they do?

- A. Add `presets: { trigger_pause_status: UNPAUSED }` to the dev target
- B. Add `pause_status: UNPAUSED` to the job's `schedule`
- C. Change the cron to `0 6 * * *`
- D. Redeploy with `--force`

<details><summary>Answer</summary>

**B.** Development mode pauses all schedules and triggers. An explicit resource setting overrides the mode default.
- A: wrong. When `mode: development` is set, `trigger_pause_status` is **always PAUSED**. It would also affect every job, not just one.
- C: wrong. Quartz needs a seconds field, so the original expression was valid and this one is not.
- D: wrong. `--force` only skips the production git-branch check and has nothing to do with pausing.

Reread: section 4.
</details>

---

### Q2
`databricks.yml` declares `catalog` with `default: dev_cat`, and `targets.prod.variables.catalog: prod_cat`. The CI agent has the environment variable `BUNDLE_VAR_catalog=stg_cat` left over from an earlier step. CI runs `databricks bundle deploy -t prod` with no `--var`. Which catalog do the deployed prod jobs use?

- A. `dev_cat`
- B. `prod_cat`
- C. `stg_cat`
- D. Deployment fails because of conflicting values

<details><summary>Answer</summary>

**C.** The precedence order is `--var` > `BUNDLE_VAR_*` > `variable-overrides.json` > target `variables` > `default`. The env var wins over the target value.
- A: wrong. The default has the lowest precedence.
- B: wrong. Target variables lose to `BUNDLE_VAR_`.
- D: wrong. There is no conflict error. The first match wins.

Reread: section 5.
</details>

---

### Q3 (choose two)
You configure `run_as` for the prod target of a bundle that contains jobs and a Lakeflow pipeline. The bundle is deployed by CI service principal **SP-Deploy**, and it should run as **SP-Run**. Which two statements are true?

- A. `run_as.service_principal_name` must be SP-Run's application ID
- B. `run_as.group_name` can be set at the target level to run everything as a group
- C. Because the deployer differs from `run_as`, only jobs and pipelines are supported in the bundle
- D. Any workspace user can set `run_as.user_name` to any other user's email
- E. `service_principal_name` accepts the SP display name if it is unique

<details><summary>Answer</summary>

**A and C.**
- A: correct. The field takes the **application ID** (GUID), and the deployer needs the `servicePrincipal/user` role on that SP.
- C: correct. When the deploying identity differs from `run_as`, only job and pipeline resources are supported.
- B: wrong. `group_name` is allowed only inside a job or pipeline resource's own `run_as`, not at the top level or target level.
- D: wrong. Non-admins can set `user_name` only to their own email.
- E: wrong. Display names are not accepted.

Reread: section 7.
</details>

---

### Q4
Security requires that the GitHub Actions workflow deploying bundles to production must not store any Databricks credential in GitHub. What should you configure?

- A. A PAT for a service principal stored as a GitHub secret in `DATABRICKS_TOKEN`
- B. OAuth M2M with `DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET` as GitHub secrets
- C. A service principal federation policy, `permissions: id-token: write`, and `DATABRICKS_AUTH_TYPE: github-oidc` with `DATABRICKS_CLIENT_ID`
- D. `databricks auth login` as the first workflow step

<details><summary>Answer</summary>

**C.** Workload identity federation exchanges GitHub's short-lived OIDC token for a Databricks token, so nothing is stored. The client ID alone is not a credential.
- A: wrong. A PAT is a stored secret, and the legacy option.
- B: wrong. The client secret is a stored secret.
- D: wrong. That is U2M login, which is interactive (browser) and unusable in CI.

Reread: section 9.
</details>

---

### Q5
In a job, task `rollback` depends on `load_a` and `load_b`. It must run **only** if at least one of them fails. On a day when both loads succeed, what `run_if` should `rollback` use, and what is its resulting state?

- A. `ALL_FAILED`, state *Upstream failed*
- B. `AT_LEAST_ONE_FAILED`, state *Excluded*
- C. `NONE_FAILED`, state *Skipped*
- D. `AT_LEAST_ONE_FAILED`, state *Upstream failed*

<details><summary>Answer</summary>

**B.** `AT_LEAST_ONE_FAILED` fires when at least one dependency fails. When the condition is unmet, failure-handling conditions mark the task **Excluded** (skipped), and Excluded counts as success downstream.
- A: wrong. `ALL_FAILED` needs **both** loads to fail, and its unmet state is also Excluded.
- C: wrong. `NONE_FAILED` runs on success, which is the opposite of what's wanted.
- D: wrong. *Upstream failed* is the unmet state for `ALL_SUCCESS`, `AT_LEAST_ONE_SUCCESS`, and `NONE_FAILED`, not for the failure-handling conditions.

Reread: section 13.
</details>

---

### Q6
A job has `transform → cleanup`, where `cleanup` uses `run_if: ALL_DONE`. Job-level `email_notifications.on_failure` is configured. Last night `transform` failed and `cleanup` succeeded. On-call got **no** email. Why?

- A. Job-level notifications are never sent for tasks that use `run_if`
- B. The run finished as *Succeeded with failures*, which is treated as success, because all leaf tasks succeeded
- C. `ALL_DONE` suppresses upstream failure states
- D. Email notifications need `webhook_notifications` to be configured as well

<details><summary>Answer</summary>

**B.** Job status is computed from **leaf** tasks. `cleanup` is the only leaf and it succeeded, so the run is *Succeeded with failures*, which triggers `on_success`, not `on_failure`. Fix it with task-level `on_failure` on `transform`, or an `AT_LEAST_ONE_FAILED` leaf that raises.
- A: wrong. No such rule exists.
- C: wrong. `transform` is still recorded as failed. `ALL_DONE` just lets cleanup run.
- D: wrong. Email and system destinations are independent.

Reread: sections 13 and 17.
</details>

---

### Q7
A Python notebook task `profile` computes `null_pct` and must pass it to a downstream **SQL** task, which filters with `WHERE pct > :threshold`. What is the correct approach?

- A. In `profile`, `dbutils.jobs.taskValues.set("null_pct", v)`. In the SQL task, call `dbutils.jobs.taskValues.get`
- B. In `profile`, `dbutils.jobs.taskValues.set(key="null_pct", value=v)`. Set the SQL task parameter `threshold` to `{{tasks.profile.values.null_pct}}`
- C. Set the SQL task parameter to `${tasks.profile.values.null_pct}`
- D. Return the value with `dbutils.notebook.exit(v)` and reference `{{tasks.profile.output}}`

<details><summary>Answer</summary>

**B.** Task values are **set** in Python notebooks and **referenced** in any task type through the run-time dynamic reference `{{tasks.<task>.values.<key>}}`. The SQL task reads it as `:threshold`.
- A: wrong. `taskValues.get` is a Python dbutils call and can't be used from a SQL task.
- C: wrong. `${...}` is a deploy-time bundle substitution, and task values exist only at run time.
- D: wrong. `exit` returns a value to a `dbutils.notebook.run` caller, not to downstream job tasks. `{{tasks.x.output}}` isn't a valid notebook reference (`output.rows`/`first_row` apply to SQL tasks).

Reread: sections 15 and 16.
</details>

---

### Q8
Job DAG: `A → B → C` and `A → D`. Run #44 result: A succeeded, B failed, C was *Upstream failed*, D succeeded. You fix B's notebook, then click **Repair run** without changing the selection. What happens?

- A. A, B, C, and D all rerun, because repair restarts the job graph
- B. B and C rerun using the **updated** notebook, inside run #44
- C. B and C rerun using the notebook version from the original run
- D. Repair is unavailable because the job had a failed task. Use Run now

<details><summary>Answer</summary>

**B.** A repair reruns unsuccessful tasks plus their dependents, using the **current** job and task settings, and records the result in the same run.
- A: wrong. Successful tasks (A, D) are not rerun.
- C: wrong. A repair picks up current settings and code.
- D: wrong. Repair exists precisely for failed or canceled multi-task runs. **Run now** is only required for single-task jobs.

Reread: section 18.
</details>

---

### Q9 (choose two)
A bundle job uses `${var.source_table}` inside a notebook task's configuration. Operators want to process a different table **for a single run** without redeploying. Which two changes or actions accomplish this?

- A. Run `databricks bundle run my_job --var="source_table=other"`
- B. Define a job parameter `source_table` with `default: ${var.source_table}`, and read it in the notebook with `dbutils.widgets.get("source_table")`
- C. After redeploying once with the job parameter defined, run `databricks bundle run my_job --params source_table=other`
- D. Set `BUNDLE_VAR_source_table=other` and run `databricks bundle run my_job`
- E. Edit `variable-overrides.json` and click Run now in the UI

<details><summary>Answer</summary>

**B and C.** Bundle variables resolve at **deploy** time. A job parameter whose default is the variable gives per-environment defaults that can be overridden **per run**, using `--params`, the UI, or the API.
- A: wrong. `--var` on `run` doesn't change the deployed job definition.
- D: wrong. The env var is also deploy-time input, so `run` uses what's deployed.
- E: wrong. The override file is only read by the CLI on validate/deploy. The UI run uses the deployed definition.

Reread: sections 5 and 16.
</details>

---

### Q10
A job must start when **both** `sales.silver.orders` and `sales.silver.customers` have new commits since the last run. It should wait until updates have been quiet for 5 minutes. After you configure a table update trigger, the job never starts, although both tables are updated hourly. The trigger is set to `condition: ALL_UPDATED`. What is the most likely cause?

- A. Table update triggers support only one table
- B. The job's **run-as** principal lacks `SELECT` on the tables (or `USE CATALOG`/`USE SCHEMA`)
- C. `ALL_UPDATED` is invalid. Use a Quartz cron with `?`
- D. File events must be enabled for any table update trigger

<details><summary>Answer</summary>

**B.** The trigger reads table metadata and history **as the run-as identity**. Without SELECT (+ USE CATALOG/SCHEMA) it fails to evaluate and the job doesn't run. Configure failure notifications to catch this.
- A: wrong. Up to 25 tables are supported.
- C: wrong. `ALL_UPDATED`/`ANY_UPDATED` are the valid conditions, and a cron schedule wouldn't express "both tables changed."
- D: wrong. File events are required only to monitor **more than 10** tables. They are recommended otherwise.

Reread: section 19.
</details>

---

### Q11 (choose two)
You need an always-on Structured Streaming job that restarts automatically on failure, with no scheduling logic. You configure `continuous: { pause_status: UNPAUSED }`. Which two statements are true?

- A. Continuous jobs retry failed runs with exponential backoff and have no limit on retry count
- B. You should set `max_retries: -1` and `max_concurrent_runs: 1` on the task, as the recommended pattern
- C. Only one run of the job can be active at a time, and **Run now** is replaced by **Restart run**
- D. A continuous job can also have a Quartz schedule as a fallback
- E. Task dependencies with `run_if` are fully supported in continuous jobs

<details><summary>Answer</summary>

**A and C.**
- A: correct. Consecutive failures trigger exponential backoff, the retry period grows to a system maximum, and retries are unlimited.
- C: correct. There is exactly one active instance, and Restart run is also how the job picks up new config.
- B: wrong. That is the **legacy** pattern that continuous mode replaces. Retry policies aren't supported in continuous jobs; use `task_retry_mode: ON_FAILURE`.
- D: wrong. A continuous trigger takes the place of a schedule.
- E: wrong. Task dependencies can't be used with continuous jobs.

Reread: section 20.
</details>

---

### Q12
An hourly job sometimes runs for 70 minutes. When that happens, the next run shows status **Skipped**. The business requires that no run is lost and that runs never overlap, because they write to the same table with MERGE. What should you configure?

- A. `max_concurrent_runs: 2`
- B. `queue: { enabled: true }` and keep `max_concurrent_runs: 1`
- C. A `RUN_DURATION_SECONDS` health rule at 3600
- D. `timeout_seconds: 3600` on every task

<details><summary>Answer</summary>

**B.** With queueing enabled, a run blocked by the concurrency limit is held for up to 48 h and starts when the previous run finishes, so there is no overlap and nothing is skipped.
- A: wrong. It allows overlap.
- C: wrong. A health rule only warns.
- D: wrong. A timeout would kill the long run, losing work and risking partial writes.

Reread: section 21.
</details>

---

### Q13
A bundle job has a `python_wheel_task` with no cluster settings, intended for **serverless** compute. Deployment and validation fail. The wheel is built by bundle `artifacts`. What is missing?

- A. `existing_cluster_id: ${var.cluster_id}`
- B. A job-level `environments` entry, plus `environment_key` on the task
- C. `libraries: [{ whl: ../dist/*.whl }]` on the job cluster definition
- D. `performance_target: PERFORMANCE_OPTIMIZED`

<details><summary>Answer</summary>

**B.** On serverless, Python wheel, Python script, and dbt tasks **require** `environment_key` pointing to a job `environments` spec (`environment_version`, `dependencies`). Notebook tasks can omit it.
- A: wrong. That targets an all-purpose cluster, which is not serverless and is costly for prod.
- C: wrong. Libraries can't be declared on a (shared) job cluster, and this isn't serverless anyway.
- D: wrong. It is optional and only tunes cost versus speed.

Reread: section 22.
</details>

---

### Q14
A driver notebook reads a control table that lists notebook paths to execute. For each one it must pass a `date` argument, catch failures and retry once, and collect a status string from each child. Lakeflow Jobs For each is not an option because the paths differ per row. Which mechanism fits?

- A. `%run` inside a Python loop
- B. `dbutils.notebook.run(path, 3600, {"date": d})` inside try/except, with each child ending in `dbutils.notebook.exit(status)`
- C. `%run` with widgets, reading `dbutils.widgets.get("status")` afterwards
- D. `dbutils.jobs.taskValues.get(taskKey=path, key="status")`

<details><summary>Answer</summary>

**B.** `dbutils.notebook.run` is a function call, so the path can be dynamic. It runs an ephemeral job, sets the child's widgets from `arguments`, throws `WorkflowException` that you can catch and retry, and returns the child's `exit` string.
- A: wrong. `%run` must be alone in its cell with a literal path, so it can't loop.
- C: wrong. `%run` returns nothing and runs in a shared context.
- D: wrong. Task values pass between job tasks, not between notebooks called by a driver.

Reread: section 23.
</details>

---

### Q15 (choose two)
A team adopts bundles for a workspace where jobs were previously created in the UI. Which two statements are true?

- A. Use `databricks bundle generate job --existing-job-id <id>` and then `databricks bundle deployment bind <key> <id>` to bring an existing job under bundle management without recreating it
- B. Bundles match resources by display name, so a YAML job named like an existing UI job automatically updates it
- C. If a job's YAML is later removed from the bundle, the next `bundle deploy` deletes that job from the workspace
- D. Renaming `bundle.name` and redeploying updates the existing jobs in place
- E. `databricks bundle destroy` deletes only files and keeps jobs

<details><summary>Answer</summary>

**A and C.** Deployments track resources **by ID in a state file**. `generate` + `bind` attaches an existing resource. A deployed resource that disappears from config is deleted.
- B: wrong. Names are not used for correlation. You'd get a second job.
- D: wrong. A new name means a new identity, so new resources are created and the old ones are orphaned.
- E: wrong. `destroy` permanently deletes deployed jobs, pipelines, and artifacts. Use `lifecycle.prevent_destroy` to protect resources.

Reread: section 6.
</details>

---

## Scoring

**13+/15:** move on to the next module.
**10-12:** redo the exercises in `02-exercises.md` that match the sections you missed.
**<10:** reread `01-qa.md` sections **4-7** (modes, variables, identity, run_as), **13-16** (run_if, task values, parameters), and **18-21** (repair, triggers, continuous, concurrency), then retake this quiz.
