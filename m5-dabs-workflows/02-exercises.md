# Module 5 Exercises: Bundles + Lakeflow Jobs (second pass)

Four exercises, easy to hard. Fill each `__(n)__` blank or fix each `# BUG` line. Hints come right after each code block, and full solutions are inside the collapsible block. Reference: `01-qa.md`.

---

## Exercise 1: Complete a `databricks.yml` with dev/prod targets (6 blanks)

Requirements:
- The bundle is called `orders_etl`. Job definitions live in `resources/` as separate files.
- `catalog` defaults to `dev_cat`, and prod uses `prod_cat`.
- `warehouse_id` must resolve to the ID of the warehouse **named** `Shared Serverless` without hard-coding it.
- `dev` is used when no target is given, and gets user-prefixed names plus paused schedules.
- In prod, jobs run as service principal app ID `6b1f2c9e-1111-2222-3333-444455556666`, and deploys are only allowed from `main`.

```yaml
bundle:
  name: orders_etl

__(1)__:
  - resources/*.yml

variables:
  catalog:
    description: Target UC catalog
    default: dev_cat
  warehouse_id:
    __(2)__:
      warehouse: 'Shared Serverless'

targets:
  dev:
    mode: development
    __(3)__: true
    workspace:
      host: https://adb-111.11.azuredatabricks.net
  prod:
    mode: __(4)__
    git:
      branch: main
    workspace:
      host: https://adb-222.22.azuredatabricks.net
      root_path: /Workspace/Users/6b1f2c9e-1111-2222-3333-444455556666/.bundle/${bundle.name}/${bundle.target}
    run_as:
      __(5)__: 6b1f2c9e-1111-2222-3333-444455556666
    variables:
      __(6)__: prod_cat
```

**Hints**
- (1) This top-level key merges other YAML files into the bundle, using globs.
- (2) This variable key resolves a named object to its ID at deploy time.
- (3) This makes `bundle deploy` with no `-t` use this target.
- (4) This mode enables the git-branch validation and forbids `--cluster-id`.
- (5) The `run_as` field for an SP takes the application ID.
- (6) You override the variable value per target by its name.

<details><summary>Solution</summary>

```yaml
bundle:
  name: orders_etl

include:
  - resources/*.yml

variables:
  catalog:
    description: Target UC catalog
    default: dev_cat
  warehouse_id:
    lookup:
      warehouse: 'Shared Serverless'

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://adb-111.11.azuredatabricks.net
  prod:
    mode: production
    git:
      branch: main
    workspace:
      host: https://adb-222.22.azuredatabricks.net
      root_path: /Workspace/Users/6b1f2c9e-1111-2222-3333-444455556666/.bundle/${bundle.name}/${bundle.target}
    run_as:
      service_principal_name: 6b1f2c9e-1111-2222-3333-444455556666
    variables:
      catalog: prod_cat
```

1. `include`: globs relative to `databricks.yml`. Paths inside those files resolve relative to **those** files.
2. `lookup`: resolves the name to an ID at deploy time. It errors if zero or several warehouses have that name.
3. `default`: selected when `-t` and `DATABRICKS_BUNDLE_TARGET` are absent.
4. `production`: validates `git.branch` (override with `--force`), forbids cluster overrides, and validates pipelines are not in development.
5. `service_principal_name`: must be the **application ID**, not the display name.
6. `catalog`: `targets.<t>.variables.<name>` overrides the top-level `default`, but loses to `--var` and `BUNDLE_VAR_catalog`.
</details>

---

## Exercise 2: CLI deploy sequence and CI authentication (7 blanks)

**Part A: local loop.** Alice works locally against the `dev` target, which is the default.

```bash
# authenticate interactively (browser) and save a profile
databricks auth __(1)__ --host https://adb-111.11.azuredatabricks.net

# check syntax and see fully resolved variable values as JSON
databricks bundle validate --output __(2)__

# deploy (default target)
databricks bundle deploy

# run the job whose RESOURCE KEY is orders_job, overriding job parameter run_date
databricks bundle run orders_job __(3)__ run_date=2026-11-01

# re-run only task 'silver' and everything downstream of it, as a NEW run
databricks bundle run orders_job --only __(4)__
```

**Part B: GitHub Actions to prod, no stored secrets.**

```yaml
name: deploy-prod
on:
  push:
    branches: [main]
permissions:
  __(5)__: write
  contents: read
jobs:
  deploy:
    runs-on: ubuntu-latest
    environment: prod
    env:
      DATABRICKS_AUTH_TYPE: __(6)__
      DATABRICKS_HOST: ${{ vars.DATABRICKS_HOST }}
      DATABRICKS_CLIENT_ID: ${{ vars.DATABRICKS_CLIENT_ID }}
    steps:
      - uses: actions/checkout@v4
      - uses: databricks/setup-cli@main
      - run: databricks bundle validate -t prod
      - run: databricks bundle deploy __(7)__ --auto-approve
```

**Hints**
- (1) The CLI subcommand for OAuth user-to-machine login.
- (2) The output format that shows resolved substitutions.
- (3) The `bundle run` flag for job parameters as `k=v` pairs (not `--var`).
- (4) A `+` suffix includes downstream tasks.
- (5) The GitHub token permission that OIDC needs.
- (6) The auth type for GitHub workload identity federation.
- (7) Select the prod target.

<details><summary>Solution</summary>

```bash
databricks auth login --host https://adb-111.11.azuredatabricks.net
databricks bundle validate --output json
databricks bundle deploy
databricks bundle run orders_job --params run_date=2026-11-01
databricks bundle run orders_job --only silver+
```
```yaml
permissions:
  id-token: write
  contents: read
...
      DATABRICKS_AUTH_TYPE: github-oidc
...
      - run: databricks bundle deploy -t prod --auto-approve
```

1. `login`: OAuth U2M, for humans only and never for CI.
2. `json`: `validate --output json` prints the merged config with variables resolved.
3. `--params`: job parameters are run-time values. `--var` would do nothing on `run`, because variables are baked in at deploy. This also errors if the job defines no job-level `parameters`.
4. `silver+`: `+silver` would add **upstream** tasks instead. Note this starts a new run and is not a repair.
5. `id-token`: lets the runner mint the OIDC token.
6. `github-oidc`: with `DATABRICKS_CLIENT_ID` set to the SP application ID and a federation policy on the SP, no secret is needed.
7. `-t prod` (or `--target prod`). Without it the CLI would deploy to the default `dev` target.
</details>

---

## Exercise 3: Fix the broken job DAG (8 bugs)

Intended behavior of `orders_job` (file `resources/orders_job.yml`, notebooks in `../src/`):
1. `ingest` loads data and publishes the loaded row count as task value `row_count`.
2. `has_rows` checks whether `row_count > 0`.
3. `transform` runs **only** if rows were loaded. It receives the count as notebook parameter `rows`.
4. `notify_failure` runs **only** if `ingest` or `transform` failed, and is skipped otherwise.
5. `cleanup` **always** runs after `transform`, whatever happened.
6. `ingest` retries twice, 60 s apart, and a timeout should also be retried.

Notebook `ingest.ipynb`:
```python
cnt = spark.read.table(f"{dbutils.widgets.get('catalog')}.raw.orders_new").count()
dbutils.jobs.taskValues.get(key="row_count", value=cnt)          # BUG A
```

Job YAML:
```yaml
resources:
  jobs:
    orders_job:
      name: orders-job
      tasks:
        - task_key: ingest
          max_retries: 2
          min_retry_interval_millis: 60                          # BUG B
          notebook_task:
            notebook_path: ../src/ingest.ipynb
            base_parameters:
              catalog: ${var.catalog}
          # (missing setting for retry-on-timeout)              # BUG C

        - task_key: has_rows
          depends_on: [ { task_key: ingest } ]
          condition_task:
            op: '>'                                              # BUG D
            left: '{{tasks.ingest.values.row_count}}'
            right: '0'

        - task_key: transform
          depends_on:
            - task_key: has_rows
              outcome: true_branch                               # BUG E
          notebook_task:
            notebook_path: ../src/transform.ipynb
            base_parameters:
              rows: '${tasks.ingest.values.row_count}'           # BUG F

        - task_key: notify_failure
          depends_on: [ { task_key: ingest }, { task_key: transform } ]
          run_if: ALL_FAILED                                     # BUG G
          notebook_task: { notebook_path: ../src/notify.ipynb }

        - task_key: cleanup
          depends_on: [ { task_key: transform } ]
          run_if: NONE_FAILED                                    # BUG H
          notebook_task: { notebook_path: ../src/cleanup.ipynb }
```

**Hints**
- A: Which `taskValues` method *publishes* a value?
- B: Check the unit in the key name.
- C: Timeouts are not retried by default.
- D: YAML uses enum names, not symbols.
- E: The valid outcomes are two quoted booleans.
- F: Is this a deploy-time or run-time value? Pick the matching brace style.
- G: Must fire when *either* upstream fails.
- H: Must run even when `transform` failed.
- Bonus (think, don't fix): why is `${var.catalog}` correct in `ingest`'s `base_parameters` while `${tasks...}` in `transform` is not? And what would bundle validation say if you also added a job-level `parameters:` block?

<details><summary>Solution</summary>

```python
dbutils.jobs.taskValues.set(key="row_count", value=cnt)
```
```yaml
resources:
  jobs:
    orders_job:
      name: orders-job
      tasks:
        - task_key: ingest
          max_retries: 2
          min_retry_interval_millis: 60000
          retry_on_timeout: true
          notebook_task:
            notebook_path: ../src/ingest.ipynb
            base_parameters:
              catalog: ${var.catalog}

        - task_key: has_rows
          depends_on: [ { task_key: ingest } ]
          condition_task:
            op: GREATER_THAN
            left: '{{tasks.ingest.values.row_count}}'
            right: '0'

        - task_key: transform
          depends_on:
            - task_key: has_rows
              outcome: 'true'
          notebook_task:
            notebook_path: ../src/transform.ipynb
            base_parameters:
              rows: '{{tasks.ingest.values.row_count}}'

        - task_key: notify_failure
          depends_on: [ { task_key: ingest }, { task_key: transform } ]
          run_if: AT_LEAST_ONE_FAILED
          notebook_task: { notebook_path: ../src/notify.ipynb }

        - task_key: cleanup
          depends_on: [ { task_key: transform } ]
          run_if: ALL_DONE
          notebook_task: { notebook_path: ../src/cleanup.ipynb }
```

- **A:** `set(key, value)` publishes. `get(taskKey, key, default, debugValue)` reads.
- **B:** The value is in milliseconds, so 60 s = `60000`.
- **C:** `retry_on_timeout: true`. It defaults to false, so a timed-out task would not retry.
- **D:** `GREATER_THAN`. Valid ops are `EQUAL_TO`, `NOT_EQUAL`, `GREATER_THAN`, `GREATER_THAN_OR_EQUAL`, `LESS_THAN`, `LESS_THAN_OR_EQUAL`.
- **E:** `outcome: 'true'`. Only `'true'` or `'false'` are valid, and only on a dependency that points at a `condition_task`.
- **F:** Use `{{tasks.ingest.values.row_count}}`. `${...}` is a deploy-time bundle substitution and would fail or resolve wrongly. Task values exist only at run time.
- **G:** `AT_LEAST_ONE_FAILED`. `ALL_FAILED` needs both upstreams to fail. Also note: if `ingest` fails, `transform` becomes *Upstream failed*, which counts as failed, so ALL_FAILED would actually fire then too. But ALL_FAILED misses the case where `ingest` succeeds and only `transform` fails. When nothing fails, the task is **Excluded** (skipped).
- **H:** `ALL_DONE`. `NONE_FAILED` would mark cleanup *Upstream failed* when transform fails.
- **Bonus:** `${var.catalog}` is a per-environment value known at deploy, so a deploy-time substitution is right. The row count exists only at run time, so it must be `{{...}}`. If you also added job-level `parameters:`, bundle validation would **reject** the job, because job-level `parameters` and task `base_parameters` can't coexist. You'd then move `catalog` to a job parameter (pushed down to notebooks automatically) and read the count in `transform` with `dbutils.jobs.taskValues.get(taskKey="ingest", key="row_count")`. A job parameter default can't reference a task value, because job parameters resolve when the run starts, before any task has set a value.
- **Extra trap:** if `transform` fails, `cleanup` (ALL_DONE) and `notify_failure` both succeed, and both are leaves, so the run shows **"Succeeded with failures"**. That fires `on_success`, not `on_failure`. If on-call relies on the job `on_failure` email, add a task-level `email_notifications.on_failure` on `transform`, or make `notify_failure` raise.
- **Extra:** when `row_count` is 0, `transform` is **Excluded**. `cleanup` (ALL_DONE) still runs, and `notify_failure` is Excluded because nothing failed.
</details>

---

## Exercise 4: Production job with loop, trigger, shared cluster, alerting + repair scenario (12 blanks)

Requirements for `regional_job` in the `prod` target:
- Start automatically when new files land under UC volume `/Volumes/sales/landing/raw/`. Batches arrive in bursts with files ≤30 s apart, so **wait 120 s after the last file**, and never start more than one run per 10 minutes.
- If a run is still active when the next trigger fires, the new run must **wait, not be dropped**, with no overlap.
- `list_regions` (Python notebook) emits a list of regions as task value `regions`.
- `load_region` loops over the regions, **at most 3 in parallel**, passing each region as notebook parameter `region`.
- `list_regions` and the loop share **one** classic cluster defined once, using the complex variable `etl_cluster`.
- `publish` is a Python wheel task on **serverless**, using package `sales_pub` with entry point `main`.
- Email `oncall@contoso.com` on failure. **Warn** (don't kill) when the whole run exceeds 1 hour. Kill `publish` if it runs over 30 minutes.
- Mute alerts for skipped runs.

```yaml
resources:
  jobs:
    regional_job:
      name: regional-job
      max_concurrent_runs: 1
      __(1)__:
        enabled: true
      trigger:
        pause_status: UNPAUSED
        __(2)__:
          url: /Volumes/sales/landing/raw/
          __(3)__: 120
          min_time_between_triggers_seconds: 600
      __(4)__:
        - job_cluster_key: etl
          new_cluster: ${var.etl_cluster}
      environments:
        - environment_key: pub_env
          spec:
            environment_version: '2'
            dependencies: [ ../dist/*.whl ]
      email_notifications:
        on_failure: [ oncall@contoso.com ]
        __(5)__: [ oncall@contoso.com ]
      notification_settings:
        no_alert_for_skipped_runs: true
      health:
        rules:
          - metric: __(6)__
            op: GREATER_THAN
            value: 3600
      tasks:
        - task_key: list_regions
          job_cluster_key: etl
          notebook_task: { notebook_path: ../src/list_regions.ipynb }

        - task_key: load_region
          depends_on: [ { task_key: list_regions } ]
          __(7)__:
            inputs: '__(8)__'
            concurrency: 3
            task:
              task_key: load_region_iter
              job_cluster_key: etl
              notebook_task:
                notebook_path: ../src/load_region.ipynb
                base_parameters:
                  region: '__(9)__'

        - task_key: publish
          depends_on: [ { task_key: load_region } ]
          __(10)__: pub_env
          __(11)__: 1800
          python_wheel_task:
            package_name: sales_pub
            entry_point: __(12)__
```

**Hints**
- (1) The job-level setting that holds runs instead of skipping them (up to 48 h).
- (2) The trigger type for new files in a UC volume or external location.
- (3) The debounce option: the timer resets on each new file.
- (4) The job-level list of reusable cluster specs.
- (5) The email event that matches a health-rule breach.
- (6) The health metric for total run time.
- (7) The loop task type.
- (8) A run-time reference to the upstream task value.
- (9) The per-iteration element reference.
- (10) Serverless wheel tasks *require* this pointer to an `environments` entry.
- (11) The setting that kills the task (unlike a health rule).
- (12) Straight from the requirements.

**Repair scenario (answer in words):** Run #812 got this result: `list_regions` succeeded. In `load_region`, iterations EU and US succeeded and APAC failed. `publish` became *Upstream failed*. You fix a bug in `load_region.ipynb`, redeploy, and click **Repair run**.
- a) Which tasks re-execute?
- b) Which notebook version does APAC use?
- c) Which cluster runs the repaired iterations?
- d) What risk exists if `load_region.ipynb` does `df.write.mode("append")`?
- e) Would `max_concurrent_runs: 1` + queueing block the repair?

<details><summary>Solution</summary>

```yaml
      queue:
        enabled: true
      trigger:
        pause_status: UNPAUSED
        file_arrival:
          url: /Volumes/sales/landing/raw/
          wait_after_last_change_seconds: 120
          min_time_between_triggers_seconds: 600
      job_clusters:
        - job_cluster_key: etl
          new_cluster: ${var.etl_cluster}
      ...
      email_notifications:
        on_failure: [ oncall@contoso.com ]
        on_duration_warning_threshold_exceeded: [ oncall@contoso.com ]
      health:
        rules:
          - metric: RUN_DURATION_SECONDS
            op: GREATER_THAN
            value: 3600
      tasks:
        ...
        - task_key: load_region
          depends_on: [ { task_key: list_regions } ]
          for_each_task:
            inputs: '{{tasks.list_regions.values.regions}}'
            concurrency: 3
            task:
              task_key: load_region_iter
              job_cluster_key: etl
              notebook_task:
                notebook_path: ../src/load_region.ipynb
                base_parameters:
                  region: '{{input}}'
        - task_key: publish
          depends_on: [ { task_key: load_region } ]
          environment_key: pub_env
          timeout_seconds: 1800
          python_wheel_task:
            package_name: sales_pub
            entry_point: main
```

1. `queue`: with `max_concurrent_runs: 1`, extra runs are **queued** for up to 48 h instead of skipped, so there is no overlap and nothing is dropped.
2. `file_arrival`: the URL must be a UC volume or UC external location (root or subpath, recursive). Only new files trigger.
3. `wait_after_last_change_seconds`: debounce for burst batches. `min_time_between_triggers_seconds` is the separate cooldown.
4. `job_clusters`: tasks share the cluster via `job_cluster_key`. Libraries can't be declared on it, so declare them per task.
5. `on_duration_warning_threshold_exceeded`: fires when a `RUN_DURATION_SECONDS` rule is breached.
6. `RUN_DURATION_SECONDS`: this **warns only**, and the run continues.
7. `for_each_task`: runs the nested `task` once per input.
8. `{{tasks.list_regions.values.regions}}`: a run-time task value reference, which must resolve to a JSON array. `${...}` would be wrong because it is resolved at deploy.
9. `{{input}}`: the current element. Use `{{input.<field>}}` for objects or SQL rows.
10. `environment_key`: required for Python wheel, Python script, and dbt tasks on serverless.
11. `timeout_seconds`: kills the task after 1800 s. Add `retry_on_timeout: true` if it should also retry.
12. `main`: the entry point declared in the wheel's `entry_points`.

**Repair answers**
- a) Only the **unsuccessful** parts plus their dependents: the failed `load_region` (APAC iteration) and `publish`. `list_regions` is not rerun. Successful work is preserved.
- b) The **current** (fixed, redeployed) notebook. A repair uses the current job and task settings.
- c) A **new** shared job cluster named `etl_v1`, built from the current `etl` settings.
- d) A rerun task starts from the beginning, and repair doesn't make it idempotent. If APAC appended some rows before failing, a plain append **duplicates** them. Use `MERGE` or `replaceWhere`/overwrite on the region partition.
- e) No. A repair runs *inside the same run* (#812), which is not active anymore, so it is not a new concurrent run. The run must not be in progress when you repair.
</details>
