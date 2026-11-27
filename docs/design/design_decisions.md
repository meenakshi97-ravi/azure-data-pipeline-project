# Design Decisions: NYC 311 Incremental Lakehouse Pipeline

Status: updated 27 November 2026. **Built** means implemented and tested. **Decided** means backed by API tests in `docs/samples/`. **Planned** means designed but not built.

## 1. Goal

Build a production-style batch pipeline on Azure that ingests a large, constantly changing public dataset and serves it as a clean star schema. The focus is on engineering quality: incremental loading, repeatable runs, data quality handling, failure handling, and clear documentation of trade-offs.

## 2. Dataset: NYC 311 Service Requests (2020 to present)

**Status: Decided**

- Public dataset from NYC Open Data, exposed through a Socrata API, updated regularly.
- Large (tens of millions of rows overall), with real-world messiness, which makes it a good test for incremental loading and data quality checks.
- Domain is easy to explain: noise, parking, rodents, heating complaints, and which agencies resolve them and how fast.
- No personally identifying information about callers.
- The dataset has no explicit license information. The README credits the source, and the project is for learning and portfolio use only.

**Scope control:** a rolling 30-day slice (about 314,516 rows for 4 Oct to 3 Nov 2026, and about 385,000 by 25 Nov) is enough to demonstrate the design and keeps cost low on the Azure free account.

## 3. Watermark column: `:updated_at`

**Status: Decided**

- Tested in `docs/samples/311_updated_at_sample.json`. Socrata exposes a system field `:updated_at`, set whenever a row is added or changed.
- `created_date` is not safe as a watermark: a request created on 31 Oct 2026 was still being updated on 2 Nov 2026 (status and closed date change after creation).
- `:updated_at` captures every change, including changes to old rows.

## 4. Rolling-window incremental load

**Status: Built**

**Finding:** on 3 Nov 2026, 558,980 rows had `:updated_at` after 2 Nov 2026, and 433,442 of them (about 78%) were created before the start of the 30-day window. A plain `:updated_at` watermark would pull more than half a million mostly old rows in a single run.

**Decision:** each run pulls only rows where

- `created_date` is within the last `window_days` days (30), and
- `:updated_at` is later than the saved watermark minus `overlap_minutes` (60).

Rows are fetched in pages of 10,000, ordered by `:updated_at, :id` so pages are stable.

**Why:** the work per run stays bounded no matter what the source does to old rows, while status changes on recent requests (open to closed) are still captured.

**Trade-off:** a request older than the window that changes later is intentionally ignored. This is acceptable for analysis of recent service performance. A wider weekly reconciliation run is a possible later extension.

**Watermark value:** after a successful run, the watermark is set to the time the run *started*, not to the highest `:updated_at` seen. Rows changed at the source while a run is in progress could otherwise be skipped. The next run starts 60 minutes before that bookmark, so it re-reads anything changed in that gap, and the silver dedupe makes the repeats harmless. The state file is written only after every page has copied, so a failed run never moves the bookmark.

**Observed source behaviour (20 to 25 Nov 2026).** The first run, with the bookmark at the seed date, pulled the full 30-day window (about 340,000 rows in about 34 pages). Later runs:

| Run | Rows changed in the window since the bookmark | Pages |
|---|---|---|
| 20 Nov, evening: two runs within about an hour of the first | 0 and 0 | 0 |
| 21 Nov | 128,354 | 13 |
| 23 Nov | 139,421 | 14 |
| 25 Nov, daily run | 129,929 | 13 |
| 25 Nov, replay with the bookmark moved back about three days | 159,866 | 16 |

Runs made soon after another run returned nothing, and runs a day or more apart returned over 100,000 rows. That is consistent with the source being refreshed in batches, but only a few runs were observed, so it stays an open question (section 12). Either way the rolling window kept each run bounded, and a zero-row run was handled cleanly: the paging loop was skipped, and the watermark and audit steps still ran.

**Incident: source API returned 503 (23 Nov 2026).** On page 5 of a 14-page run the source answered with status 503 (service unavailable). The run failed without moving the watermark, because the watermark update runs only after every page has copied. No cleanup was needed: pages already written to bronze are harmless duplicates that the silver dedupe removes. Two changes were made in response: the page copy and the count lookup now retry up to 3 times at 30-second intervals, and the paging loop's parallelism was lowered from 4 to 2 to reduce load on the source. The re-run completed and counted the same 139,421 rows as the failed attempt, which confirms the failed run left no gaps or double-counts.

**Observability (Planned):** each run would also log the number of rows updated outside the window, to show how often bulk updates happen. This was not built.

## 5. Config and state are kept separate

**Status: Built**

- **Config** (`config/sources.json`, in the repo and in the `config` container): static settings an engineer edits. One entry per source, so adding a source means adding an entry, not building a new pipeline.
- **State** (`config/state/nyc311_watermark.json`): the last successful watermark, written by the pipeline only after a run completes.

**Implementation notes**

- The ADF Lookup activity does not support REST datasets, so the row-count query uses an HTTP dataset, while the paged data copy uses a REST dataset.
- The storage linked service uses the factory's managed identity, so no keys are stored anywhere. The two API linked services (REST and HTTP) use anonymous access, because the source is public.

## 6. Lake layers (medallion)

**Status: Bronze, silver and gold built (v1)**

| Layer | Container | Contents |
|---|---|---|
| Bronze | `bronze` | Raw API pages, unchanged, by ingest date and run id. Never edited, so any bug can be replayed. |
| Silver | `silver` | Typed, deduplicated Parquet with data quality flags, one row per request (49 columns). |
| Gold | `gold` | Star schema: a fact table and four dimensions. |

Bronze path pattern: `bronze/nyc311/ingest_date=YYYY-MM-DD/run_id=<id>/part-<n>.json`

### Gold layer: star schema

Gold is built by the data flow `df_nyc311_gold`, which reads silver and writes one fact table and four dimension tables as Parquet under `gold/nyc311/`. Every output folder is cleared and rebuilt on each run, so the result depends only on the current silver data.

| Table | Grain | Key | Rows (first gold run) |
|---|---|---|---|
| `fact_service_request` | one row per request | `unique_key` | 364,526 |
| `dim_agency` | one row per agency | `agency_key` | 14 |
| `dim_problem` | one row per complaint type and descriptor | `problem_key` | 867 |
| `dim_location` | one row per borough, zip code and city | `location_key` | 569 |
| `dim_date` | one row per day on which requests were created | `date_key` (yyyymmdd) | 33 |

**Reconciliation:** the fact table has exactly as many rows as silver had when gold ran. This held for the first gold run (364,526) and for the full daily run on 25 Nov 2026, where silver and the fact table both held 384,465 rows. Gold applies no filters and no joins to the fact table, so any difference between the two would indicate a bug. The Power BI model loaded from the exported files also contains 384,465 fact rows.

### Decisions

**Hashed keys instead of generated numbers.** Dimension keys are `md5` hashes of the natural key (for example `md5(complaint_type, descriptor)`). A key made from the data is the same on every run, so the fact table and its dimensions stay linked even though gold is rebuilt from scratch each time. Generated sequence numbers would change on every rebuild and break those links. `md5` was chosen over a shorter 32-bit hash because the collision risk of a 32-bit hash, small as it is, is avoidable. **Trade-off:** keys are 32-character strings, which take more space and join more slowly than integers. At this size that is not a problem.

**Missing values become `UNKNOWN` before hashing.** A blank agency, borough, zip code or city is replaced with `UNKNOWN` first, so every fact row gets a valid dimension key and no key is ever null. `incident_zip` is read as a number from silver and is converted to text first, because zip codes are identifiers, not quantities.

**Measures and flags carried into the fact table.** `resolution_hours` is the hours between `created_date` and `closed_date`, and is null for open requests. It was checked by hand against ten real rows, including a 25-minute request and a request open for about 368 hours. The three data quality flags from silver (`dq_future_closed`, `dq_closed_before_created`, `dq_bad_coords`) are kept in the fact table, so analysts can filter out suspect rows or measure how many there are.

**Date keys.** `created_date_key` and `closed_date_key` are integers in `yyyyMMdd` form. `closed_date_key` is null for open requests.

**Small output files.** Dimension sinks write a single file each, and the fact table is written as five files. The data flow's default of 200 partitions produced hundreds of tiny files in earlier runs.

### Known limitations (v1)

- **`dim_date` covers only the days on which requests were created.** A request closed on a day with no new requests would have no date dimension row for its `closed_date_key`. At roughly 10,000 requests a day this is unlikely, but it has not been tested. A complete calendar table is the proper fix.
- **No history on dimensions.** Each dimension holds the current attributes only. If an agency name changes, the old name is overwritten. Slowly changing dimensions (type 2) are a planned extension and were not built.
- **Gold is rebuilt in full on every run.** This is simple and correct at the current scale, but the cost grows with the size of silver.
- **`dim_location` is built from text fields,** not from verified addresses, so spelling variations in a borough or city name would produce separate rows.

## 7. Schema handling

**Status: Decided**

Evidence in `docs/samples/311_recent_requests.json` and `docs/samples/311_bad_close_dates.json`:

- Every value arrives as a string, including dates, latitude, and longitude.
- Null fields are omitted from the JSON. Some fields appear on only some rows (for example `vehicle_type`, `facility_type`, `due_date`).
- Schema inference would drift between runs.

**Decision:** define the silver schema explicitly, cast types explicitly, and drop system-generated columns (`:@computed_region_*`).

## 8. Duplicate handling

**Status: Built**

The overlap window and repeated runs mean the same request is landed in bronze more than once. Silver keeps the latest version of each `unique_key`, ranked by `updated_at` (a window function with `rowNumber()`, keeping row number 1). Re-running therefore produces the same silver result (idempotent).

| Run date | Read from bronze | Written to silver | Duplicates removed |
|---|---|---|---|
| 21 Nov 2026 | 797,254 | 342,455 | 454,799 (about 57%) |
| 23 Nov 2026 | not recorded | 364,452 | not recorded |
| 25 Nov 2026 | not recorded | 384,465 | not recorded |

**Replay test (25 Nov 2026):** the watermark was moved back about three days by hand and the full daily pipeline was re-run.

| Measure | Value |
|---|---|
| Silver rows before the replay | 384,465 |
| Rows re-pulled from the API | 159,866 |
| Silver rows after the replay | 384,465 |
| Fact table rows after the replay | 384,465 |

About 160,000 rows were read again and landed in bronze a second time, and silver and gold were unchanged. Re-running a window cannot inflate the data, because silver keeps one row per `unique_key`.

Evidence: `docs/screenshots/31a_replay_test.png` to `31c_replay_test.png`.

Silver has 49 columns: 45 from the first build, plus `closed_date_original` and the three `dq_` flags added by the quality step. Seven source columns were dropped: the system columns (`:created_at`, `:version`, the `:@computed_region_*` columns) and the nested `location` object, whose coordinates are already in `latitude` and `longitude`.

**Known v1 limitation:** silver is rebuilt from all of bronze on every run (the sink clears its folder), and bronze grows with each run. This is simple and correct at the current scale, but reads will slow as bronze grows.

**Planned improvement:** read only the new run folders and merge them into silver by `unique_key`.

## 9. Data quality rules

**Status: Built**

Real problems found in the source (`docs/samples/311_bad_close_dates.json`):

- Request `54304503` was created on 28 May 2022 but has a `closed_date` of 1 March 2033, years in the future. Its resolution update date is March 2023, so the close date is clearly wrong.
- Request `67996481` was created on 14 Feb 2026 and shows a `closed_date` of 14 Dec 2026, which is after the date it was checked (3 Nov 2026).

| Rule | Handling |
|---|---|
| `unique_key` is missing | Rejected to a quarantine folder |
| `closed_date` is in the future | Set to null in silver; the original is kept in `closed_date_original`; row flagged |
| `closed_date` is earlier than `created_date` | Same handling |
| Latitude or longitude missing or outside the NYC area | Row kept and flagged (`dq_bad_coords`) |

Flagged rows are kept and marked, not dropped, so data quality can be measured over time. Every flagged row is also written to an issue log (`silver/dq_issues/nyc311`).

**Results (21 Nov 2026 run, 342,455 silver rows):**

- **Quarantine:** 0 rows. Every row had a `unique_key`, so the rule exists as a safeguard that found nothing to catch.
- **Issue log:** 5,494 rows (about 1.6% of silver) were flagged by at least one rule. The flagged rows inspected in the preview were failing the coordinate check. A count per rule was not broken out, so it is not known how many rows, if any, were flagged by the two date rules in this window.

The two bad-date examples above came from older requests outside the 30-day window, so those rules are tested against known bad data but have not been confirmed to fire in the live window.

**Output handling:** the quarantine and issue-log sinks write a single file each, and both clear their folders on every run, so they always reflect the latest run only.

## 10. Audit logging and failure handling

**Status: Built (v1)**

Each pipeline run writes one small JSON file, named after the run id, to `config/audit/nyc311/<run_id>.json`. It is written only after the run's watermark update succeeds.

| Field | Meaning |
|---|---|
| `run_id` | Data Factory pipeline run id |
| `source_id` | Which source was loaded (`nyc311`) |
| `started_at` | When the run started (UTC) |
| `watermark_used` | The bookmark the run started from |
| `rows_expected` | Row count returned by the count query for the window |
| `pages` | Number of pages the run fetched |
| `status` | Run outcome |

Example from a run where nothing had changed at the source:

```json
{"run_id":"10afd9f3-5818-414a-b899-d571f3c3fac5","source_id":"nyc311","started_at":"2026-11-21T01:00:36.5837805Z","watermark_used":"2026-11-21T00:22:33.647Z","rows_expected":"0","pages":"0","status":"Succeeded"}
```

Consecutive audit files show the bookmark moving forward: each run's `watermark_used` is the previous run's start time.

**Not yet recorded in the audit file:** `rows_written` and `rows_rejected`. Row counts are read from the data flow run diagnostics instead, and are recorded in this document.

### Failure handling

The daily pipeline `poc_nyc311_daily` runs the ingest, silver and gold pipelines in order, each as an Execute Pipeline activity that waits for completion. Each stage has a failure path:

1. A Copy activity writes a `Failed` audit file to `config/audit/nyc311/<run_id>_failed_<stage>.json`, containing `run_id`, `source_id`, `started_at`, `status`, `failed_step` and `error_message`.
2. A Fail activity then fails the whole run. Without it, a handled failure would show as a succeeded run and hide the problem.
3. Later stages do not start.

**Tested:** the ingest failure path, by pointing the watermark dataset at a file name that did not exist. The ingest stage failed, the failure audit file was written with the step and error message, the Fail activity failed the run, and silver and gold did not start. The silver and gold failure paths use the same pattern but were not tested separately.

**Alerting:** an Azure Monitor metric alert on failed pipeline runs is attached to an action group that sends an email. In testing, the email arrived about 25 minutes after the failed run (the run failed at about 04:32 UTC on 25 Nov, and the alert fired at 04:57 UTC). Metric alerts run on a schedule and the metric itself lags, so the alert is a backstop and the failure audit file is the immediate record. The alert rule costs about $0.10 per month.

**Limitation:** the failure audit activities read `ds_config_json` as a carrier file, so a missing config file would break both the run and its failure record. The fix would be a dedicated carrier file.

## 11. Cost and constraints

- Built on an Azure free account with a credit window that ended on 29 November 2026. All Azure-side work for this project fit inside that window.
- Compute is kept small: Data Factory orchestration and short data flow runs. The data flow debug session was switched off after use, and the daily trigger was stopped after it had been shown to work.
- Usage stayed far below the free credit: about CA$2.97 of the roughly CA$280 credit, as shown on the Azure home page.
- Everything is stored as code in this repository, so the portfolio holds up after the Azure trial ends. The gold files, the issue log, the audit files and a bronze sample were exported before the resources were deleted.

## 12. Open questions

- Does the source refresh in batches? Runs soon after another run returned nothing, and runs a day or more apart returned over 100,000 changed rows, but only a few runs were observed.
- How often does the source touch old rows in bulk? The `rows_updated_outside_window` count was planned but not built.
- Silver is rebuilt from all of bronze every run. Reading only the new bronze folders and merging by `unique_key` would scale better.
- SCD Type 2 on a dimension is planned but not built.
- How many rows does each data quality rule flag? Only the combined total (5,494) was recorded.

## 13. Orchestration and scheduling

**Status: Built**

- **Master pipeline:** `poc_nyc311_daily` runs `poc_nyc311_ingest`, then `poc_nyc311_silver`, then `poc_nyc311_gold`. Each step starts only if the previous one succeeded.
- **Schedule:** a daily schedule trigger set in Eastern Time. It was published, fired at its scheduled time, and ran the chain, then was stopped so it would not keep using the free credit.
- **Parameters and configuration:** the source URL path, window length, overlap, and page size come from `config/sources.json`, and the bookmark comes from `config/state/nyc311_watermark.json`, so changing the load behaviour needs no pipeline edit.
- **Security:** Data Factory reaches the storage account through its managed identity, so no keys or passwords are stored anywhere.
- **Source control:** the factory is connected to this GitHub repository, so pipeline, dataset and linked service definitions are stored as JSON under `adf/`.
- **Cost control:** compute is limited to Data Factory orchestration and short data flow runs.

## 14. Infrastructure as code and CI

**Status: Written and validated, not deployed**

`infra/main.bicep` defines the lake and the factory: a storage account with hierarchical namespace (Data Lake Gen2), LRS replication and no public blob access, the four containers (`bronze`, `silver`, `gold`, `config`), a Data Factory with a system-assigned managed identity, and the role assignment that lets the factory read and write storage without keys. Names are parameters in `infra/main.parameters.json`.

**Validation:** the file compiles, and it was checked with `az deployment group what-if` against the live resource group (output saved as `docs/screenshots/34_bicep_whatif.png`). A GitHub Actions workflow (`.github/workflows/validate-infra.yml`) builds the Bicep file and checks that every JSON file under `infra/`, `config/` and `adf/` is valid, on every push (evidence: `docs/screenshots/35_github_actions.png`). The workflow does not deploy, because deployment would need Azure credentials.

**Not covered by the Bicep file:** the Git connection on the factory, the linked services, datasets, pipelines and data flows (these are stored as JSON under `adf/` and synchronised through the factory's Git integration), the schedule trigger, and the Azure Monitor alert.

**Why not deployed:** the live factory has Git connected and its role assignment was created by hand, so a deployment would try to recreate the assignment and could reset the factory's Git settings.

## 15. Dashboard

**Status: Built**

A one-page Power BI report built on the exported gold Parquet files (not on Azure, so it works after the resources were deleted). The model is a star schema with one many-to-one relationship from the fact table to each dimension. The fact table loads 384,465 rows, matching the pipeline. Measures and loading steps are documented in `dashboard/measures.md`.

Visuals: three cards (total requests, median resolution hours, open requests), requests by agency, top 10 complaint types, requests created per day, median hours to resolve by agency, and slicers for borough and agency. The median is used instead of the mean because a few requests stay open for hundreds of hours and pull an average up. Resolution time covers closed requests only.
