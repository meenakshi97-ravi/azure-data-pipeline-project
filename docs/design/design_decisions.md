# Design Decisions: NYC 311 Incremental Lakehouse Pipeline

Status: draft, updated 20 November 2026. **Built** means implemented and tested. **Decided** means backed by API tests in `docs/samples/`. **Planned** means designed but not built.

## 1. Goal

Build a production-style batch pipeline on Azure that ingests a large, constantly changing public dataset and serves it as a clean star schema. The focus is on engineering quality: incremental loading, repeatable runs, data quality handling, and clear documentation of trade-offs.

## 2. Dataset: NYC 311 Service Requests (2020 to present)

**Status: Decided**

- Public dataset from NYC Open Data, exposed through a Socrata API, updated daily.
- Large (tens of millions of rows overall), with real-world messiness, which makes it a good test for incremental loading and data quality checks.
- Domain is easy to explain: noise, parking, rodents, heating complaints, and which agencies resolve them and how fast.
- No personally identifying information about callers.
- The dataset has no explicit license information. The README credits the source.

**Scope control:** a rolling 30-day slice (about 314,516 rows for 4 Oct to 3 Nov 2026) is enough to demonstrate the design and keeps cost low on the Azure free account.

## 3. Watermark column: `:updated_at`

**Status: Decided**

- Tested in `docs/samples/311_updated_at_sample.json`. Socrata exposes a system field `:updated_at`, set whenever a row is added or changed.
- `created_date` is not safe as a watermark: a request created on 31 Oct 2026 was still being updated on 2 Nov 2026 (status and closed date change after creation).
- `:updated_at` captures every change, including old rows.

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

**Observed behaviour (20 Nov 2026):** a first run with the bookmark at the seed date pulled the full 30-day window (about 340,000 rows in about 34 pages). Two runs made within about an hour afterwards each returned a count of 0, and the pipeline handled that cleanly: the paging loop was skipped, and the watermark and audit steps still ran. The source may refresh in one nightly batch, but this is based on a single sample and is to be confirmed with a morning run. A run the next morning returned 128,354 changed rows across 13 pages (12 full pages of 10,000 and a last page of 8,354), and the audit file for that run matched the count.

**Observability (Planned):** each run will also log the number of rows updated outside the window, to show how often bulk updates happen.

## 5. Config and state are kept separate

**Status: Built**

- **Config** (`config/sources.json`, in the repo and in the `config` container): static settings an engineer edits. One entry per source, so adding a source means adding an entry, not building a new pipeline.
- **State** (`config/state/nyc311_watermark.json`): the last successful watermark, written by the pipeline only after a run completes.

**Implementation notes**
- The ADF Lookup activity does not support REST datasets, so the row-count query uses an HTTP dataset, while the paged data copy uses a REST dataset.
- The linked services use the factory's managed identity for storage access, so no keys are stored anywhere.

## 6. Lake layers (medallion)

**Status: Bronze built, silver and gold planned**

| Layer | Container | Contents |
|---|---|---|
| Bronze | `bronze` | Raw API responses, unchanged, partitioned by ingest date and run id. Never edited, so any downstream bug can be replayed. |
| Silver | `silver` | Typed, deduplicated Parquet with data quality flags. |
| Gold | `gold` | Star schema: fact table of requests, with dimensions for agency, problem type, location, and date. |

Bronze path pattern: `bronze/nyc311/ingest_date=YYYY-MM-DD/run_id=<id>/part-<n>.json`

## 7. Schema handling

**Status: Decided**

Evidence in `docs/samples/311_recent_requests.json` and `docs/samples/311_bad_close_dates.json`:
- Every value arrives as a string, including dates, latitude, and longitude.
- Null fields are omitted from the JSON. Some fields appear on only some rows (for example `vehicle_type`, `facility_type`, `due_date`).
- Schema inference would drift between runs.

**Decision:** define the silver schema explicitly, cast types explicitly, and drop system-generated columns (`:@computed_region_*`).

## 8. Duplicate handling

**Status: Planned**

The overlap window means some rows are pulled more than once. Silver keeps the latest version of each `unique_key`, ranked by `:updated_at`. Re-running a window therefore produces the same result (idempotent).

## 9. Data quality rules

**Status: Planned, with real examples from the source**

Real problems found in the source (`docs/samples/311_bad_close_dates.json`):
- Request `54304503` was created on 28 May 2022 but has a `closed_date` of 1 March 2033, years in the future. Its resolution update date is March 2023, so the close date is clearly wrong.
- Request `67996481` was created on 14 Feb 2026 and shows a `closed_date` of 14 Dec 2026, which is after the date it was checked (3 Nov 2026).

| Rule | Handling |
|---|---|
| `closed_date` is in the future | Set to null in silver, keep the original in a `dq_issues` table, add a flag |
| `closed_date` is earlier than `created_date` | Same handling |
| `unique_key` is missing | Reject to a quarantine folder |
| Latitude or longitude missing or outside the NYC area | Keep the row, add a flag |

Flagged rows are kept and marked, not dropped, so data quality can be measured over time.

## 10. Audit logging

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

**Not yet recorded:** `rows_written` and `rows_rejected`. Summing row counts across parallel page copies inside the ingestion pipeline is awkward, so these will be filled in at the silver stage, where bronze and silver row counts can be compared directly.

**Known limitation:** all values are stored as strings. Cast them if the audit files are queried as a table.

## 11. Cost and constraints

- Built on an Azure free account with a 30-day credit window that ends on 29 November 2026. All Azure-side work for this project fits inside that window.
- Compute is kept small: Data Factory orchestration and lightweight transformations. Always-on or hourly-billed services are avoided, or used briefly and switched off.
- Everything is stored as code in this repository, so the portfolio holds up after the Azure trial ends.

## 12. Open questions

-Does the source refresh in one nightly batch? Partly answered: on 21 Nov, two afternoon runs returned 0 changed rows, and a run the next morning returned 128,354 changed rows (13 pages). This supports a nightly batch, but it is a single overnight sample, so it needs a few more mornings to confirm.
- How often does the source touch old rows in bulk? The audit table will show this over time.
- Best way to run silver transformations within the credit window (Data Flow, currently planned).