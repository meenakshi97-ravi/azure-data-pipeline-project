# Dashboard: NYC 311 service requests

Power BI Desktop report built on the exported gold layer. The `.pbix` file is not stored in this repository, because it contains the data. A screenshot is at `docs/screenshots/36_dashboard.png`.

## Data loading

Each table is loaded from the exported gold Parquet files (not from Azure), by combining every `.parquet` file in the table's folder. The folder also holds a `_SUCCESS` marker, which is skipped by filtering on the file extension.

```
let
  Files = Folder.Files("<path to export>\Gold\nyc311\fact_service_request"),
  Parquets = Table.SelectRows(Files, each [Extension] = ".parquet"),
  Parsed = Table.AddColumn(Parquets, "t", each Parquet.Document([Content])),
  Combined = Table.Combine(Parsed[t])
in
  Combined
```

The same query, with a different folder, loads `dim_agency`, `dim_problem`, `dim_location` and `dim_date`.

**Reconciliation:** the fact table loads 384,465 rows, the same as the gold layer's row count in the pipeline.

## Model

A star schema. Each relationship is many-to-one, from the fact table to a dimension, with a single filter direction.

| Fact column | Dimension column |
|---|---|
| `agency_key` | `dim_agency[agency_key]` |
| `problem_key` | `dim_problem[problem_key]` |
| `location_key` | `dim_location[location_key]` |
| `created_date_key` | `dim_date[date_key]` |

`closed_date_key` is not related to `dim_date`. A second relationship to the same table would have to be inactive, and nothing on the report needs it.

## Measures

| Measure | Definition |
|---|---|
| Total Requests | `COUNTROWS(fact_service_request)` |
| Median Resolution Hours | `MEDIAN(fact_service_request[resolution_hours])` |
| Open Requests | `CALCULATE(COUNTROWS(fact_service_request), ISBLANK(fact_service_request[closed_date]))` |

```
Total Requests = COUNTROWS(fact_service_request)

Median Resolution Hours = MEDIAN(fact_service_request[resolution_hours])

Open Requests = CALCULATE(COUNTROWS(fact_service_request), ISBLANK(fact_service_request[closed_date]))
```

## Reading the numbers

- **The median is used instead of the mean** because a few requests stay open for hundreds of hours and pull an average up. The median describes a typical request.
- **Resolution time covers closed requests only.** Open requests have no `resolution_hours`, so they are left out of the median.
- **"Open Requests"** counts requests with no close date. A close date that was set to null by the data quality rules (a future date, or earlier than the creation date) also appears as open.
- **The first and last days** in the daily chart are partial days, because the window starts and ends mid-day, so they look lower.

## Visuals

Three cards (Total Requests, Median Resolution Hours, Open Requests), requests by agency, top 10 complaint types, requests created per day, median hours to resolve by agency, and slicers for borough and agency.