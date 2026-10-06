# Data Profiling — FHV (For-Hire Vehicle) Base Dataset


## 1. Missing Data

| Column | Missing Count | Per Data Dictionary |
|---|---:|---|
| `dispatching_base_num` | 0 | Documented as always present — **confirmed** |
| `pickup_datetime` | 0 | Documented as always present — **confirmed** |
| `dropOff_datetime` | 0 | Documented as always present — **confirmed** |
| `PUlocationID` | 1,646,462 | Documented as always present — **violated** |
| `DOlocationID` | 216,251 | Documented as always present — **violated** |
| `SR_Flag` | 1,941,722 | Documented as null for non-shared rides — **expected, not a violation** |
| `Affiliated_base_number` | 141,985 | Documented as always present ("must be provided even if same as dispatching base") — **violated** |

## 2. Key Findings

### Finding 1 — Data dictionary's "mandatory" fields are not reliable

The dictionary explicitly states `PUlocationID`, `DOlocationID`, and
`Affiliated_base_number` must always be populated. In practice:
- `PUlocationID` missing in **1,646,462** rows
- `DOlocationID` missing in **216,251** rows
- `Affiliated_base_number` missing in **141,985** rows

`dispatching_base_num` and both timestamp fields hold up as fully populated,
matching documentation. `SR_Flag` nulls are expected/documented behavior
(null = non-shared ride) and are **not** a data quality issue.

### Finding 2 — Missing location data is distributed across bases, not concentrated

Unlike Yellow Taxi (where null patterns concentrated by `VendorID`, pointing
to specific source-system gaps), missing `PUlocationID` in FHV is **spread
across many different `dispatching_base_num` values**, with no single
dominant offender:

| dispatching_base_num | Missing PUlocationID Count |
|---|---:|
| B01338 | 113,690 |
| B01717 | 113,158 |
| B00856 | 91,041 |
| B02550 | 89,411 |
| B01536 | 89,194 |
| B00412 | 70,804 |
| B02437 | 70,078 |
| B00937 | 68,072 |
| B01312 | 50,401 |
| B01899 | 39,567 |

**Implication:** this cannot be isolated and handled via a single "bad base"
exclusion rule the way Vendor 6 was handled for Yellow. The gap appears to be
a broader, systemic limitation in how FHV bases report location data —
there is no clean single root cause to filter on. A meaningful share of FHV
trips (~1.6M rows) will simply have reduced analytical value for any
location-based ("where") business question.

### Finding 3 — No fare/revenue data in this dataset (grain/scope limitation)

The FHV base dataset contains **no fare, fee, or payment fields** of any
kind (no `fare_amount`, `total_amount`, tips, tolls, surcharges — nothing).

**Implication:** this dataset can support "where/when" demand-pattern
questions, but **cannot support any revenue, pricing, or payment-related
business question**. This is a hard scope boundary for this source,
independent of data quality — it's a schema limitation, not a dirty-data
problem.

## 3. Business Rules (reused from Yellow/Green methodology)

| Rule | Status |
|---|---|
| `dropOff_datetime > pickup_datetime` | To confirm (apply same check as Yellow) |
| Pickup date falls within stated file month | To confirm (apply same check as Yellow) |
| `dispatching_base_num` must not be null | **Holds** — 0 violations |
| `Affiliated_base_number` must not be null | **Violated** — 141,985 broken records |
| `PUlocationID` must not be null | **Violated** — 1,646,462 broken records |
| `DOlocationID` must not be null | **Violated** — 216,251 broken records |
| Duplicate rows | To confirm (apply same check as Yellow) |

