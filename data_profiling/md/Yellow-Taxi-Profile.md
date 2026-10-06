# Data Profiling — NYC Taxi — Yellow Dataset (January 2026)



Dataset: Yellow Taxi Trip Records, January 2026
Row count: 3,724,889

---

## 1. Missing Data

| Column | Count | Missing | Missing % |
|---|---:|---:|---:|
| `VendorID` | 3,724,889 | 0 | 0.00% |
| `tpep_pickup_datetime` | 3,724,889 | 0 | 0.00% |
| `tpep_dropoff_datetime` | 3,724,889 | 0 | 0.00% |
| `passenger_count` | 2,636,831 | 1,088,058 | 29.21% |
| `trip_distance` | 3,724,889 | 0 | 0.00% |
| `RatecodeID` | 2,636,831 | 1,088,058 | 29.21% |
| `PULocationID` | 3,724,889 | 0 | 0.00% |
| `DOLocationID` | 3,724,889 | 0 | 0.00% |
| `payment_type` | 3,724,889 | 0 | 0.00% |
| `fare_amount` | 3,724,889 | 0 | 0.00% |
| `extra` | 3,724,889 | 0 | 0.00% |
| `mta_tax` | 3,724,889 | 0 | 0.00% |
| `tip_amount` | 3,724,889 | 0 | 0.00% |
| `tolls_amount` | 3,724,889 | 0 | 0.00% |
| `improvement_surcharge` | 3,724,889 | 0 | 0.00% |
| `total_amount` | 3,724,889 | 0 | 0.00% |
| `congestion_surcharge` | 2,636,831 | 1,088,058 | 29.21% |
| `Airport_fee` | 2,636,831 | 1,088,058 | 29.21% |
| `cbd_congestion_fee` | 3,724,889 | 0 | 0.00% |

## 2. Distinct Values Count

| Column | Count | Distinct |
|---|---:|---:|
| `VendorID` | 3,724,889 | 4 |
| `tpep_pickup_datetime` | 3,724,889 | 1,757,191 |
| `tpep_dropoff_datetime` | 3,724,889 | 1,755,995 |
| `passenger_count` | 2,636,831 | 10 |
| `trip_distance` | 3,724,889 | 4,777 |
| `RatecodeID` | 2,636,831 | 7 |
| `PULocationID` | 3,724,889 | 262 |
| `DOLocationID` | 3,724,889 | 260 |
| `payment_type` | 3,724,889 | 5 |
| `fare_amount` | 3,724,889 | 12,867 |
| `extra` | 3,724,889 | 45 |
| `mta_tax` | 3,724,889 | 6 |
| `tip_amount` | 3,724,889 | 4,326 |
| `tolls_amount` | 3,724,889 | 1,495 |
| `improvement_surcharge` | 3,724,889 | 4 |
| `total_amount` | 3,724,889 | 21,228 |
| `congestion_surcharge` | 2,636,831 | 3 |
| `Airport_fee` | 2,636,831 | 8 |
| `cbd_congestion_fee` | 3,724,889 | 3 |

## 3. Ranges / Percentiles

Full min/max:

| Column | Min | Max |
|---|---:|---:|
| `VendorID` | 1.0 | 7.0 |
| `tpep_pickup_datetime` | 2025-12-31 23:57:29 | 2026-02-01 00:45:01 |
| `tpep_dropoff_datetime` | 2025-12-31 23:57:32 | 2026-02-01 23:35:31 |
| `passenger_count` | 0.0 | 9.0 |
| `trip_distance` | 0.0 | 269,097.48 |
| `RatecodeID` | 1.0 | 99.0 |
| `PULocationID` | 1.0 | 265.0 |
| `DOLocationID` | 1.0 | 265.0 |
| `payment_type` | 0.0 | 4.0 |
| `fare_amount` | -2,555.20 | 2,555.20 |
| `extra` | -7.50 | 17.46 |
| `mta_tax` | -0.50 | 4.75 |
| `tip_amount` | -88.88 | 766.00 |
| `tolls_amount` | -94.50 | 122.22 |
| `improvement_surcharge` | -1.00 | 1.00 |
| `total_amount` | -2,560.20 | 2,560.20 |
| `congestion_surcharge` | -2.50 | 2.50 |
| `Airport_fee` | -1.75 | 26.75 |
| `cbd_congestion_fee` | -0.75 | 0.75 |

Percentiles, key numeric columns (confirms the bulk of the data is sane and the
extreme max values are rare tail outliers, not a systemic problem):

| Percentile | `trip_distance` | `fare_amount` | `total_amount` |
|---|---:|---:|---:|
| min | 0.00 | -2,555.20 | -2,560.20 |
| 25% | 1.00 | 10.00 | 17.00 |
| 50% | 1.81 | 15.60 | 23.05 |
| 75% | 3.73 | 26.10 | 33.83 |
| 95% | 12.45 | 58.50 | 75.25 |
| 99% | 19.54 | 81.40 | 104.75 |
| 99.9% | 29.79 | 145.80 | 175.51 |
| max | 269,097.48 | 2,555.20 | 2,560.20 |

**RatecodeID note:** value `99` is a documented sentinel ("Null/unknown"), not
an out-of-range error — confirmed against the TLC data dictionary. All observed
`RatecodeID` and `payment_type` values fall within documented codes; no rogue
values found.

## 4. Business Rule Violations

| Rule | Broken Records |
|---|---:|
| `passenger_count > 0` (excludes nulls, counted separately) | 14,787 |
| `passenger_count` is null | 1,088,058 |
| `tpep_dropoff_datetime > tpep_pickup_datetime` | 45,070 |
| `trip_distance > 0` | 125,738 |
| `total_amount > 0` | 40,417 |
| `PULocationID` valid | 0 |
| `DOLocationID` valid | 0 |
| Duplicate rows | 0 |


## 5. Data Quality Decision Rules (for the cleaning/transformation layer)

| Field Type | Rule | Treatment |
|---|---|---|
| **Non-optional** (`passenger_count`, `RatecodeID`) | Null, zero, or outside valid range (e.g. `passenger_count > 9`, taxi capacity) | **Error** — route to quarantine |
| **Conditional / optional** (`congestion_surcharge`, `Airport_fee`) | Null | Set to `0` (honest "did not apply" value) |
| **`trip_distance`** | > ~146 miles (99.9th percentile boundary) **and** matches the Vendor 2 / Flex Fare corruption pattern | **Error** — route to quarantine |
| **Timestamps** | `dropoff <= pickup`, or pickup date outside the file's stated month | **Error** — route to quarantine |
| **`total_amount` / `fare_amount`** | <= 0 | **Error** — route to quarantine |

**Error handling pattern:** failing rows are **not dropped silently** and are
**not mixed into the clean/modeled layer**. They are routed to a separate
**quarantine table**, carrying the full original row plus metadata
(`failed_rule`, `processing_timestamp`, pipeline run ID) for further
investigation and reporting. This preserves auditability and avoids losing
data that may later prove useful (e.g. if a quarantined pattern turns out to
be a legitimate edge case rather than an error).

## 6. Relationships & Foreign Keys

- `PULocationID`, `DOLocationID` → foreign keys into `taxi_zone_lookup.csv`
  (TLC Taxi Zone lookup table), providing `Borough`, `Zone`, `service_zone`.
- 262 distinct `PULocationID` and 260 distinct `DOLocationID` observed out of
  265 possible zones in the lookup table.
