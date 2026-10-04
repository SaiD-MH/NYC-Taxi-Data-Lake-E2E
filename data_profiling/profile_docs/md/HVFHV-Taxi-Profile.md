# Data Profiling — High Volume For-Hire Vehicle (HV-FHV) Dataset

## Phase 0 / Step 3 Deliverable — Light Pass

Dataset: January 2026, ~20.9M rows (largest of the four sources — first real
big-data-scale source in this project).

Per project decision: HV-FHV gets a **light profiling pass**, sufficient to
inform pipeline/model design without duplicating the full Yellow-style
investigation.

---

## 1. Missing Data

| Column | Missing Count | Missing % | Per Data Dictionary |
|---|---:|---:|---|
| `hvfhs_license_num` | 0 | 0.0% | Always present — confirmed |
| `dispatching_base_num` | 0 | 0.0% | Always present — confirmed |
| `originating_base_num` | 5,676,843 | 27.1% | Expected always present — **violated** |
| `request_datetime` | 0 | 0.0% | Always present — confirmed |
| `on_scene_datetime` | 0 | 0.0% | Documented as Accessible-Vehicles-only; fully populated here — see note below |
| `pickup_datetime` | 0 | 0.0% | Always present — confirmed |
| `dropoff_datetime` | 0 | 0.0% | Always present — confirmed |
| `PULocationID` / `DOLocationID` | 0 | 0.0% | Always present — confirmed |
| `trip_miles`, `trip_time` | 0 | 0.0% | Always present — confirmed |
| All fare/fee fields (`base_passenger_fare`, `tolls`, `bcf`, `sales_tax`, `congestion_surcharge`, `airport_fee`, `tips`, `driver_pay`, `cbd_congestion_fee`) | 0 | 0.0% | Always present — confirmed |
| All flag fields (`shared_request_flag`, `shared_match_flag`, `access_a_ride_flag`, `wav_request_flag`, `wav_match_flag`) | 0 | 0.0% | Always present — confirmed |

**Note on `on_scene_datetime`:** dictionary describes this as applicable to
Accessible Vehicles only, but it shows 0% missing across the full dataset —
unlike the FHV `SR_Flag` case, this field appears to be populated regardless
of vehicle type in this data. Not flagged as an issue, just noted as a
documentation/reality mismatch worth remembering (harmless direction — more
data than promised, not less).

**Scope note:** only 2 of the 4 documented HVFHS licensees appear in this
data — `HV0003` (Uber, 72.8%) and `HV0005` (Lyft, 27.2%). Juno (`HV0002`) and
Via (`HV0004`) do not appear in this dataset/time period.

## 2. Ranges / Sanity Checks

| Column | Min | Max | Negative Count | Zero Count |
|---|---:|---:|---:|---:|
| `trip_miles` | 0 | 1,454.43 | 0 | 2,782 |
| `trip_time` (sec) | 0 | 44,998 (~12.5 hrs) | 0 | 737 |
| `base_passenger_fare` | -260.90 | 1,596.10 | 29,965 | 70,530 |
| `driver_pay` | -29.68 | 1,243.91 | 56 | 52,153 |
| `tolls`, `bcf`, `sales_tax`, `congestion_surcharge`, `airport_fee`, `tips`, `cbd_congestion_fee` | 0 | (various, see profiling tool output) | 0 | majority zero — expected (most trips don't incur every fee) |

`PULocationID`/`DOLocationID`: fully populated, range 1–265, consistent with
the TLC zone lookup table, no negatives or zeros.

## 3. Key Findings

### Finding 1 — `originating_base_num` null pattern is fully explained by company (Lyft)

All 5,676,843 missing `originating_base_num` values belong to `HV0005`
(Lyft). Uber (`HV0003`) populates this field on 100% of its trips; Lyft
populates it on 0%. This is a clean, company-level reporting gap — same
shape as the Yellow vendor-6 and FHV base-completeness findings — not random
missingness.

**Assumption / imputation rule (pending business confirmation):** for Lyft
(`HV0005`) trips, **use `dispatching_base_num` as a substitute for the
missing `originating_base_num`**. Rationale: `dispatching_base_num` is
populated 100% of the time, and operationally the dispatching base and
originating base are frequently the same entity. This is a reasonable
working assumption, not a confirmed fact — flagged as an open item for the
business/data owner to validate if the field becomes important downstream.

### Finding 2 — Negative fare/pay amounts (provisional business rule)

`base_passenger_fare` has 29,965 negative values (range down to -260.90);
`driver_pay` has 56 negative values (range down to -29.68). Same pattern
family as Yellow's mirrored negative fare amounts — most likely refunds,
corrections, or reversed transactions rather than random corruption, but not
yet confirmed row-by-row.

**Provisional business rule (pending business clarification):** treat
negative amounts as invalid for now — **quarantine `base_passenger_fare < 0`
or `driver_pay < 0` rows**, tagged with the open question: *"negative
amounts observed in base_passenger_fare / driver_pay — are these legitimate
refunds/reversals, or data errors?"* Revisit once the business defines the
characteristics of a "legitimate" negative.

### Finding 3 — Long-duration/long-distance trips are plausible, not inherently broken

Max `trip_miles` (1,454.43) and max `trip_time` (44,998 sec / ~12.5 hrs) look
extreme in isolation, but unlike Yellow's `trip_distance` bug (where distance
was corrupted while fare stayed normal), these values don't show the same
disconnect — long inter-city trips are a real, if rare, use case for
for-hire vehicles. Rather than flagging by raw distance or duration alone,
the correct validation is **implied average speed**:

```
speed_mph = trip_miles / (trip_time / 3600)
```

A legitimate long trip should average a sane highway speed (roughly
40–70 mph). A broken record will show an implausible speed (e.g. hundreds of
mph, or near-zero speed over hours). This check is more reliable than a flat
distance or duration cutoff and should be applied before deciding on any
exclusion threshold.

**Status:** rule defined, not yet executed against the full dataset — add to
open items.

## 4. Business Rules Summary

| Rule | Status |
|---|---|
| `originating_base_num` not null | Violated for 100% of Lyft trips — handled via imputation assumption (Finding 1) |
| `base_passenger_fare >= 0` | Violated — 29,965 rows; provisional quarantine rule (Finding 2) |
| `driver_pay >= 0` | Violated — 56 rows; provisional quarantine rule (Finding 2) |
| Implied speed sanity (`trip_miles` / `trip_time`) within plausible range | Defined, not yet executed (Finding 3) |
| `dropoff_datetime > pickup_datetime` | To confirm (apply same check as Yellow/FHV) |
| Timestamp sequence: `request_datetime` ≤ `on_scene_datetime` ≤ `pickup_datetime` ≤ `dropoff_datetime` | To confirm |
| Duplicate rows | To confirm |
| Pickup date within stated file month | To confirm |
