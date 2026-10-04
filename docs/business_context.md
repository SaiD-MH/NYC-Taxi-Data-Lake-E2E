# Business Context — NYC Taxi Data Engineering Project

## Phase 0 / Step 1 Deliverable

---

## 1. Persona / Stakeholder

**Operations / Fleet Efficiency Team**

This team is responsible for understanding taxi demand patterns across the city and
identifying where supply (available vehicles) is misaligned with demand (riders looking
for trips). Their goal is to improve driver positioning and reduce wasted/idle time in
the fleet.

---

## 2. Business Questions

1. **Where is demand highest?**
   Which pickup zones/boroughs generate the most trips, ranked as a distribution
   (not just a single "winner") so ops can act on it (e.g. driver staffing,
   positioning recommendations).

2. **When is demand highest?**
   How does trip volume vary by hour of day (and potentially day of week)? Again,
   as a distribution/heatmap (zone × hour), not a single max value.

3. **Where are drivers wasting time empty?** *(see redefinition below — this
   question could not be answered as originally asked)*

---

## 3. Finding: The "Idle Time" Question Cannot Be Answered As Asked

### What was asked
The stakeholder wants to know where/when drivers sit idle without passengers —
i.e., true per-driver or per-vehicle empty time between trips.

### What's missing
The public TLC Yellow Taxi trip record dataset contains **no driver ID and no
vehicle/medallion ID**. The only identifier present, `VendorID`, refers to the
**technology provider** that supplied the trip record (e.g. Creative Mobile
Technologies vs. Curb/VeriFone) — not the driver or car. Without a way to link
multiple trips to the same physical vehicle, it is impossible to calculate a
literal gap between one trip's drop-off and that same vehicle's next pickup.

### The grain problem (root cause)
This is fundamentally a **grain limitation**. Grain = the level of detail one row
of data represents. You can always aggregate *up* to a coarser grain, but you can
never manufacture detail that doesn't exist in the source to go *down* to a finer
grain. Data availability puts a floor on how fine the grain can go:

```
[ trip level, per vehicle/driver ]   <- finest grain — DOES NOT EXIST in this data
[ trip level, per record ]           <- finest grain that DOES exist (1 row = 1 trip)
[ zone x hour ]                      <- coarser, aggregated grain (where we're forced to land)
[ zone x day ]                       <- even coarser
[ borough x hour ]                   <- even coarser
```

We are forced to answer the idle-time question at the coarsest grain the data
allows (zone × hour) rather than the finest grain the business actually asked
for (per driver).

### Redefinition (the proxy)
Reframe the business intent from *"how long does each driver sit idle"* to
*"where/when does the market show a supply/demand imbalance"* — a zone-level
question rather than a driver-level question.

**Proxy metric:** For each zone × hour, compare **drop-off count vs. pickup
count**.

- High ratio of drop-offs to pickups in a zone/hour → taxis are arriving there
  but demand isn't immediately absorbing them → likely idle/empty accumulation.
- Low ratio (pickups >> drop-offs) → zone is starved of supply.

This does not track any individual vehicle's idle clock — it measures aggregate
market flow imbalance as a stand-in for it.

### Caveats (must travel with this metric wherever it's used downstream)

1. **Temporal resolution blind spot.** Hourly bucketing can hide idle time that
   happens *within* an hour. Example: a drop-off at 12:01 and the next pickup
   in that same zone at 12:59 both fall in "hour 12," making the hour look
   balanced/active even though a vehicle may have sat idle for nearly an hour.
   Tighter buckets (e.g. 15-min) reduce this blind spot but never eliminate it —
   bucket size is a tunable tradeoff between temporal precision and sparser,
   noisier counts per bucket.

2. **Fungibility assumption.** The ratio assumes any taxi that drops off in a
   zone is available to fulfill any pickup in that same zone/hour. In reality,
   the vehicle that dropped off and the vehicle that picks up next are not
   necessarily linked at all — they could be 10 different cars each. So a
   "balanced" ratio (dropoffs ≈ pickups) can still coexist with real idle time
   happening elsewhere, off the books of this metric. This is a **market-level
   proxy for an individual-level question**, forced by data grain — it measures
   aggregate flow, not individual vehicle continuity.

---

## 4. Open Questions for "The Business"

- Is a zone/hour supply-demand imbalance ratio an acceptable proxy for "idle
  time," or does the team need to pursue alternate data sources to get closer
  to a true per-vehicle measure?
- **Stretch idea:** NYC TLC also publishes **FHV / High Volume FHV (Uber, Lyft,
  etc.) trip records**, which may include vehicle-level fields closer to what a
  true idle-time metric requires. Flagged as a possible future data source —
  **out of scope for the initial build**, which stays scoped to Yellow Taxi
  trip record data only.

---

## 5. Refinement Notes on the "Where / When" Questions

- **Where:** group by `PULocationID`, join to the TLC zone lookup table, count
  trips per zone.
- **When:** extract hour from `tpep_pickup_datetime`, group and count.
- In both cases, deliver a **ranked distribution** (e.g. top 10 zones, a
  zone × hour heatmap), not a single max value — a single "winner" is a trivia
  answer, a distribution is something ops can actually act on.
