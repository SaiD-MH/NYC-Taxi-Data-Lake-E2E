# Project Roadmap

## Phase 0 — Data Understanding 

### Step 1: Business Context — Done

- **Persona:** Operations / Fleet Efficiency
- **Business questions:** Defined.
- **Idle-time question:** Hit a real data-availability wall because the Yellow Taxi data does not contain a driver or vehicle ID.
- Worked through the **data grain** concept and redefined the question as a:
  - **Zone × Hour supply/demand imbalance proxy**
- Documented two important caveats:
  1. **Temporal resolution blind spot**
  2. **Fungibility assumption**
- Documentation:
  - `docs/business_context.md`
- **Status:** Delivered and awaiting review/edits.

### Step 2: Pull Raw Data + Understand The Data Dictionary 

- Pull Yellow  - Green - FHV - HV FHV datasets
  - Understand the fields values
  - Understand buesiness value of each field

### Step 3: Data Profiling

Use **pandas / Jupyter-Notebook** at this stage 

Profile the raw data for:

- Row counts
- Nulls
- Distinct counts
- Ranges and outliers:
  - Fare
  - Distance
  - Passenger count
- Timestamp sanity:
  - Drop-off before pickup
  - Invalid timestamps
  - Other timestamp inconsistencies
- Distribution shapes
- Other anomalies discovered during exploration

**Deliverable:**

> An anomaly report documenting what is wrong, suspicious, or unexpected in the data — **without applying fixes yet**.

### Step 4: Buesiness Rules / Dashboard Gathering & Data Modeling 

- Gathering the Business Requests:
  - Dashboards & Reports 
  - Business Rules

- Validating The Dashboards with Data Avaiablility


- Build The Data Modeling That Serve Validated Dashboards

  - The model will be designed using:

    - The data dictionary
    - Profiling findings
    - Business context
    - Defined analytical grain

  - This will determine:

    - Fact table grain
    - Dimension structure
    - Measures
    - Relationships
    - Required transformations

> Detailed modeling scope will be defined once profiling exposes the real characteristics and issues in the data.

---

## Phase 1 — Environment & Pipeline Architecture


> The goal is to establish that the core platform components are correctly wired together before building the full pipeline.

---

## Phase 2 — Ingestion

Design and implement the ingestion layer.

### Topics

- Landing zone design
- Batch ingestion
- Simulated incremental ingestion
- Storage partitioning strategy

---

## Phase 3 — Transformation

Implement the transformation layer using:

- PySpark
- Spark SQL


Data quality checks should be incorporated into the transformation process.

>Transformation rules should be **informed by the anomalies discovered during Phase 0 profiling**, rather than making assumptions before understanding the data.

---



## Phase 4 — Loading 

Build the orchestration layer using **Apache Airflow**.



## Phase 5 — Orchestration

Build the orchestration layer using **Apache Airflow**.

### Topics

- Airflow DAGs
- Retries
- Sensors
- Failure handling
- SLA thinking
- Pipeline scheduling

---

## Phase 6 — Serving + Testing + CI/CD

### Serving

Load the final modeled data into:

- PostgreSQL

### Testing

Use:

- `pytest`
- Unit tests for transformation logic
- Data quality tests

### CI/CD

Implement a GitHub Actions pipeline covering:

- Code quality checks
- Tests
- Pipeline validation

---

## Phase 7 — Stretch

Optional extensions after the core pipeline is complete.

### Streaming Simulation

Introduce a simulated streaming/incremental processing scenario to extend the batch pipeline.

### Snowflake Side Quest

Explore how the modeled data pipeline could be adapted to use Snowflake as an analytical serving layer.

### Performance Deep-Dive

Investigate Spark performance topics such as:

- Partition pruning
- Shuffle behavior
- Partition sizing
- Join strategies
- Execution planning

### FHV / Uber Data Exploration

Investigate whether **FHV or Uber-related datasets** can provide the missing granularity needed to explore the original **idle-time metric**.

This remains an open question because the current Yellow Taxi dataset does not provide driver/vehicle identifiers.
