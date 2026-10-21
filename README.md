# Edtech Learning Analytics Pipeline

A medallion-style ETL project for edtech learner analytics, implemented with PySpark notebooks (Bronze → Silver → Gold) and a downstream Snowflake SQL notebook for loading, masking, and analysis.

## Project Purpose

This repository demonstrates an end-to-end analytics pipeline for online learning data:

- Ingest raw CSV files for students, courses, and assessments.
- Standardize and enrich data in Silver tables.
- Build Gold-level aggregated analytics for course performance and student engagement.
- Export curated data for warehouse ingestion.
- Load curated files into Snowflake and run analytical SQL, including PII masking.

## End-to-End Workflow

1. **Raw inputs** (`students.csv`, `courses.csv`, `assessments.csv`) are read from OneLake-style paths (`Files/raw/edtech`) in `bronze.ipynb`.
2. **Bronze layer** writes raw datasets to Delta managed tables:
   - `bronze_students`
   - `bronze_courses`
   - `bronze_assessments`
3. **Silver layer** (`silver.ipynb`) performs type casting and derived columns, then writes:
   - Delta managed tables: `silver_students`, `silver_courses`, `silver_assessments`
   - Physical Delta files under `Files/silver/*_delta`
4. **Gold layer** (`gold.ipynb`) builds aggregated marts:
   - `gold_course_performance`
   - `gold_student_engagement`
   and writes them as Delta and export files (`Files/export/*`) in Parquet and CSV.
5. **Warehouse/analytics layer** (`onelake_snowflake.ipynb`) creates Snowflake database/schema, stages CSV files, loads tables with `COPY INTO`, applies masking policy on student PII fields, and runs business analysis queries.

## Source Data and Outputs

### Source inputs (repository root)

- `/home/runner/work/Edtech_learning_analytics_pipeline/Edtech_learning_analytics_pipeline/students.csv` (~500 rows)
- `/home/runner/work/Edtech_learning_analytics_pipeline/Edtech_learning_analytics_pipeline/courses.csv` (~100 rows)
- `/home/runner/work/Edtech_learning_analytics_pipeline/Edtech_learning_analytics_pipeline/assessments.csv` (~4,000 rows)

### Example curated/export outputs

Sample exported CSVs are included under:

- `/home/runner/work/Edtech_learning_analytics_pipeline/Edtech_learning_analytics_pipeline/export from onelake/students.csv`
- `/home/runner/work/Edtech_learning_analytics_pipeline/Edtech_learning_analytics_pipeline/export from onelake/courses.csv`
- `/home/runner/work/Edtech_learning_analytics_pipeline/Edtech_learning_analytics_pipeline/export from onelake/assessments.csv`
- `/home/runner/work/Edtech_learning_analytics_pipeline/Edtech_learning_analytics_pipeline/export from onelake/course_performance.csv`
- `/home/runner/work/Edtech_learning_analytics_pipeline/Edtech_learning_analytics_pipeline/export from onelake/student_engagement.csv`

These outputs reflect Silver enrichments (for example, `account_age_days`, `completion_days`, `pass_flag`, `revenue_per_enrollment`) and Gold aggregates.

## Architecture and Key Files

```text
Edtech_learning_analytics_pipeline/
├── README.md
├── students.csv                      # raw learner dimension
├── courses.csv                       # raw course catalog dimension
├── assessments.csv                   # raw learner-course activity fact
├── bronze.ipynb                      # ingestion to bronze Delta tables
├── silver.ipynb                      # cleaning/enrichment to silver Delta tables
├── gold.ipynb                        # aggregations + export for warehouse load
├── onelake_snowflake.ipynb           # Snowflake DDL/DML, masking, analytics SQL
└── export from onelake/
    ├── students.csv                  # curated silver export
    ├── courses.csv                   # curated silver export
    ├── assessments.csv               # curated silver export
    ├── course_performance.csv        # gold aggregate export
    └── student_engagement.csv        # gold aggregate export
```

## Technologies and Roles

- **PySpark (Synapse/Fabric notebooks)**: ETL transformations and Delta writes.
- **Delta Lake tables**: Bronze/Silver/Gold managed data layers.
- **Parquet/CSV**: Interchange/export formats for downstream loading.
- **Snowflake SQL**: Warehouse table creation, staged loads, masking policy, and analytics.

## Prerequisites

Because this project is notebook-first, execution depends on a Spark notebook runtime (the ETL notebooks are configured with `synapse_pyspark`).

- Access to a Spark environment that supports:
  - `spark.read` / `spark.table`
  - `saveAsTable(...)`
  - Delta format read/write
- Access to a OneLake/Lakehouse-style filesystem path used in notebooks (`Files/...`).
- (Optional) Snowflake environment for `onelake_snowflake.ipynb` SQL steps.

## Environment Setup

### 1) Clone repository

```bash
git clone https://github.com/Dynamic-Ak/Edtech_learning_analytics_pipeline.git
cd Edtech_learning_analytics_pipeline
```

### 2) Load raw CSV inputs

Ensure the raw CSV files are available at the path referenced in `bronze.ipynb`:

```python
base_path = "Files/raw/edtech"
```

### 3) Run notebooks in order

1. `bronze.ipynb`
2. `silver.ipynb`
3. `gold.ipynb`
4. `onelake_snowflake.ipynb` (if running Snowflake load/analytics)

## Running the Pipeline

From the notebook environment, execute all cells in sequence per notebook.

Expected notebook console outputs include row-count verification such as:

- Bronze row counts for students/courses/assessments
- Silver row counts for students/assessments/courses
- Gold row counts for course and engagement aggregates

Expected data artifacts after successful execution:

- Delta tables: `bronze_*`, `silver_*`, `gold_*`
- Physical Delta files: `Files/silver/*_delta`, `Files/gold/*_delta`
- Export files: `Files/export/{students,courses,assessments,course_performance,student_engagement}`

## Data Quality and Validation in Current Code

Current quality checks are lightweight and notebook-based:

- Explicit schema/type casting in Silver layer.
- Derived metric creation checks via successful writes.
- Row-count printouts in Bronze/Silver/Gold notebooks.

No standalone automated test suite, CI validation, or dedicated data-quality framework files are present in this repository at this time.

## Logging, Monitoring, and Orchestration Status

- **Logging**: basic notebook `print(...)` statements.
- **Monitoring**: no separate monitoring stack/configuration committed.
- **Orchestration/Scheduling**: no scheduler or pipeline-as-code assets are included in this repository.

## Snowflake Layer Notes

`onelake_snowflake.ipynb` includes:

- `CREATE DATABASE/SCHEMA` statements (`LEARNCO.ANALYTICS`)
- CSV file format + stage creation
- `COPY INTO` loads for `STUDENTS`, `COURSES`, `ASSESSMENTS`, `COURSE_PERFORMANCE`, `STUDENT_ENGAGEMENT`
- A masking policy (`learner_pii_mask`) applied to `STUDENTS.first_name`, `last_name`, `email`
- Analytical SQL examples (difficulty/completion analysis, churn-risk learners, instructor ranking)

## Privacy, Credentials, and Secrets

- Student-level identifiers and contact-style fields exist in sample data and warehouse tables.
- The Snowflake notebook demonstrates column-level masking for sensitive learner fields.
- No credentials or environment variable templates are committed in this repository; keep warehouse credentials and connection secrets in your execution environment, not in source control.

## Troubleshooting

- **`table not found` errors**: run notebooks in Bronze → Silver → Gold order.
- **Path not found (e.g., `Files/...`)**: verify your notebook runtime has the same Lakehouse/OneLake mount conventions expected by the notebooks.
- **Schema overwrite issues**: `silver.ipynb` uses `option("overwriteSchema", "true")`; rerun the relevant Silver/Gold writes when schema changes occur.
- **Snowflake load issues**: verify staged filenames and file format settings match exported CSV structure.
