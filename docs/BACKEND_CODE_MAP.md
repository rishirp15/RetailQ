# RetailIQ backend code map

This guide follows the code in the order it runs. It is intended as a viva aid and
does not introduce or change any project features.

## 1. Start here: batch pipeline

Run the baseline analysis from the project root:

```powershell
python -m retailiq.pipeline --input data/raw/online_retail.csv
```

```text
CSV input
   |
   v
schema.read_transactions       Explicit Spark CSV schema
   |
   v
analytics.clean_transactions   Type, validate, audit, deduplicate, calculate Revenue
   |
   +--> analytics.compute_rfm  Spark SQL and DataFrame aggregations
   |       |
   |       +--> analytics.cluster_customers  MLlib Pipeline: assemble, scale, K-Means
   |       +--> analytics.similar_customers   Product-vector cosine similarity
   |
   +--> analytics.country_revenue_rdd        DataFrame-to-RDD map/reduceByKey
   +--> pipeline monthly/product summaries   DataFrame groupBy and aggregations
   |
   v
CSV/JSON outputs in outputs/uci
   |
   v
app.py / retailiq.dashboard                  Streamlit frontend reads those outputs
```

`pipeline.main()` coordinates the stages. The reusable analysis functions live in
`analytics.py`; this separation lets the same cleaning and RFM logic be reused by
the advanced stages.

## 2. Advanced analysis pipeline

Run requested advanced stages from the project root:

```powershell
python -m retailiq.advanced
```

`advanced.main()` reads and cleans the input once, calculates RFM, then dispatches
the selected `--features` in the `FEATURES` list. Results and each stage's status
are recorded in `outputs/uci/advanced/advanced_summary.json`. A completed stage is
reused on later runs unless `--rerun` is supplied. If a stage fails, inspect its
`status` and `error` in that manifest and the traceback in the terminal; later
stages can have dependencies (for example, migration needs clustering).

| Feature name | Module and main function | Spark concepts shown |
| --- | --- | --- |
| `clustering` | `experiments.compare_k`, `experiments.describe_selected` | MLlib Pipeline, K-Means, feature scaling, silhouette, repeated seeds, ARI stability |
| `baskets` | `commerce.market_baskets` | MLlib FP-Growth, support, confidence, lift, association rules |
| `recommendations` | `commerce.train_recommendations` | MLlib implicit ALS and chronological holdout ranking metrics |
| `cohorts` | `longitudinal.retention_cohorts` | DataFrame aggregation, month sequences, cohort retention |
| `migration` | `longitudinal.monthly_migration` | Saved MLlib pipeline applied to historical snapshots and window `lag` transitions |
| `prediction` | `prediction.inactivity_prediction` | Logistic Regression, Random Forest, Gradient-Boosted Trees compared on a temporal holdout |
| `streaming` | `streaming.replay_stream` | Structured Streaming file source, watermark, `foreachBatch`, checkpoint recovery |
| `performance` | `benchmark.performance_study` | CSV vs Parquet, repartitioning, cache comparison, query plan inspection |

The feature names describe analyses the program runs; they are not separate Spark
services. The standard setup is local Spark (`local[2]`) unless `--master` is
changed. The streaming stage replays prepared historical CSV files through Spark
Structured Streaming. It is not connected to a live retailer event feed.

## 3. Shared backend modules

| File | Responsibility | Key functions / ideas |
| --- | --- | --- |
| `retailiq/schema.py` | Input contract | `transaction_schema`, `read_transactions`; explicit types and source-column order |
| `retailiq/runtime.py` | Spark session and Windows local filesystem setup | `create_spark`, `portable_filesystem_jar` |
| `retailiq/analytics.py` | Core business analysis | `clean_transactions`, `compute_rfm`, `cluster_customers`, `similar_customers`, `country_revenue_rdd` |
| `retailiq/storage.py` | Safe small report writes and source fingerprints | `write_json`, `write_records`, `file_hash` |
| `retailiq/pipeline.py` | Baseline command-line orchestration and Spark CSV export | `main`, `export_csv` |
| `retailiq/advanced.py` | Advanced feature orchestration and resumable manifest | `FEATURES`, `main` |
| `retailiq/data.py` | Download/convert the UCI workbook to CSV | `convert_workbook`, `main` |
| `retailiq/sample.py` | Deterministic synthetic sample generation | `generate` |
| `retailiq/scale_benchmark.py` | Optional local row replication study | `run_scale_study`, `main` |
| `retailiq/verify_models.py` | Reload saved models and check persisted behavior | `main` |
| `retailiq/dashboard.py`, `app.py` | Read output files and display the Streamlit UI | Frontend; not part of Spark transformations |

## 4. Spark concepts to point to in code

| Concept | Where to show it | Plain-language explanation |
| --- | --- | --- |
| SparkSession | `pipeline.main`, `runtime.create_spark` | Entry point for creating DataFrames and accessing Spark SQL/ML |
| DataFrame transformations | `analytics.clean_transactions` | Operations describe a plan; Spark evaluates it when an action needs results |
| Actions | `count`, `collect`, `first`, `write` calls across the modules | Actions execute the lazy plan and return or persist results |
| Spark SQL | `analytics.compute_rfm` | SQL groups purchases by customer to calculate RFM inputs |
| RDD | `analytics.country_revenue_rdd` | Explicit `map` and `reduceByKey` example for distributed key/value aggregation |
| MLlib Pipeline | `analytics.cluster_customers`, `experiments.compare_k` | Fits feature preparation and model stages as one reusable workflow |
| Partitioning and shuffle | `benchmark.performance_study` | Grouped aggregations move records by key; partition count affects parallel tasks |
| Persistence | `cache` / `unpersist` calls in analysis modules | Keeps reused intermediate results in memory when practical and releases them afterward |
| Structured Streaming | `streaming.replay_stream` | Processes files incrementally with micro-batches and uses a checkpoint to recover progress |

## 5. Suggested code walkthrough in the viva

1. Open `retailiq/pipeline.py` and explain that `main()` is the baseline driver.
2. Open `retailiq/schema.py` to show how the source CSV gets an explicit schema.
3. Open `analytics.clean_transactions` and explain validation, mutually exclusive
   audit reasons, duplicate removal, and `Revenue = Quantity * UnitPrice`.
4. Open `analytics.compute_rfm` for DataFrame aggregation, SQL, approximate
   quantiles, and RFM scoring.
5. Open `analytics.cluster_customers` for `VectorAssembler`, `StandardScaler`,
   `KMeans.fit`, `transform`, and silhouette evaluation.
6. Show `analytics.country_revenue_rdd` as the project's explicit RDD example.
7. For the advanced pipeline, show one feature in `advanced.main()` and its
   implementation in the module listed in the table above.
8. For streaming, explain the file source, `withWatermark`, `foreachBatch`,
   checkpoint directory, and that input is a historical replay.
9. Finish at `app.py`/`dashboard.py` and explain that the UI displays generated
   outputs; it does not run the Spark transformations itself.

## 6. Output and troubleshooting pointers

- Baseline output: `outputs/uci/summary.json` and neighboring CSV files.
- Advanced output: `outputs/uci/advanced/advanced_summary.json`, stage CSVs, and
  persisted models under `outputs/uci/advanced/models`.
- Streaming output: `outputs/uci/advanced/streaming/summary.json`, batch snapshots,
  progress, and the checkpoint directory.
- If the advanced UI says a model or analysis is not generated, check that the
  corresponding feature status is `completed` in `advanced_summary.json`. If it is
  `failed`, read its error and the terminal traceback. If it is absent, confirm the
  feature was included in `--features` and that `python -m retailiq.advanced`
  completed successfully.
- Spark's local web UI is usually available at `http://localhost:4040` only while
  a Spark application is actively running. It is not the RetailIQ dashboard and
  closes when the Spark application stops.
