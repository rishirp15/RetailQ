# RetailIQ Spark Viva Guide

## What to focus on in the viva

The strongest answer is to explain the **data path through Spark** and name the project functions at each stage. RetailIQ uses Spark as its processing engine: it reads invoice line items, builds a lazy DataFrame plan, executes cleaning and aggregations when actions require results, runs MLlib models, then exports small summaries for the dashboard.

The project runs Spark in **local mode** (`local[2]`). That means two local task slots in one machine, not two cluster nodes. Describe the project as a Spark implementation and a demonstration of Spark APIs. Do not claim that these results prove cluster speedup.

## Spark mental model

- **Driver:** runs the Python application, builds plans, schedules work, and coordinates tasks. In this project, the Python process calling `retailiq.pipeline.main()` is the driver.
- **SparkSession:** the entry point for DataFrames and Spark SQL. `SparkSession.builder` creates or retrieves it. It also provides access to the SparkContext.
- **Executor/task slots:** execute partitions of work. With `local[2]`, Spark uses two local worker threads; this is not a real multi-machine deployment.
- **DataFrame:** a distributed table with named columns and types. RetailIQ uses DataFrames for most cleaning and analysis. Spark can inspect and optimize their logical plans.
- **Transformation:** describes a new DataFrame/RDD, such as `select`, `filter`, `withColumn`, `groupBy`, or `join`. Transformations are lazy.
- **Action:** asks Spark to produce a result, such as `count`, `first`, `collect`, `toLocalIterator`, writing output, or fitting a model. An action causes the required plan to execute.
- **Job, stage, task:** an action may launch one or more jobs; a job is split into stages around shuffle boundaries, and each stage runs tasks over partitions.
- **Shuffle:** redistributes data between partitions. `groupBy`, `distinct`, many joins, `orderBy`, `dropDuplicates`, and `reduceByKey` commonly require a shuffle. Shuffles cost network/disk I/O in a cluster and repartition data in local mode.
- **Cache:** stores a computed DataFrame/RDD for reuse. The first action after `cache()` usually has to populate the cache. Caching helps only when a result is reused enough to offset its memory cost.

For DataFrames, Spark builds a logical plan and can optimize it through Spark SQL's optimizer before execution. It does not execute one Python operation per row for the DataFrame transformations shown here. Python lambdas in the RDD example are lower-level Python functions and have different optimization characteristics.

## End-to-end baseline pipeline

The main entry point is `retailiq/pipeline.py`. The analytical functions it calls are in `retailiq/analytics.py`.

```text
CSV source
  → SparkSession and Spark CSV DataFrame
  → clean_transactions(raw)
  → compute_rfm(clean)
  → cluster_customers(rfm, K)
  → similar_customers(clean, target)
  → country_revenue_rdd(clean)
  → monthly/product aggregations
  → CSV and JSON export
  → stop Spark
  → Streamlit reads exported files
```

### Step-by-step code walkthrough

1. **Build the Spark session** — `pipeline.main()` uses `SparkSession.builder`, sets the application name, master `local[2]`, four SQL shuffle partitions, and parsing options. It sets log level to WARN. `spark.sql.shuffle.partitions=4` controls the default number of partitions for many SQL shuffles; it is not the number of executors or cluster nodes.

2. **Read the CSV** — `read_transactions()` in `retailiq/schema.py` reads the header and builds an explicit Spark schema by column name while preserving source order. `Quantity` and `UnitPrice` are parsed as doubles; IDs, dates, descriptions, and country remain strings. This supports reordered required columns and extra string fields. Invalid numeric text becomes null and is classified by the cleaning audit. Spark reads multiline records and escaped quotes.

3. **Clean and audit** — `clean_transactions(raw)` first checks required column names, then uses `select`, `trim`, `cast`, `to_timestamp`, `coalesce`, and `regexp_replace` to normalize fields. `withColumn("reason", ...)` classifies each row. `cache()` marks the tagged DataFrame for reuse. `groupBy("reason").count().collect()` is an action: Spark computes the audit counts and brings the small reason summary to the driver. The valid rows are filtered; `dropDuplicates()` removes exact duplicates and may shuffle; `withColumn("Revenue", ...)` computes line revenue. A `count()` materializes and counts the cleaned result, then it is cached for later steps.

4. **Create customer RFM** — `compute_rfm(clean)` gets the latest valid date with `agg(max(...)).first()`. `first()` is an action that returns one row to the driver. It registers a temporary view called `purchases`, then Spark SQL groups by `CustomerID` to calculate `MAX(InvoiceDate)`, `COUNT(DISTINCT InvoiceNo)`, and `SUM(Revenue)`. The `GROUP BY` and distinct count require distributed aggregation and can involve a shuffle. Recency is calculated with `datediff`. Four quantile boundaries per metric are obtained with `approxQuantile`, an approximate distributed quantile operation. RFM scores, concatenated score, and high-value/inactive flags are then added with `withColumn` expressions.

5. **Cluster customers** — `cluster_customers(rfm, k)` checks the number of distinct RFM profiles, adds `logRecency`, `logFrequency`, and `logMonetary`, and creates an MLlib `Pipeline`. `VectorAssembler` puts the three features into a vector; `StandardScaler` scales them; `KMeans` fits the model. `fit()` is an action that must examine the feature data and train the model. `model.transform()` returns assignments. The predictions are cached because the project evaluates and summarizes them more than once. Cluster counts, silhouette evaluation, profile averages, revenue shares, and labels are computed afterward.

6. **Find similar baskets** — `similar_customers(clean, target)` groups quantity by `(CustomerID, StockCode)`, extracts the target customer's product vector, computes its norm, computes other customer norms, joins product overlaps, and aggregates dot products. The target vector is broadcast because it is small relative to the full transaction data. The result is cosine similarity, sorted and limited to ten customers. It compares one selected customer against others; it does not create a full all-pairs matrix.

7. **Demonstrate RDDs** — `country_revenue_rdd(clean)` selects `Country` and `Revenue`, converts the DataFrame to an RDD, maps rows to `(country, revenue)` pairs, combines values with `reduceByKey`, sorts, then collects the small country summary. `reduceByKey` performs local combining where possible, followed by a shuffle to bring each key together. Decimal values preserve the money precision used by this operation.

8. **Build monthly and product summaries** — `groupBy`, `agg`, `orderBy`, and `limit` calculate monthly revenue/orders and top products. These are DataFrame operations, not pandas calculations.

9. **Export outputs** — `export_csv()` formats timestamps in Spark, then iterates through a DataFrame using `toLocalIterator()` to write rows to a local CSV. This avoids one giant `collect()` of the full exported DataFrame, but it is still a driver-side local file export and is intended for the small project outputs. Country revenue is already a small collected RDD result. The pipeline writes a JSON summary, then calls `spark.stop()` in a `finally` block.

10. **Display results** — `app.py` and `retailiq/dashboard.py` read the exported results and render charts and tables. Opening the dashboard does **not** rerun Spark or retrain the model. pandas is used for display of the compact exported results; it is not the main transaction-processing engine.

## Project function map

| Function | File | Spark role | Key idea to say aloud |
|---|---|---|---|
| `main()` | `retailiq/pipeline.py` | Orchestrates the baseline Spark application | “The driver wires together ingestion, cleaning, analysis, export, and shutdown.” |
| `read_transactions()` | `retailiq/schema.py` | Header-aware explicit schema and CSV ingestion | “It sets numeric types by column name without assuming source column order.” |
| `clean_transactions()` | `retailiq/analytics.py` | DataFrame transformations and cleaning audit | “Each row gets one primary rejection reason; the audit is a Spark aggregation.” |
| `compute_rfm()` | `retailiq/analytics.py` | Spark SQL and approximate quantiles | “Frequency counts distinct invoices, because each invoice can have multiple product rows.” |
| `cluster_customers()` | `retailiq/analytics.py` | MLlib pipeline and K-Means | “The feature pipeline transforms skewed, differently scaled RFM values before distance-based clustering.” |
| `similar_customers()` | `retailiq/analytics.py` | DataFrame joins, dot product and cosine normalization | “It compares one target basket to all customers without an all-pairs join.” |
| `country_revenue_rdd()` | `retailiq/analytics.py` | DataFrame-to-RDD conversion and pair aggregation | “This is the explicit low-level RDD map/reduce example.” |
| `export_csv()` | `retailiq/pipeline.py` | Spark-side timestamp formatting plus driver export | “Spark keeps the processing distributed; only compact result files are exported for the UI.” |
| `compare_k()` | `retailiq/experiments.py` | Repeated MLlib K-Means fits and evaluation | “It compares K and seed stability; it does not create ground-truth labels.” |
| `market_baskets()` | `retailiq/commerce.py` | Spark MLlib FP-Growth | “It mines co-occurrence rules from unique product sets per invoice.” |
| `train_recommendations()` | `retailiq/commerce.py` | Spark MLlib implicit ALS | “Purchase counts are implicit feedback; evaluation tests novel products in a later period.” |
| `inactivity_prediction()` | `retailiq/prediction.py` | Spark MLlib Random Forest with chronological holdout | “The label is no observed purchase in the next 60 days, not permanent churn.” |
| `retention_cohorts()` / `monthly_migration()` | `retailiq/longitudinal.py` | Spark SQL/DataFrame joins, windows and aggregations | “Cohorts track activity; migration is retrospective with one frozen model.” |
| `replay_stream()` | `retailiq/streaming.py` | Structured Streaming file source, event-time windows, checkpoint | “This is historical replay and checkpoint recovery, not a live store feed.” |
| `performance_study()` | `retailiq/benchmark.py` | Spark reads/writes, repartitioning and caching | “The measurements are local and do not prove cluster speedup.” |

## Transformations and actions in RetailIQ

| Operation | Type | What it does in this project |
|---|---|---|
| `select`, `withColumn`, `filter`, `drop`, `fillna` | Transformation | Shape, normalize, derive, or remove columns/rows |
| `groupBy`, `agg`, `join`, `distinct`, `dropDuplicates` | Transformation | Build grouped/joined plans; often require shuffle when executed |
| `orderBy`, `limit` | Transformation | Define sorted/limited result plan; sorting can require shuffle |
| `cache`, `persist` | Persistence request | Mark data for reuse; not by itself a guarantee the data is already materialized |
| `count`, `first`, `collect` | Action | Return count, one row, or rows to the driver |
| `approxQuantile` | Action-like computation | Computes approximate quantile cut points for RFM scores |
| `toLocalIterator` | Action/driver consumption | Streams partitions to the driver one at a time for CSV export |
| `Pipeline.fit`, `KMeans.fit`, `ALS.fit` | Model training action | Executes distributed work to fit model parameters |
| `model.transform` | Transformation | Adds model output columns; work executes when a later action consumes it |
| `writeStream.start`, `processAllAvailable` | Streaming execution | Starts the streaming query and processes available source files |

**Key viva point:** a chain such as `clean.groupBy(...).agg(...).orderBy(...)` usually has not read and aggregated all data yet. It describes a plan. An action later asks Spark to run that plan. A single action can trigger multiple stages, and an application can run many jobs because it has many actions.

## Important PySpark functions used

| Function/API | How RetailIQ uses it |
|---|---|
| `trim()` | Removes surrounding whitespace from invoice and product identifiers. |
| `cast()` | Converts quantity and unit price from CSV strings to numeric values. |
| `to_timestamp()` / `coalesce()` | Parses supported timestamp formats and takes the first successful parse. |
| `regexp_replace()` | Normalizes CustomerID strings by removing a trailing `.0`. |
| `when()` / `otherwise()` | Assigns one ordered cleaning reason and creates rule-based customer labels. |
| `isNull()` / `isnan()` / comparisons | Detects missing, non-finite, and non-positive values. |
| `withColumn()` | Adds normalized fields, row reasons, revenue, recency, scores, and log features. |
| `filter()` / `drop()` | Keeps valid purchase rows and removes helper columns. |
| `dropDuplicates()` | Removes exact duplicate normalized purchase rows. |
| `groupBy()` / `agg()` | Summarizes audit reasons, customers, months, countries, products, and baskets. |
| `countDistinct()` | Counts distinct invoice IDs for customer frequency and distinct orders. |
| `createOrReplaceTempView()` | Registers the cleaned purchase DataFrame as `purchases` for Spark SQL. |
| `approxQuantile()` | Calculates approximate 20/40/60/80% thresholds for RFM scoring. |
| `datediff()` | Converts the reference date and last purchase date into recency days. |
| `log1p()` | Reduces skew in positive RFM values before distance-based clustering. |
| `VectorAssembler` | Packs three log-RFM columns into an MLlib feature vector. |
| `StandardScaler` | Standardizes the assembled features before K-Means. |
| `KMeans` / `ClusteringEvaluator` | Fits clusters and evaluates their in-sample silhouette. |
| `broadcast()` | Sends the small target basket vector to workers for the similarity join. |
| `sqrt()`, `sum()`, `join()` | Build the basket norms and dot products used in cosine similarity. |
| `rdd.map()` / `reduceByKey()` / `sortBy()` | Demonstrate country-revenue aggregation through the RDD API. |
| `cache()` / `unpersist()` | Reuse intermediate data, then release cached data when a stage is finished. |
| `collect()` / `first()` / `count()` | Trigger execution and return small summaries, one row, or counts to the driver. |
| `toLocalIterator()` | Streams compact exported result rows to the local CSV writer. |

When describing a function, connect it to the field or output it changes. For example: “`countDistinct(InvoiceNo)` is used for Frequency because frequency means distinct customer orders, not product line count.”

## Advanced Spark components

### Automatic K comparison — `experiments.py`

The project compares K=2–8 over seeds 42, 7, and 21: 21 K-Means runs. It calculates silhouette, cluster sizes, training cost, and adjusted Rand index (ARI) between assignments. ARI checks whether two runs place pairs of customers together, and is unaffected by numeric cluster-label renaming. The measured selection is K=2, mean silhouette 0.62354, mean stability ARI 1.000. K=4 remains as the more detailed baseline. Explain the selection rule and business tradeoff; a high silhouette is not proof of better business outcomes.

### FP-Growth — `commerce.py`

`market_baskets()` groups each customer/invoice into a sorted set of StockCodes and fits `pyspark.ml.fpm.FPGrowth`. Support is the fraction of baskets containing an itemset; confidence estimates the conditional frequency of the consequent when the antecedent appears; lift compares that confidence to the consequent's baseline support. The run exports 81 rules meeting support ≥2%, confidence ≥25%, and lift >1. A rule suggests a bundle to test; it does not show that one product caused another purchase.

### ALS — `commerce.py`

`train_recommendations()` converts customer and product IDs to integer indices and counts distinct invoices per customer-product pair. Spark MLlib ALS is configured for implicit feedback, rank 12, ten iterations, regularization 0.1, alpha 10, and seed 42. A time cutoff reserves the final 60 days as holdout; it evaluates discovery of previously unseen products. Precision@5 was 5.33%, versus 4.50% for a popularity baseline. The dashboard model is then refit on all observed purchases. These scores are ranking metrics, not probabilities.

### Inactivity classifiers — `prediction.py`

The target is “no purchase observed in the next 60 days,” not permanent churn. Features come from historical snapshots of log recency, invoice frequency, monetary value, tenure, product count, and average order value. Two earlier snapshots train the models; a later 60-day window is held out. There is no random row split that would mix earlier and later time periods.

The project compares three MLlib classifiers on that same holdout:

- **Logistic Regression:** learns a weighted linear boundary; it is the simpler baseline.
- **Random Forest:** combines many decision trees and can capture nonlinear thresholds.
- **Gradient-Boosted Trees:** adds trees sequentially to correct earlier errors.

PR AUC is the primary selection metric, with ROC AUC and F1 as tie-breakers. After evaluation, the selected pipeline is refit on fully labelled historical snapshots. `model_comparison.csv` shows the holdout comparison; `feature_importance.csv` explains the selected evaluation model. The latest customer risk scores have no observed future outcomes, and the scores are not assumed calibrated.

### Structured Streaming — `streaming.py`

The source is a directory of historical CSV files read with `spark.readStream` and an explicit schema. `maxFilesPerTrigger=1` limits each trigger to one new file. The query applies a two-day event-time watermark and aggregates revenue and row count in one-day windows by country. `writeStream` uses `foreachBatch` and a checkpoint location. The replay processes three files of 2,000 purchase rows each, reconciles 6,000 rows and £130,841.06, then restarts from the same checkpoint and checks that totals do not change. This demonstrates recovery from processed files; it is not an always-on live feed. In complete output mode, old aggregation state remains, so the watermark does not evict the full historical state.

### Benchmark — `benchmark.py`

The project writes equivalent data as CSV and Parquet, repartitions it to 1, 2, and 4 partitions, runs a country aggregation with/without cache, and repeats each setup twice. It checks that all variants return identical row counts and Decimal revenue. Parquet is smaller for this projection in the recorded run. Explain the limitation: local warm-cache timings with two local threads are not a production benchmark or a cluster speedup result.

The optional `retailiq.scale_benchmark` command adds a separate row-scaling exercise. It cross-joins the cleaned transactions with replica IDs at 1x, 2x, and 4x, then re-runs country aggregation and checks exact row and Decimal revenue totals. It writes to `outputs/scale_study` by default, leaving the saved UCI dashboard results alone. This demonstrates how workload size changes; it still runs locally and must not be described as multi-node scaling.

```powershell
.\.venv\Scripts\python.exe -m retailiq.scale_benchmark --input data/raw/online_retail.csv --output outputs/scale_study --factors 1 2 4 --repetitions 2 --partitions 4 --master 'local[2]'
```

## What to show in the live demo

1. Open `outputs/uci/summary.json` or the dashboard's Method & data quality view to show the input/audit reconciliation.
2. Show the segment table and explain RFM, baseline K=4, and why labels are descriptive.
3. Open `retailiq/analytics.py` to point to `compute_rfm()`, `cluster_customers()`, and `country_revenue_rdd()`.
4. Open `retailiq/pipeline.py` to show the call order and the `SparkSession` configuration.
5. If asked for Spark execution evidence, show the Spark UI at `http://localhost:4040` **while a Spark job is running**. The standard wrapper stops Spark before the dashboard is shown, so the UI disappears when analysis completes. The UI can show jobs, stages, tasks, storage, and SQL plans; it does not convert local mode into a distributed cluster.
6. If advanced pages are ready, show one advanced result and its evaluation caveat rather than trying to cover all eight extensions in five minutes.

For the simplest dashboard walkthrough with already saved UCI results, run `.\scripts\run.ps1 -DashboardOnly` from PowerShell at the project root. The synthetic run is `.\scripts\run.ps1 -Demo`; clearly label those results synthetic. A full analysis run is `.\scripts\run.ps1 -Advanced` and can take several minutes.

## Likely examiner questions with project-specific answers

**Where does Spark start in the project?**  
In `retailiq.pipeline.main()`, at `SparkSession.builder`. The session is the entry point for DataFrames and SQL; SparkContext is available through it.

**What does `local[2]` mean?**  
Run Spark locally with two worker threads/task slots. It does not mean two machines or a cluster.

**Are CSV read and DataFrame transformations eager?**  
No. Reading and most transformations are lazy. Actions, writes, and model fitting execute the required computation.

**Which operations cause shuffles?**  
Operations that regroup or reorder data commonly shuffle: `groupBy`, `distinct`, `dropDuplicates`, many joins, `orderBy`, and RDD `reduceByKey`. Exact execution depends on Spark's physical plan and optimizer.

**Why does the cleaning function call `collect()`?**  
It collects only a small aggregation with one row per rejection reason, not all transaction rows. Collecting the whole source to the driver would be unsafe at larger scale.

**Does `cache()` execute immediately?**  
No. It marks a DataFrame for persistence. A later action computes and populates the cache. RetailIQ caches tagged rows, cleaned transactions, RFM features, and predictions because later actions reuse them.

**Why use Spark SQL for RFM?**  
The customer profile is a group aggregation over distributed transaction rows. SQL expresses the `MAX`, `COUNT(DISTINCT)`, and `SUM` clearly and Spark executes the aggregation.

**Why use an RDD if DataFrames are higher-level?**  
The country revenue function intentionally demonstrates the RDD pair pattern `map → reduceByKey → sortBy`. Most of the project uses DataFrames because they preserve schema and allow Spark SQL optimizations.

**What does `VectorAssembler` do?**  
It turns selected numeric columns into the feature vector format expected by Spark MLlib estimators such as K-Means.

**Why scale for K-Means?**  
K-Means compares distances. Log transformation reduces feature skew; standardization puts R, F, and M on comparable scales before distance calculations.

**What is Spark's lazy evaluation useful for?**  
It lets Spark optimize and combine operations in a plan before doing work, and avoids computing results that are never requested.

**Why can one action create several stages?**  
Spark splits execution into stages at shuffle boundaries. A `groupBy` may require a map-side partial aggregation, a shuffle, and a final aggregation stage.

**What is the difference between a partition and a task?**  
A partition is a chunk of distributed data. For a stage, Spark commonly runs one task per partition. Task slots control how many tasks can run concurrently; `local[2]` has two local slots.

**Is silhouette the accuracy of the cluster model?**  
No. There are no known correct segment labels. Silhouette is a geometric in-sample separation/cohesion measure using the configured squared-Euclidean distance.

**What is the biggest limitation in claiming this is big data?**  
The input is about 542k rows and was processed locally with two threads. The implementation demonstrates distributed processing APIs, but the experiment does not demonstrate scale-out performance.

## One-minute answer to “How is Spark used here?”

“RetailIQ uses PySpark as the processing and modeling engine. The driver creates a SparkSession and reads invoice lines as a DataFrame. `clean_transactions` casts and filters rows, assigns audit reasons, removes duplicates, and calculates line revenue. `compute_rfm` uses Spark SQL to group by customer and calculate last purchase, distinct invoice frequency, and monetary spend. `cluster_customers` builds an MLlib pipeline with VectorAssembler, StandardScaler, and K-Means. Other DataFrame joins calculate one-customer basket cosine similarity, while `country_revenue_rdd` demonstrates map and reduceByKey. The advanced modules use FP-Growth, implicit ALS, Random Forest, and Structured Streaming. Spark transformations are lazy, and actions such as count, first, collect, writes, and model fitting trigger execution. The project ran in local[2], so it demonstrates Spark APIs without claiming cluster speedup.”

## Code navigation

- Baseline orchestration: [`retailiq/pipeline.py`](../retailiq/pipeline.py)
- Explicit CSV schema: [`retailiq/schema.py`](../retailiq/schema.py)
- Core Spark analytics: [`retailiq/analytics.py`](../retailiq/analytics.py)
- Runtime/session and Windows storage adapter: [`retailiq/runtime.py`](../retailiq/runtime.py)
- K comparison/stability: [`retailiq/experiments.py`](../retailiq/experiments.py)
- FP-Growth and ALS: [`retailiq/commerce.py`](../retailiq/commerce.py)
- Cohorts and migration: [`retailiq/longitudinal.py`](../retailiq/longitudinal.py)
- Inactivity model: [`retailiq/prediction.py`](../retailiq/prediction.py)
- Structured Streaming replay: [`retailiq/streaming.py`](../retailiq/streaming.py)
- Local performance study: [`retailiq/benchmark.py`](../retailiq/benchmark.py)
- Measured baseline: [`docs/RESULTS.md`](RESULTS.md)
- Measured advanced findings: [`docs/ADVANCED_RESULTS.md`](ADVANCED_RESULTS.md)

