# RetailIQ: 30-minute, four-person viva script

**Subject:** Big Data Analytics  
**Project:** RetailIQ customer and purchase analytics using Apache Spark / PySpark  
**Target length:** 30 minutes of presentation, about 7 minutes 30 seconds per speaker  
**Suggested speaking pace:** 110–125 words per minute, including screen changes and short pauses

This script uses the current saved results in `outputs/uci_schema_demo`. Recheck the result files if you rerun the project with a different input or settings. Do not call the file replay a live retailer feed: the project does not connect to a live point-of-sale, Kafka, or cloud stream.

## Presenter split

| Time | Presenter | Main responsibility | Files / screen to show |
| --- | --- | --- | --- |
| 0:00–7:30 | Member 1 | Retail problem, dataset, data quality, Big Data framing | Dashboard overview; `retailiq/data.py`; `retailiq/schema.py`; `outputs/uci_schema_demo/summary.json` |
| 7:30–15:00 | Member 2 | Apache Spark, PySpark, architecture, execution, core code pipeline | Architecture figure; `retailiq/runtime.py`; `retailiq/pipeline.py`; `retailiq/analytics.py` |
| 15:00–22:30 | Member 3 | Feature engineering, analytics, ML models and measured results | Dashboard segment/model pages; `retailiq/analytics.py`; `retailiq/experiments.py`; `retailiq/commerce.py`; `retailiq/prediction.py` |
| 22:30–30:00 | Member 4 | Streaming, outputs, demo walkthrough, limitations, conclusion | Dashboard streaming/performance pages; `retailiq/streaming.py`; Spark UI if a Spark job is running |

Each presenter owns a substantial technical section. Rehearse transitions so one presenter does not repeat another's material.

## Before the external viva

1. Start the dashboard in a terminal from `R:\ReatilQ`:

   ```cmd
   .\.venv\Scripts\python.exe -m streamlit run app.py -- --results outputs/uci_schema_demo
   ```

2. Open the dashboard at `http://localhost:8501`. The main customer dashboard defaults to the four-cluster baseline. The advanced Model selection page shows why silhouette scoring selected K=2; the sidebar checkbox can switch to it.
3. Keep the code editor open to `retailiq/pipeline.py`, `retailiq/analytics.py`, `retailiq/advanced.py`, and the relevant model modules. Use Ctrl+F for the function names in this script.
4. Do not start a full advanced rerun during the 30-minute talk; it takes several minutes. Use saved results for the walkthrough.
5. Spark's application UI at `http://localhost:4040` exists only while a Spark application is running. It is different from the RetailIQ dashboard. If you want to show it, run the streaming replay in advance of the demo section and keep that process running; use only one process per streaming checkpoint.
6. Have a backup: screenshots or saved output files. Internet access is not needed after the data and dependencies have been prepared.

## Architecture to put on screen

```text
UCI Online Retail workbook
        |
        | retailiq.data: stream workbook rows into CSV
        v
online_retail.csv --> schema.read_transactions --> Spark DataFrame
                                               |
                                               v
                                  analytics.clean_transactions
                                  type, validate, audit, de-duplicate,
                                  calculate line Revenue
                                               |
                  +----------------------------+--------------------------+
                  |                            |                          |
                  v                            v                          v
       compute_rfm + SQL           country_revenue_rdd             monthly/product
                  |                map/reduceByKey                 aggregations
                  v
       K-Means / customer profiles / similarity
                  |
                  v
       small CSV + JSON outputs --> Streamlit dashboard

Separate advanced branch: FP-Growth | ALS | cohorts | migration |
                            inactivity classifiers | stream replay | benchmark
```

In this project, the real batch analytics and the historical streaming replay are separate Spark jobs. The dashboard reads saved reports; it does not run or retrain Spark models.

### Spark execution components

```text
Python driver process
        |
        +-- SparkSession / SparkContext
        |       builds the plan and submits jobs
        |
        +-- cluster manager in cluster deployments
        |       allocates worker resources
        |
        +-- executors
                run tasks over data partitions
                may cache reused data

Action -> Job -> stages separated around shuffles -> tasks -> executor results

RetailIQ run: local[2] means local worker threads on one computer.
There are no separate worker machines in the measured run.
```

---

# Spoken script

## Member 1 — problem, data, and data quality (0:00–7:30)

### 0:00–0:45 — opening

**Say:**

“Good morning. Our project is RetailIQ, a Big Data Analytics project for retail transaction analysis. We use Apache Spark through PySpark to process invoice-line data and answer four practical questions: who are the retailer’s different customer groups, what products are associated in the same baskets, what products could be recommended, and which customers show a pattern of no purchase in a defined future period? We also demonstrate customer retention, streaming file replay, and Spark execution measurements. Our main goal is to show the full data path and explain the Spark functions that implement it.”

### 0:45–2:30 — business problem and dataset

**Show:** Dashboard overview and `retailiq/data.py`.

**Say:**

“The dataset is the UCI Online Retail transaction dataset. It contains 541,909 invoice-line records from a UK-based online retailer, covering 1 December 2010 through 9 December 2011. A row is a product line on an invoice, not one complete order and not one customer. The fields include InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, and Country. A cancellation invoice can have an invoice number beginning with C. Prices are in pounds sterling.”

“The row grain is important. One invoice can contain several stock codes, and one customer can have many invoices. We preserve CustomerID as a string because it identifies an account; we group by it but never treat its numeric-looking code as a spending amount. StockCode is also an identifier, while Quantity and UnitPrice are measured numeric values. Country is a category useful for geographic aggregation.”

“The original source is an Excel workbook. In `retailiq/data.py`, `convert_workbook` opens it in read-only mode and iterates over rows to write CSV. This is a bounded-memory ingestion/conversion step; it does not first load all 541,909 rows into a pandas DataFrame. In `retailiq/schema.py`, `read_transactions` reads the header and builds an explicit Spark schema. Quantity and UnitPrice are numeric; identifiers such as InvoiceNo and CustomerID stay strings. That matters because invoice and customer codes are identifiers, not quantities to add.”

### 2:30–4:30 — quality and cleaning

**Show:** Dashboard “Method & data quality”; open `clean_transactions` in `retailiq/analytics.py`.

**Say:**

“The raw file has 541,909 rows. Our cleaning output has 392,692 positive-purchase rows, 4,338 customers, and 18,532 distinct invoices. The audit is mutually exclusive: 135,080 rows have a missing customer, 8,905 are cancellations, 40 have invalid prices, 5,192 are exact duplicates, and 392,692 remain. These counts reconcile exactly to the raw total. Other rejection categories in the code had zero records in this run.”

“Missing customer IDs are a large group. Those rows may still be valid sales, but they cannot be used for customer-level RFM or personalized recommendations, so this analysis excludes them. That is a scope decision, not a claim that those transactions have no business value. A production report could show transaction totals both with and without identified-customer filters.”

“The function `clean_transactions` first checks the required columns. It trims identifiers, casts Quantity and UnitPrice, and tries supported timestamp formats. It then uses a chained `when` expression to give each rejected row one primary reason. This ordering prevents one row from appearing in several audit categories. It excludes missing customers and identifiers, cancellations, invalid dates, nonpositive or non-finite quantity and price, and then removes exact duplicates. It calculates line Revenue as quantity multiplied by unit price and stores it as `decimal(20,2)` to retain money precision.”

“We exclude returns and cancellations from positive-purchase analysis. Therefore our £8,887,208.89 total is purchase revenue, not net revenue. Duplicate removal assumes identical normalized rows are accidental. We state both assumptions because they affect the result.”

### 4:30–6:30 — Big Data framing

**Say:**

“We use the common 5V framework carefully. Volume is 541,909 transaction lines: useful for a Spark demonstration, but not so large that pandas cannot handle it on a suitable computer. We do not claim this dataset proves Spark was required by its size. Velocity is represented only by replaying historical events in small file-based micro-batches; it is not a live retailer feed. Variety is limited because the main source is structured transaction data, although it has categorical, numeric, and timestamp fields. Veracity is important because of missing customer IDs, cancellations, invalid prices, and duplicate rows. Value comes from turning transactions into customer profiles, product associations, and recommendations.”

“The Big Data value in our project is using Spark’s distributed processing APIs and showing how the pipeline could be run on a cluster, while being honest that our measured run uses local mode.”

### 6:30–7:30 — transition

**Say:**

“We have now introduced the source and the cleaned-data contract. The next part explains how Spark loads this data, what its components do, and how a PySpark operation becomes distributed work.”

---

## Member 2 — Spark, PySpark, architecture, and core pipeline (7:30–15:00)

### 7:30–9:00 — what Apache Spark and PySpark are

**Show:** Architecture figure; then `retailiq/runtime.py` and `retailiq/pipeline.py`.

**Say:**

“Apache Spark is a distributed data-processing engine. It provides APIs for processing data in parallel, SQL and DataFrames, machine learning, and stream processing. PySpark is Spark’s Python API. An API is the set of methods and objects a program uses to request a library’s functionality. In our code, examples include `spark.read`, `DataFrame.groupBy`, `withColumn`, `Pipeline.fit`, and `writeStream`.”

“Our environment installs PySpark 3.5.6 in the project’s virtual environment from the dependency files, with Python 3.11 and JDK 17. PySpark is not a second Python-only Spark engine: Python code uses the PySpark API and communicates with Spark’s JVM runtime through Py4J. `create_spark` in `retailiq/runtime.py` builds the Spark session. It names the application, selects `local[2]`, sets four SQL shuffle partitions, applies runtime options, and gets or creates the session. On Windows it also registers the project’s local filesystem adapter, which is needed for Spark’s Hadoop-style writes.”

“A repeatable setup uses the repository setup script, which creates `.venv` and installs the locked requirements. The command `python -m retailiq.advanced` tells Python to load the `retailiq.advanced` module as the driver program. The code imports PySpark classes, then creates the session before reading the data.”

### 9:00–11:00 — components

**Say:**

“The driver is the process running our Python application. `SparkSession` is the entry point used by our code for DataFrames and SQL; it provides access to Spark’s execution context. In a cluster deployment, a cluster manager such as Spark Standalone, YARN, or Kubernetes allocates resources. Executors run tasks and can keep intermediate data in memory or on disk. A partition is a chunk of a dataset; a task is a unit of work on a partition.”

“Our actual setting is `local[2]`: the driver and Spark work run on one computer with two local worker threads. We do not have separate worker machines, and we should not describe this as a cluster or claim measured scale-out. The same application design can connect to a real cluster manager in a different deployment.”

“A Spark action creates a job. Spark divides a job into stages, commonly separated by shuffle boundaries, and schedules tasks for partitions. A `groupBy`, `distinct`, or global sort may need a shuffle: records move between partitions so matching keys can be combined. That is why a group-by is more expensive than a simple projection.”

“There are two meanings of stage in our project. A Spark execution stage is part of a job’s physical execution plan. An MLlib Pipeline stage is a model component, such as a feature assembler, scaler, or estimator. In our K-Means pipeline, VectorAssembler and StandardScaler prepare features and KMeans is the estimator. `Pipeline.fit` trains the estimator and returns a fitted PipelineModel; `transform` applies the saved transformations and model to rows.”

### 11:00–13:30 — loading, laziness, and core functions

**Show:** `pipeline.main`, `schema.read_transactions`, then `analytics.clean_transactions` and `compute_rfm`.

**Say:**

“The entry point is `python -m retailiq.pipeline`. `pipeline.main` parses the input and output arguments, creates the session, calls `read_transactions`, and coordinates the baseline analysis. The CSV reader gives Spark a schema and returns a Spark DataFrame. It does not convert the whole dataset to pandas.”

“Most DataFrame transformations are lazy. `select`, `filter`, `withColumn`, `groupBy`, and `join` describe a plan. Actions such as `count`, `first`, `collect`, a write, or model fitting cause Spark to execute the necessary parts of that plan. Spark SQL can optimize structured DataFrame operations. We call `cache` where a result is reused, and `unpersist` when we are finished with it.”

“`compute_rfm` derives customer-level values. It finds each customer’s latest purchase date, counts distinct invoice numbers, and sums line revenue. It uses Spark SQL and DataFrame aggregations. It calculates days since last purchase using a reference date of 10 December 2011. `approxQuantile` estimates quintile cut points for R, F, and M scores. Recency is reversed so more recent customers receive a higher recency score. Frequency and monetary scores increase with the value.”

### 13:30–15:00 — APIs and transitions

**Say:**

“RetailIQ primarily uses the DataFrame and Spark SQL APIs because the data has named columns and these APIs support query optimization. We also include one explicit RDD example in `country_revenue_rdd`: select Country and Revenue, convert to `.rdd`, use `map` to form key-value pairs, `reduceByKey` to sum revenue by country, `sortBy` to order it, and `collect` the small final answer to the driver. The RDD API is useful to show a lower-level distributed collection, although Spark’s DataFrame-based MLlib API is the primary API for our models.”

“The key idea is: the driver describes the work, Spark builds a plan, the scheduler divides executable work into tasks, and executors process data partitions. The next presenter will show what features we derive and what each model or analytic actually does.”

---

## Member 3 — feature engineering, models, and results (15:00–22:30)

### 15:00–16:45 — RFM and customer segmentation

**Show:** Dashboard segment overview and `cluster_customers`; show K=4 default and then Model selection page.

**Say:**

“The main customer features are RFM: Recency, Frequency, and Monetary value. Recency is the number of days since the customer’s last purchase as of the reference date. Frequency is the number of distinct invoices, not the number of product lines. Monetary value is the customer’s total positive purchase revenue. We extract these because they summarize recentness, repeat behavior, and value without using a customer ID as a numeric feature.”

“In `cluster_customers`, we apply `log1p` to reduce skew, assemble the three values into a vector with `VectorAssembler`, standardize them with `StandardScaler`, and fit Spark MLlib K-Means. Scaling matters because K-Means uses distances, and a large monetary scale could otherwise dominate recency and frequency. `model.transform` adds a cluster assignment. We summarize each cluster’s RFM profile and apply readable business rules to label it Developing, At risk, Loyal, or Champions. K-Means itself returns cluster numbers; the business names are our later interpretation.”

“For our K=4 baseline, Developing has 862 customers and about 5.3 percent of the purchase revenue; At risk has 1,593 customers and about 6.2 percent; Loyal has 1,168 customers and 23.4 percent; Champions has 715 customers and about 65.1 percent. These profiles are more helpful for a business conversation than saying only ‘cluster zero’ or ‘cluster one.’ The K-Means ID itself has no fixed meaning.”

“The dashboard defaults to the K=4 baseline for four more actionable profiles. In the advanced comparison, K=2 has a stronger average silhouette of 0.624 than K=4 at about 0.495, so it is available for comparison. That does not mean K=2 is automatically the best business choice; it simply separates the data better under this distance metric. The K comparison tests K=2 through 8 over three seeds and checks silhouette, cluster sizes, and adjusted Rand stability.”

### 16:45–18:45 — basket analysis and recommendations

**Show:** Basket analysis, then Product recommendations.

**Say:**

“The next analytics answer product questions. `market_baskets` makes unique StockCode sets per invoice and fits FP-Growth. Support is how often an itemset occurs; confidence is how often B occurs in baskets with A; lift compares that confidence with B’s general popularity. We use two percent support, 25 percent confidence, and lift above one. The run exported 81 rules. These associations can suggest bundle tests; they do not prove causation.”

“`train_recommendations` counts distinct invoices per customer-product pair and fits implicit-feedback ALS. ALS learns latent user/product factors and preference scores, not purchase probabilities. We train before 11 October and evaluate novel purchases in the final 60 days. Precision@5 is 5.33 percent versus 4.50 percent for a train-only popularity baseline; recall@5 is 1.72 versus 1.15 percent. ALS has recommendations for 1,698 of 2,295 evaluated customers, about 74 percent coverage. Mention cold-start customers and modest absolute performance.”

### 18:45–20:45 — inactivity classifiers and model comparison

**Show:** Inactivity prediction page and `model_comparison.csv`; point to `retailiq/prediction.py`.

**Say:**

“Our supervised task predicts a specific observed outcome: no purchase in the next 60 days. It does not prove permanent churn. We derive six historical features: log Recency, log Frequency, log Monetary value, log Tenure since first observed purchase, log number of distinct products, and log average order value. Customer ID is not used as a predictive feature.”

“We compare three MLlib classifiers on the same time-ordered holdout. Logistic Regression is a weighted linear baseline; Random Forest combines trees to capture nonlinear rules; Gradient-Boosted Trees add trees sequentially to correct earlier errors. Training snapshots are 13 June and 12 August 2011; the holdout is 11 October through 9 December. This avoids mixing future rows into training.”

“Logistic Regression was selected by our declared primary rule, highest PR-AUC: 0.7119, versus 0.7070 for Random Forest and 0.7049 for Gradient-Boosted Trees. Random Forest had a slightly higher ROC-AUC, 0.7384 versus 0.7374 for Logistic Regression. Gradient-Boosted Trees had the highest F1, 0.7366, narrowly above Random Forest at 0.7353. We select on PR-AUC because it summarizes precision and recall for the positive no-purchase outcome. The differences are small, so we say Logistic Regression wins under our rule, not that it is universally superior. It had 68.14 percent accuracy versus a 52.45 percent majority-class baseline. Latest risk scores are for an unlabelled future and are not assumed calibrated.”

### 20:45–22:30 — other useful analysis and model terms

**Say:**

“FP-Growth and K-Means are not supervised classifiers. We also calculate 13 first-observed-purchase-month retention cohorts and retrospective segment migration using one frozen model; migration is descriptive, not a historical forecast. Similar-customer analysis uses cosine similarity between product-quantity vectors. These methods answer distinct retail questions, so we do not add unrelated algorithms merely to inflate the model count.”

“The next presenter will explain the streaming feature and distinguish the streaming demonstration from the batch model pipeline.”

---

## Member 4 — streaming, Big Data trade-offs, live demo, conclusion (22:30–30:00)

### 22:30–24:30 — batch versus streaming

**Show:** Streaming replay page and `retailiq/streaming.py`.

**Say:**

“Batch processing works over a bounded set of stored data. Our main `pipeline.py` run reads the transaction CSV, cleans all rows, computes RFM, clusters, calculates summaries, and writes output files. Streaming processes newly available data incrementally. In our project, `streaming.py` stages historical transaction events into files and reads that inbox using Structured Streaming. It is a replay, not a live connection to a shop, Kafka, or a socket.”

“The streaming source has an explicit schema. `readStream` reads files with at most one new file per trigger. The query applies a two-day watermark, groups by one-day event-time window and Country, and calculates purchase-row count and revenue. `foreachBatch` calls `save_batch` to write each result snapshot. `checkpointLocation` records progress and state. We process all available files, stop and restart the same query, and reconcile the results.”

“The saved run replayed three batches of 2,000 events: 6,000 rows and £130,841.06. The result reconciled to the published input and checkpoint recovery did not duplicate or lose rows. Important detail: the query uses complete output mode, which preserves all aggregate totals; the watermark does not evict old aggregation state in complete mode. We present it as a bounded teaching demonstration, not a design for an indefinite production stream.”

### 24:30–26:00 — outputs, UI, and demo

**Show:** Dashboard overview, model selection, then streaming page.

**Say:**

“The baseline output folder contains `summary.json` plus customer, segment, monthly revenue, country revenue, top product, and similar-customer CSV files. The advanced folder contains the manifest `advanced_summary.json`, analysis CSVs, comparison reports, and saved models. The Streamlit frontend reads these results using pandas for display. It does not start Spark or train a model when we open a page.”

“In our demo we first show four K=4 customer profiles. Then we open Model selection to compare K values and explain why silhouette favors two broad clusters. We open the basket page to explain support, confidence, and lift; recommendations to compare ALS with popularity; and inactivity prediction to show the three classifiers and time split. We finish at streaming replay for checkpoint recovery. If a Spark job is active, we can open port 4040 to inspect jobs, stages, tasks, executors, and storage. Port 4040 is Spark’s application UI, not our website, and it usually disappears after the Spark process stops.”

### 26:00–28:00 — why Spark, alternatives, strengths and limitations

**Say:**

“Why Spark instead of only pandas? Pandas is excellent for smaller datasets and local interactive analysis. It usually expects data and operations to fit on one machine’s memory. Spark gives us distributed DataFrames and SQL, lazy query planning, partition-based parallel tasks, MLlib, and Structured Streaming in one engine. In our project pandas only displays the compact exported reports; Spark handles the transaction cleaning and analysis. The honest caveat is that this dataset fits on one machine, and our run is local[2], so Spark is chosen to demonstrate the Big Data architecture and APIs, not because pandas could not process 541,909 rows.”

“Hadoop MapReduce is another distributed approach, often expressed as map and reduce jobs with more disk-oriented intermediate processing. Spark offers a richer unified API and can cache reused data, although it still performs shuffle and disk I/O when needed. Apache Flink is another strong streaming engine and may be preferred for some low-latency streaming systems. Kafka is not a direct Spark replacement; it is commonly used as an event broker/source with a processor such as Spark or Flink. We chose Spark because the project needs batch SQL, MLlib, and a Structured Streaming demonstration together, and it is the required technology we are studying.”

“Spark’s strengths include scalability across partitions, fault-tolerant execution, SQL/DataFrame optimization, caching for reuse, and integrated analytics APIs. Limitations include JVM and cluster configuration, shuffle/network costs, memory tuning, small-data overhead, and the fact that bad partitioning or skew can slow a job. DataFrame operations are usually preferable to Python UDFs because Spark can understand and optimize built-in expressions.”

### 28:00–30:00 — closing summary

**Say:**

“To summarize: our project ingests a public retail transaction dataset, applies a transparent Spark cleaning and audit process, engineers customer and basket features, then uses Spark SQL, DataFrames, RDDs, MLlib, and Structured Streaming. Our four baseline customer groups are Developing, At risk, Loyal, and Champions. The advanced selection experiment found K=2 has higher silhouette, while the dashboard keeps K=4 as the more detailed business view. We compared three future no-purchase classifiers and selected Logistic Regression by PR-AUC, while reporting the close alternatives. We also honestly label the streaming source as historical replay and the Spark run as local mode. Our main contribution is an end-to-end, explainable Big Data Analytics pipeline with measurable outputs and explicit limitations. Thank you; we are ready for questions.”

---

# Live code map: exact files and functions

| Viva topic | File / function | What to point at |
| --- | --- | --- |
| Download and convert UCI workbook | `retailiq/data.py` → `convert_workbook`, `main` | Read-only workbook iteration and CSV output |
| Define typed CSV input | `retailiq/schema.py` → `transaction_schema`, `read_transactions` | Explicit schema: quantity and price numeric, identifiers strings |
| Create Spark runtime | `retailiq/runtime.py` → `create_spark` | SparkSession, local master, shuffle partitions and Windows adapter |
| Orchestrate baseline | `retailiq/pipeline.py` → `main` | Read → clean → RFM → models/aggregates → exports → `spark.stop()` |
| Clean and audit | `retailiq/analytics.py` → `clean_transactions` | Cast, parse timestamps, chained `when`, cache, count, duplicates, Decimal revenue |
| RFM feature engineering | `retailiq/analytics.py` → `compute_rfm` | Group by CustomerID, max date, distinct invoice count, sum revenue, quantile scores |
| Segment model | `retailiq/analytics.py` → `cluster_customers` | `log1p`, VectorAssembler, StandardScaler, KMeans fit/transform, silhouette |
| Choose K and assess stability | `retailiq/experiments.py` → `compare_k` | K=2–8, seeds, silhouette and ARI |
| Basket rules | `retailiq/commerce.py` → `market_baskets` | Invoice basket, FP-Growth, support/confidence/lift |
| Product recommendations | `retailiq/commerce.py` → `train_recommendations` | Customer/product indices, implicit ALS, temporal holdout, ranking metrics |
| Inactivity model comparison | `retailiq/prediction.py` → `inactivity_prediction` | Six features, chronological labels, three classifiers, PR-AUC selection, final refit |
| RDD example | `retailiq/analytics.py` → `country_revenue_rdd` | `.rdd.map().reduceByKey().sortBy().collect()` |
| Cohorts and migration | `retailiq/longitudinal.py` → `retention_cohorts`, `monthly_migration` | Monthly activity and retrospective transitions |
| Streaming replay | `retailiq/streaming.py` → `replay_stream`, nested `save_batch` | ReadStream, watermark, event-time window, foreachBatch, checkpoint, recovery |
| Advanced orchestration | `retailiq/advanced.py` → `main` | Feature dispatch, resume manifest, per-stage errors and metrics |
| Dashboard | `app.py`, `retailiq/dashboard.py` | Reads outputs, presents charts; does not run Spark training |

# Numbers to keep consistent

| Measure | Current saved result |
| --- | ---: |
| Raw transaction lines | 541,909 |
| Clean positive purchase lines | 392,692 |
| Excluded missing-customer rows | 135,080 |
| Excluded cancellations | 8,905 |
| Excluded invalid-price rows | 40 |
| Exact duplicate rows removed | 5,192 |
| Customers | 4,338 |
| Distinct invoices | 18,532 |
| Positive purchase revenue | £8,887,208.89 |
| Baseline clusters | K=4; four populated clusters |
| Advanced selected K | K=2; mean silhouette 0.62354; mean ARI 1.0 |
| Basket rules exported | 81 |
| ALS precision@5 | 5.33%; popularity baseline 4.50% |
| Inactivity holdout | 3,741 customers; 60-day outcome |
| Selected inactivity model | Logistic Regression; PR-AUC 0.71190 |
| Random Forest comparison | ROC-AUC 0.73841; PR-AUC 0.70702 |
| Gradient-Boosted Trees comparison | ROC-AUC 0.73728; PR-AUC 0.70492 |
| Streaming replay | 6,000 rows / 3 batches; £130,841.06; reconciled and checkpoint restart verified |

Do not mix baseline K=4 statistics with advanced K=2 statistics. Do not describe the selected classifier as the winner on every metric: Logistic Regression is selected by PR-AUC, while Random Forest has the highest ROC-AUC and Gradient-Boosted Trees has a marginally higher F1 in this saved run.

# Short viva answer bank

**Is this really Big Data?**  
The dataset has about 542 thousand rows, which is moderate and can fit on one computer. We use Spark to demonstrate distributed processing concepts and an architecture that can run on a cluster; we do not claim this particular local run needed a cluster.

**Are you doing live streaming?**  
No. Structured Streaming reads historical events from staged CSV files as micro-batches. There is no live POS, Kafka, or cloud-feed connector.

**What is the difference between a transformation and an action?**  
A transformation describes a new distributed dataset; an action requests a result or output and causes the necessary Spark work to execute.

**What are a job, stage, and task?**  
A job is created by an action. Spark divides it into stages, commonly at shuffle boundaries. A task is a unit of execution for a partition in a stage.

**What does “stage” mean in MLlib versus Spark execution?**  
An MLlib Pipeline stage is a component such as VectorAssembler, StandardScaler, or KMeans. A Spark execution stage is a phase in the DAG scheduler’s plan, separated from another stage when data needs to shuffle. They are two different uses of the word.

**What is a shuffle and why care?**  
A shuffle moves records between partitions so rows with matching keys can meet for grouping or joining. It uses network and memory/disk resources and can become a bottleneck.

**Why use an explicit schema?**  
It avoids type guessing, preserves identifiers as strings, supports reordered source columns, and makes invalid numeric values visible to the cleaning audit.

**Why use RFM?**  
It summarizes customer purchase behavior with recentness, repeat frequency, and spend using interpretable features from the transaction history.

**Why standardize before K-Means?**  
K-Means is distance-based. Log transformation reduces skew, and standardization stops a numerically large feature from dominating distance calculations.

**Does K=2 prove two customer types are best?**  
No. It has the best silhouette in this comparison, but it creates broad groups. The main business dashboard defaults to K=4; we compare separation with business usefulness.

**Does a cluster number have meaning?**  
No. Cluster IDs are arbitrary. We describe clusters from their average RFM profiles and should compare the profiles, not the numeric ID.

**Is an FP-Growth rule causal?**  
No. It means two products co-occurred more often than a baseline association measure suggests. It motivates an experiment; it does not prove one product causes another purchase.

**Are ALS scores probabilities?**  
No. They are model preference scores. We evaluate the ranked list against later novel purchases and compare it with popularity.

**Why PR-AUC for inactivity?**  
It summarizes the precision–recall trade-off for the positive no-purchase class and is informative when class balance matters. We report ROC-AUC and threshold-based metrics too.

**Is no purchase equal to churn?**  
No. It is no observed purchase within a chosen 60-day horizon in this limited dataset. A customer can return later.

**Why does the Spark UI not open after the job ends?**  
The application UI is served by the active Spark driver, usually on port 4040. When the application stops, that UI closes. It is not the Streamlit dashboard.

**Why not pandas only?**  
Pandas is a good choice for this dataset on one machine. Spark is used to practice distributed DataFrame/SQL, MLlib and streaming APIs. In RetailIQ, pandas is only used to display compact results.

**What is the biggest limitation?**  
The project runs locally on one computer, uses a historical dataset, and its streaming input is file replay. Its predictive results have limited temporal validation and should not be treated as production-ready business decisions.

# Official references

- [UCI Online Retail dataset](https://archive.ics.uci.edu/dataset/352/online-retail)
- [Apache Spark 3.5.6 cluster overview](https://spark.apache.org/docs/3.5.6/cluster-overview.html)
- [Apache Spark 3.5.6 SQL, DataFrames, and Datasets](https://spark.apache.org/docs/3.5.6/sql-programming-guide.html)
- [Apache Spark 3.5.6 RDD programming guide](https://spark.apache.org/docs/3.5.6/rdd-programming-guide.html)
- [Apache Spark 3.5.6 MLlib guide](https://spark.apache.org/docs/3.5.6/ml-guide.html)
- [Apache Spark 3.5.6 Structured Streaming guide](https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html)
- [Apache Spark 3.5.6 PySpark installation guide](https://spark.apache.org/docs/3.5.6/api/python/getting_started/install.html)
- [Apache Spark 3.5.6 Web UI guide](https://spark.apache.org/docs/3.5.6/web-ui.html)
