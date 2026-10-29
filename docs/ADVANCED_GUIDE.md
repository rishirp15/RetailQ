# RetailIQ advanced features

The eight extensions use the same cleaned UCI transactions as the baseline. Training runs outside the dashboard; opening a page reads exported results and does not retrain anything.

## Run everything

From PowerShell with the project environment installed:

```powershell
.\scripts\run.ps1 -Advanced
```

This runs the baseline, runs/resumes the advanced stages, then opens the dashboard. On a machine already set up with baseline results, use these individual commands to avoid repeating the baseline:

```powershell
$env:JAVA_HOME = 'C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot' # adjust to your JDK 17 folder
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
.\.venv\Scripts\python.exe -m retailiq.advanced
.\scripts\run.ps1 -DashboardOnly
```

Expect several minutes for the full run. Completed stages are reused after an interruption. The manifest records status, parameters, source hash and measured metrics for each feature. Failed stages remain visibly failed and can be retried. Changes to input or analytic settings require a new output folder to prevent mixing incompatible reports.

To intentionally recompute selected stages:

```powershell
.\.venv\Scripts\python.exe -m retailiq.advanced --features recommendations --rerun
```

Run with the original settings when resuming. For example, a separate sensitivity experiment:

```powershell
.\.venv\Scripts\python.exe -m retailiq.advanced --output outputs/experiments/support03 --min-support .03
```

The dashboard uses `advanced/` under its selected baseline results folder. Separate experiment folders can be inspected directly as reports; they are not automatically attached to an unrelated baseline.

## 1. Automatic K comparison and stability

**Module:** `retailiq/experiments.py`  
**Outputs:** `k_runs.csv`, `k_comparison.csv`, `selected_customers.csv`, `selected_segments.csv`, `models/kmeans/`.

- Fit log-RFM preprocessing once, then compare K=2 through K=8 with seeds 42, 7 and 21: 21 real MLlib experiments.
- Measure squared-Euclidean silhouette, training cost, smallest/largest cluster and elapsed experiment time.
- Compare seeded assignments with adjusted Rand index (ARI). Only the small cluster contingency table is collected; customer IDs are never used as numeric features.
- Select the highest mean silhouette among K values for which every seed populates all K clusters and the smallest cluster contains at least 1% of customers, with a minimum of two customers. If none qualifies, use the highest mean score. Break mean-score ties in favor of smaller K. Choose the highest-silhouette seed within the selected K.
- The sidebar defaults to selected clusters once ready and provides a checkbox to return to the original K=4 baseline.

**Interpretation:** stability and geometric separation do not prove marketing usefulness. A two-cluster solution may separate broad groups well while four clusters provide more actionable detail. The original baseline findings remain available in `docs/RESULTS.md`.

## 2. Market basket analysis

**Module:** `retailiq/commerce.py`  
**Outputs:** `basket_rules.csv`, `frequent_itemsets.csv`, `product_catalog.csv`.

Spark FP-Growth receives a unique product set per customer/invoice. Defaults: minimum support 2%, minimum confidence 25%; exported rules must have lift above 1. All rules are mined before the best 300 are exported, ranked by lift and support. The dashboard translates stock codes into product descriptions and filters the exported rules.

- Support(A→B) = baskets containing A and B / all baskets.
- Confidence(A→B) = baskets containing A and B / baskets containing A.
- Lift(A→B) = confidence(A→B) / support(B).

Multiple copies of one product do not count as multiple basket items. Rules suggest bundle experiments, not causal effects. Lower support can produce many more rules and substantially increase computation.

## 3. Personalized recommendations

**Module:** `retailiq/commerce.py`  
**Outputs:** `recommendations.csv`, `models/als/`; evaluation metrics in the advanced manifest.

Spark implicit ALS uses distinct purchase invoice counts for each customer/product, with rank 12, alpha 10, regularization 0.1, ten iterations and seed 42. Customer and product codes receive deterministic integer IDs; identity mapping does not use future purchase frequencies. Small ID catalog indexing uses a global sort; purchase aggregation and model fitting use Spark.

Evaluation trains before the final 60 days and tests later purchases. Relevant items are products bought in the holdout that were not bought by that customer during training. Report macro precision@5, recall@5, NDCG@5 and hit rate@5 against a train-only popularity baseline. Cold-start customers remain in the denominator with zero ALS hits; coverage and known/unknown product-user counts are reported. Precision uses a denominator of five even when fewer recommendations are possible.

After evaluation, refit on all observed purchases for the displayed recommendations. Remove previously purchased products. Candidate counts are bucketed by customer history size so large wholesale baskets do not require thousands of candidates for every customer. Scores are preference strength, not calibrated purchase probabilities. This evaluates novel-product discovery, not repeat-purchase prediction.

## 4. Retention cohorts

**Module:** `retailiq/longitudinal.py`  
**Output:** `retention_cohorts.csv`.

Assign each customer to their first observed purchase month. For each later observed month, divide purchasing customers by original cohort size. Month zero is 100% by construction. Observed months with no returning customers are zero; future months are omitted, not zero-filled. Mark the last observed month as partial.

This is observational monthly repeat purchasing: a customer may miss a month and return later. The data's first month is not proof of a customer's first-ever purchase outside the dataset.

## 5. Monthly segment migration

**Module:** `retailiq/longitudinal.py`  
**Outputs:** `monthly_customers.csv`, `monthly_segments.csv`, `segment_migration.csv`.

Build cumulative customer RFM snapshots at monthly boundaries plus the final dataset snapshot. Reuse the selected full-period model, scaler and cluster descriptions across every snapshot, then compare consecutive assignments. This avoids comparing unrelated cluster numbers from separately fitted models. Display a Sankey diagram, membership trend and individual customer history.

The analysis is retrospective and descriptive: its frozen model was fitted on the full period. It must not be presented as a prediction available at the historical snapshot. New customers appear in membership totals but have no previous segment and are excluded from that first transition.

## 6. Future inactivity prediction

**Module:** `retailiq/prediction.py`  
**Outputs:** `inactivity_risk.csv`, `inactivity_holdout.csv`, `feature_importance.csv`, `models/inactivity/`.

Label = no observed positive purchase within the following 60 days. Features use only purchases strictly before each cutoff: log recency, invoice frequency, monetary value, tenure, product diversity and average order value. Customer identity is excluded.

For the UCI end date, training snapshots are 13 June and 12 August 2011, with all training outcomes ending before 11 October. The held-out snapshot is 11 October, with outcomes observed until 10 December (exclusive). There is no random transaction split. Customers can occur at multiple historical snapshots; this evaluates later behavior of an existing customer population, not generalization to entirely new identities.

Fit a 60-tree random forest (depth 5, minimum five examples per leaf). Report ROC AUC, PR AUC, precision, recall, F1, accuracy, majority-class baseline, class prevalence, Brier score and confusion matrix at threshold 0.5. Feature importance refers to the historical evaluation model.

After evaluation, refit using every fully labelled snapshot for scores at the latest reference date. Their future outcomes are unobserved. The dashboard threshold filters this latest risk list; it does not change the reported holdout confusion matrix. Scores are uncalibrated, and 60-day non-purchase is not proof of permanent churn.

## 7. Structured Streaming replay and recovery

**Module:** `retailiq/streaming.py`  
**Outputs:** `stream_events.csv`, `streaming/inbox/`, `streaming/checkpoint/`, per-batch snapshots, `streaming/stream_totals.csv`, `streaming/summary.json`, `streaming/progress.json`.

The full advanced run sorts cleaned historical events and sends the first three batches of 2,000 rows through a real `readStream` → event-time window aggregation → `foreachBatch` query. Each micro-batch updates cumulative daily/country revenue and line counts. Validate totals against the exact published source rows, stop the query, restart from the same checkpoint, and verify unchanged totals without publishing any new events.

**This is a simulated historical feed, not live retail data.** Stable source filenames and checkpoint offsets prevent the same file being counted twice on restart. The sink replaces its full snapshot atomically, so retries of the same batch do not append duplicate totals. This does not deduplicate arbitrary repeated events arriving under new filenames.

The query uses one-day event-time windows, a two-day watermark and complete output mode. Complete mode retains all windows, so the watermark does not evict historical state. This is suitable for the bounded mini-project demonstration, not indefinite production streaming. Purchase-row count is not distinct invoice count.

Extend the same replay and leave the query watching for files:

```powershell
.\scripts\run.ps1 -Streaming -StreamBatches 6
```

Or:

```powershell
.\.venv\Scripts\python.exe -m retailiq.streaming --batches 6 --interval 3 --watch
```

`--batches` means the total file count to publish, including already-published files. Keep the same source and `--batch-rows` when resuming; use a new directory for another replay. Watch mode continues until Ctrl+C. The streaming dashboard refreshes every five seconds while open. Avoid running two replay processes against the same checkpoint simultaneously.

## 8. Spark performance study

**Module:** `retailiq/benchmark.py`  
**Outputs:** `performance.csv`, `benchmark_data/csv/`, `benchmark_data/parquet/`, `benchmark_data/execution_plans.txt`.

Spark writes equivalent cleaned projections as CSV and Parquet. Compare a country count/revenue aggregation with 1, 2 and 4 partitions, with and without a materialized Spark cache, repeating each case twice: 24 measured queries. Disable adaptive partition coalescing during measurement so requested partition differences remain meaningful; restore the original setting afterward.

All variants must produce exactly identical country counts and Decimal revenue sums. Report query time, cache-build time, combined time, row count and bytes on disk. Cache-build cost is excluded from the warm-cache query and shown separately. Alternate format order across repetitions and save logical/physical plans.

These are small local measurements with warm operating-system caches, possible concurrent load, and limited repetitions. They do not establish cluster speedup, cold-disk performance or a universal best partition count. More partitions can be slower on a two-thread laptop.

## Windows storage adapter

The basic pipeline did not need Hadoop output writers. Models, Parquet and Structured Streaming do. Windows normally requires native Hadoop helpers for POSIX permission and symlink operations, which were absent on this machine.

`retailiq/runtime.py` compiles the small reviewed Java sources in `java/retailiq/fs/` against the installed PySpark JARs using JDK 17. The adapter uses Java NIO for directory listing, status and renaming, preserves existing NTFS ACLs, does not implement POSIX chmod, and advertises no symlink support. The JAR is content-versioned under `.runtime/` and added to the local driver/executor classpaths. It is limited to local Windows Spark. Linux uses Spark's normal filesystem implementation; use Linux for an actual cluster or production deployment.

No unofficial `winutils.exe`, downloaded Hadoop DLL, permanent system change or administrator installation is required.

## Demonstrate and test

```powershell
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe scripts/verify_results.py
.\.venv\Scripts\python.exe scripts/verify_dashboard.py
.\.venv\Scripts\python.exe -m retailiq.verify_models
```

Tests check original cleaning/RFM/similarity calculations, ARI label invariance, ranking cold-start penalties, cohort zero/future cells, temporal feature isolation, FP-Growth rule values, migration population conservation, and real streaming checkpoint restart/resume/extension with exact decimal totals.

The three verification commands require completed UCI outputs. They reconcile cross-feature exports and timestamp-preserving replay, exercise all dashboard pages and key filters, and reload persisted models to check their behavior.

Suggested presentation order: selected K and stability → product associations → personalized recommendations and baseline → retention heatmap → segment migration → inactivity holdout → streaming recovery → measured Spark performance. Explain evaluation limitations alongside each result.
