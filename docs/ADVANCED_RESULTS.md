# Measured advanced UCI results

These recorded results come from earlier completed Spark jobs on the same 392,692 cleaned positive-purchase rows, 4,338 customers and 18,532 invoices as the baseline. Monetary totals remain £8,887,208.89, excluding returns and cancellations. The inactivity metrics below are the earlier Random Forest-only run; the current code compares Logistic Regression, Random Forest and Gradient-Boosted Trees. Rerun the prediction feature before presenting comparative model results.

| Feature | Measured result |
| --- | --- |
| K comparison | 21 fits: K=2–8, seeds 42/7/21 |
| Selected K | 2, mean silhouette 0.62354, seed stability ARI 1.000 |
| Basket analysis | 81 rules with lift >1, support ≥2%, confidence ≥25% |
| Final recommendations | 21,690 unseen-product recommendations, five for each of 4,338 customers |
| Retention cohorts | 13 first-purchase monthly cohorts |
| Segment migration | 13 monthly/final snapshots using one frozen model |
| Inactivity prediction | ROC AUC 0.73840; PR AUC 0.70702 on a later-period holdout |
| Streaming | 3 × 2,000 purchase rows; £130,841.06; totals reconciled and checkpoint restart verified |
| Performance | 24 measured queries, with equivalent counts and exact Decimal revenue sums |

## Recommendation evaluation

Training purchases occur before 11 October 2011; holdout purchases occur from that date until 10 December, exclusive. Evaluation covers 2,295 customers with novel holdout products, including cold-start customers with zero ALS hits.

| Metric | Implicit ALS | Train-only popularity baseline |
| --- | ---: | ---: |
| Precision@5 | 5.333% | 4.497% |
| Recall@5 | 1.721% | 1.153% |
| NDCG@5 | 0.05789 | 0.04978 |
| Hit rate@5 | 19.390% | 18.606% |

ALS supplied holdout recommendations for 1,698 of the 2,295 evaluated customers (73.99% coverage). There were 67,283 novel holdout customer-product pairs, of which 43,113 had both a customer and product represented in training. Novel-product recommendation remains difficult; the absolute scores should not be described as high accuracy. After evaluation, a separate model is refitted on all observed data for the dashboard.

## Inactivity prediction

Training snapshots: 13 June and 12 August 2011. Latest training labels end before the 11 October holdout snapshot. Holdout outcomes run for 60 days, ending 10 December exclusive.

| Metric | Result |
| --- | ---: |
| Holdout customers | 3,741 |
| Non-purchase prevalence | 52.45% |
| Accuracy at threshold 0.5 | 67.79% |
| Majority-class baseline accuracy | 52.45% |
| Precision | 64.61% |
| Recall | 85.32% |
| F1 | 0.73534 |
| Brier score | 0.20565 |

The confusion matrix contains 862 true negatives, 917 false positives, 288 false negatives and 1,674 true positives. Positive means no purchase observed during the following 60 days. The latest snapshot produces scores for all 4,338 customers; at score ≥0.7, 1,448 are flagged. These scores are uncalibrated and their future outcomes are outside the dataset.

## Selected clusters versus the original baseline

The automatic criterion favors K=2: 2,670 customers in a broad at-risk/inactive cluster (14.87% of purchase revenue) and 1,668 in a broad Champions cluster (85.13%). This does not invalidate the original four-cluster analysis, which gives finer business distinctions. Use the sidebar checkbox to compare them. Do not combine customer counts or revenue shares from different K values into one segmentation result.

## Local performance observations

Median query times across the measured partition configurations and two repetitions:

| Format | Without Spark cache | With materialized Spark cache |
| --- | ---: | ---: |
| CSV | 1.334 s | 0.267 s |
| Parquet | 0.726 s | 0.275 s |

CSV occupies 12,246,074 bytes and Parquet 1,778,594 bytes for the equivalent benchmark projection. Warm-cache times exclude the separately reported cache-build cost. These are local, warm-OS-cache observations under possible concurrent verification load, not production benchmarks or evidence of cluster speedup. Inspect per-configuration measurements before generalizing.

Methods and limitations are detailed in [the advanced guide](ADVANCED_GUIDE.md). Raw metrics are in `outputs/uci/advanced/advanced_summary.json` and the exported CSV reports.
