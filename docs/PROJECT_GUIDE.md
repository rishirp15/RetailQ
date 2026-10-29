# Build and explain RetailIQ

The baseline described below is extended by all eight features in [the advanced guide](ADVANCED_GUIDE.md), including chronological model evaluation, real Structured Streaming checkpoint recovery and a performance study. The original K=4 baseline remains available even when automatic selection chooses another K.

## Objective

Use Apache Spark to clean retail transaction records, identify customer behavior using RFM and clustering, and turn the results into understandable business recommendations.

## Architecture

```text
UCI workbook → streaming CSV conversion → Spark cleaning → Spark SQL RFM
                                            │                   │
                                            │                   └→ log + scale → MLlib K-Means
                                            ├→ RDD country revenue              │
                                            ├→ monthly/product aggregates       │
                                            └→ product-vector cosine similarity │
                                                          ↓                     ↓
                                                    Exported CSV + summary JSON
                                                               ↓
                                                      Streamlit dashboard
```

## Learning order

1. **Understand the data.** Each row is a product in an invoice. One invoice can contain many rows. Review the eight source columns and explain why an invoice count differs from a row count.
2. **Run the synthetic demonstration.** `scripts/run.ps1 -Demo` creates a reproducible small dataset. Inspect the cleaning audit before interpreting any chart.
3. **Read `retailiq/analytics.py`.** Follow `clean_transactions`, then `compute_rfm`. Explain the SQL `GROUP BY CustomerID` and `COUNT(DISTINCT InvoiceNo)`.
4. **Inspect MLlib.** Follow `cluster_customers`; explain why log transformation reduces skew and standardization keeps monetary values from dominating distance solely through their units.
5. **Run the UCI dataset.** Use `scripts/run.ps1`. Record the actual output counts and charts. Do not substitute synthetic results in the report.
6. **Compare K values.** Run K=2 through K=6 into different output directories; record silhouette scores, segment sizes and interpretability. Justify the final choice rather than picking solely the highest score.
7. **Interpret and present.** Explain one high-value profile, one inactive profile, revenue contribution, a similarity result, and one limitation.

## Formulas with a small example

Take snapshot date 10 December. Customer A bought on 1 December and 8 December, across three product rows, spending £20, £15 and £5.

| Feature | Formula | Customer A |
| --- | --- | --- |
| Recency | Snapshot date − latest purchase date | 2 days |
| Frequency | Number of distinct invoices | 2, assuming two invoices |
| Monetary | Σ quantity × unit price | £40 |

The snapshot must be tied to the data period. Using today's date for 2011 purchases would make everyone appear inactive.

Cosine similarity for product vectors A and B:

`similarity(A,B) = sum(A_i × B_i) / (sqrt(sum(A_i²)) × sqrt(sum(B_i²)))`

For A=[2,0] and B=[4,0], the score is 1; for C=[0,2], the similarity to A is 0; for D=[2,2], it is approximately 0.707. The implementation uses all products in each norm, including products outside the target's basket.

## Segment interpretation

Clustering has no supplied correct labels. After training, the code examines each cluster's average scores:

| Condition, evaluated in order | Description | Example action |
| --- | --- | --- |
| Average R score < 2.5 | At risk / inactive | Test a win-back campaign |
| Average M and F scores ≥ 3.5 | Champions | VIP rewards and early access |
| Average F score ≥ 3 | Loyal customers | Complementary products and bundles |
| Remaining profiles | Developing customers | Relevant repeat-purchase offers |

These are transparent descriptive rules, not verified marketing truths. Individual inactivity and high-value flags are computed separately. Several clusters may have the same description; retain the cluster ID when comparing them.

## Suggested report structure

1. Title, team members, course and guide.
2. Abstract: problem, Spark methods, outputs and principal measured findings.
3. Problem statement and objectives.
4. Dataset source, citation, period, columns and raw row count.
5. Architecture and software environment.
6. Methodology: cleaning assumptions, RFM, preprocessing, K-Means, cosine.
7. Results: audit, customer count, selected K, silhouette, segment profiles, revenue contribution, similarity examples.
8. Business recommendations tied to actual results.
9. Limitations and future work.
10. Conclusion and references.

Populate results after running UCI. Never invent accuracy figures, cleaned counts, or revenue totals.

## Five-minute demo

- **0:00–0:40:** explain the business question and one-invoice/many-rows structure.
- **0:40–1:20:** show cleaning counts and describe exclusions.
- **1:20–2:10:** explain RFM formulas and log-standardized K-Means.
- **2:10–3:10:** show cluster sizes, revenue share and recommendations.
- **3:10–4:00:** filter high-value/inactive customers and explain basket similarity.
- **4:00–5:00:** show the RDD method and SQL aggregation in the source; state limitations.

While a local Spark job is running, its Spark UI is normally available at http://localhost:4040. The UI closes when the job stops. It can demonstrate jobs, stages and tasks, but local execution does not establish distributed speedup.

## Viva preparation

**Why Spark?** It provides distributed transformations, SQL and machine learning APIs in one engine. Our laptop demo uses those APIs locally; a larger deployment would distribute input and computation across machines.

**Transformation versus action?** `filter`, `select`, `groupBy` and RDD `map`/`reduceByKey` construct work. `count`, `collect`, `first`, iteration and model fitting trigger execution. Spark evaluates plans lazily.

**Why cache?** Clean transactions and customer features are reused across multiple actions. Caching avoids rebuilding expensive upstream transformations.

**Why distinct invoices for frequency?** Product rows measure line-item count, which would reward larger baskets even if customers never returned.

**Why K-Means?** It is an interpretable baseline for numeric behavior features. It requires a chosen K and is sensitive to feature scaling, outliers and cluster shape.

**Why no accuracy percentage?** Clustering is unsupervised and there are no ground-truth customer labels. Silhouette describes separation and cohesion; business usefulness requires further evaluation.

**How is similarity scalable?** We aggregate sparse product purchases and compare one customer against others with a join. We do not create every customer pair. All-pairs similarity would need a different strategy, such as approximate nearest neighbors.

**What is excluded from revenue?** Cancelled invoices, negative quantities and non-positive prices. Therefore totals describe positive purchase value, not refunds-adjusted net revenue.

**What could be improved?** Time-based validation, retention outcomes, a principled K comparison, returns-aware value, incremental ingestion, saved models and distributed Parquet storage.
