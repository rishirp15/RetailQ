# UCI baseline results

Measured on the downloaded UCI Online Retail data using the included pipeline, K=4 and seed 42. This is a baseline run, not a comparison establishing the best K.

| Measure | Result |
| --- | ---: |
| Original invoice line items | 541,909 |
| Retained positive purchase rows | 392,692 |
| Customers | 4,338 |
| Distinct invoices | 18,532 |
| Positive purchase revenue | £8,887,208.89 |
| Reference date | 10 December 2011 |
| Populated K-Means clusters | 4 |
| Silhouette, squared Euclidean | 0.50093 |

## Cleaning audit

| Primary exclusion reason | Rows |
| --- | ---: |
| Missing customer ID | 135,080 |
| Cancelled invoice | 8,905 |
| Non-positive price | 40 |
| Exact duplicates after normalization | 5,192 |
| Total excluded | 149,217 |

Reasons are mutually exclusive and evaluated in order: missing customer, missing invoice/product identifier, cancellation, invalid date, invalid quantity, invalid price. For example, a cancellation without a customer ID appears under missing customer, so the cancellation count is not the count of every cancelled invoice line in the source.

## Customer segments

| Cluster | Description | Customers | Average recency (days) | Average invoices | Revenue share |
| --- | --- | ---: | ---: | ---: | ---: |
| 0 | Developing customers | 862 | 19.28 | 2.13 | 5.30% |
| 1 | At risk / inactive | 1,593 | 185.08 | 1.32 | 6.19% |
| 2 | Loyal customers | 1,168 | 71.16 | 4.09 | 23.43% |
| 3 | Champions | 715 | 12.76 | 13.73 | 65.08% |

The Champions cluster contains about 16.5% of customers and contributes about 65.1% of positive purchase revenue. This suggests testing retention rewards for that group. The at-risk cluster averages 185 days since last purchase, suggesting a separate win-back experiment. These are descriptive opportunities, not measured campaign effects.

Similarity is demonstrated against customer **14646**, the highest-spending customer in this cleaned snapshot. The actual nearest baskets are in `outputs/uci/similar_customers.csv`.

## Interpretation limits

Revenue excludes returns and cancellations; it is not net revenue. Results depend on duplicate removal, source data and clustering parameters. Cluster IDs are arbitrary and can differ across runs or environments; compare profiles rather than IDs. The silhouette score is an in-sample separation measure, not classification accuracy. Compare additional K values and business usefulness before presenting a final model selection.

Source attribution: Chen, D. (2015). [Online Retail](https://archive.ics.uci.edu/dataset/352/online+retail). UCI Machine Learning Repository. DOI: 10.24432/C5BW33. CC BY 4.0.
