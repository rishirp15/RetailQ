# RetailIQ

**Customer Segmentation & Purchase Behavior Analytics Using Apache Spark**  
Big Data Analytics mini project.

RetailIQ turns invoice line items into customer RFM profiles, MLlib K-Means segments, product-basket similarity scores, and a Streamlit dashboard. All transaction cleaning and analytics use Spark; pandas is used only to display the exported results.

The advanced extension adds **all eight features**: automatic K selection/stability, FP-Growth basket rules, implicit ALS recommendations, retention cohorts, segment migration, future inactivity prediction, Structured Streaming replay/recovery, and a measured CSV/Parquet/cache/partition study. See [the advanced guide](docs/ADVANCED_GUIDE.md) for methods, evaluation and demonstrations.

See [measured advanced results](docs/ADVANCED_RESULTS.md) for the actual model evaluations and performance observations.

```powershell
.\scripts\run.ps1 -Advanced
```

Use the sidebar's **Explore** selector to open each page. The main dashboard defaults to the original K=4 baseline for more actionable customer groups. Once advanced results exist, enable **Use automatically selected clusters** to compare the silhouette-selected K; inspect the **Model selection** page to see why it was chosen.

## Run on Windows

Use **Python 3.11 + JDK 17**. The setup script uses `uv` if available, otherwise `py -3.11`. Java 17 is automatically detected under `C:\Program Files\Eclipse Adoptium`; for another installation set `$env:JAVA_HOME` to the JDK folder. Docker is an optional alternative.

Setup installs the exact dependency versions in `requirements-lock.txt`. `requirements.txt` records the intended direct dependency ranges for future updates.

From PowerShell in this folder:

```powershell
.\scripts\setup.ps1
.\scripts\run.ps1
```

The second command downloads the UCI dataset, converts the workbook to CSV, runs Spark, and starts the dashboard at http://localhost:8501. First installation downloads PySpark and may take several minutes. Stop the dashboard with Ctrl+C.

For an offline synthetic demo after installing dependencies:

```powershell
.\scripts\run.ps1 -Demo
```

To reopen existing results without rerunning Spark:

```powershell
.\scripts\run.ps1 -DashboardOnly
# For existing synthetic results:
.\scripts\run.ps1 -Demo -DashboardOnly
```

The scripts do not change your permanent environment or PowerShell policy. If PowerShell blocks scripts, use the individual Python commands below in a terminal with Java 17 configured.

## Individual commands

Use `.\.venv\Scripts\python.exe` in place of `python` if the environment is not activated.

```powershell
$env:JAVA_HOME = 'C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot' # adjust if needed
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
.\.venv\Scripts\Activate.ps1
python -m retailiq.data
python -m retailiq.pipeline --input data/raw/online_retail.csv --output outputs/uci --k 4
python -m streamlit run app.py
python -m pytest -q
```

The data importer also accepts an existing workbook:

```powershell
python -m retailiq.data --xlsx 'path/to/Online Retail.xlsx'
```

Inspect similarity for a different customer, and optionally compare other K values in separate output folders:

```powershell
python -m retailiq.pipeline --input data/raw/online_retail.csv --output outputs/k3 --k 3 --similar-to 17850
python -m streamlit run app.py -- --results outputs/k3
```

The default reference date is **one day after the dataset's latest valid purchase**, not today's date. An explicit `--reference-date YYYY-MM-DD` must be later than every included purchase. Input timestamps support ISO `yyyy-MM-dd HH:mm:ss` and UCI-style `M/d/yyyy H:mm[:ss]`.

## Docker alternative

With Docker Desktop running in Linux-container mode:

```powershell
docker build -t retailiq .
docker run --rm -v "${PWD}/data:/app/data" retailiq python -m retailiq.data
docker run --rm -v "${PWD}/data:/app/data" -v "${PWD}/outputs:/app/outputs" retailiq python -m retailiq.pipeline --input data/raw/online_retail.csv
docker run --rm -p 8501:8501 -v "${PWD}/outputs:/app/outputs" retailiq
```

## What each part demonstrates

| Subject concept | Implementation |
| --- | --- |
| Distributed data cleaning | Explicit type conversion, filters, duplicate removal, cleaning audit |
| Explicit input schema | Header-aware CSV schema: numeric purchase fields are typed at read time; identifiers stay strings |
| Spark transformations | select, withColumn, filter, groupBy, joins |
| Spark SQL | Customer-level last purchase, distinct invoices and spend |
| RDD operations | Country revenue using map, reduceByKey, sortBy |
| MLlib | VectorAssembler → StandardScaler → KMeans |
| Similarity | Product-quantity dot products divided by full basket norms |
| Business interpretation | RFM scores, high-value/inactive flags, revenue share and suggested actions |

See [the learning and presentation guide](docs/PROJECT_GUIDE.md) for formulas, execution order, a demo script and viva preparation.

To run a separate row-scaling exercise without overwriting `outputs/uci`:

```powershell
python -m retailiq.scale_benchmark --input data/raw/online_retail.csv --output outputs/scale_study --factors 1 2 4 --repetitions 2 --partitions 4 --master 'local[2]'
```

It replicates cleaned rows in Spark and checks that country counts and Decimal revenue scale exactly. This is a local workload-scaling demonstration, not a multi-node performance benchmark. Results are written to `outputs/scale_study/scale_results.csv` and `scale_summary.json`.

The first full UCI run produced 4,338 customers across 18,532 orders. See [measured baseline results](docs/RESULTS.md) for the cleaning audit and segment findings.

## Outputs

Each successful run creates `summary.json`, `customers.csv`, `segments.csv`, `monthly_revenue.csv`, `country_revenue.csv`, `top_products.csv` and `similar_customers.csv` in its output folder. A completion manifest is written last. Synthetic and UCI outputs use separate folders. Intermediate transactions stay in Spark; the small aggregated results are streamed to local CSV files for a simple dashboard. For production, replace these local exports with distributed Parquet writes and persistent model storage.

## Method and honest limitations

- Remove missing customer/invoice/product identifiers, cancellations (`InvoiceNo` starting with C), invalid dates, non-positive/non-finite quantities or prices, and exact duplicate normalized rows. Missing descriptions are allowed. The audit assigns each rejected row one primary reason.
- Frequency is distinct invoice count, **not** number of product rows. Monetary is the sum of quantity × unit price in GBP, rounded to pennies per line.
- Compute quintile-based RFM scores with approximate thresholds. Equal values stay together; scores need not produce equal-sized groups. High value means M score ≥ 4; inactive means recency ≥ 90 days.
- Cluster standardized `log1p(R, F, M)` features using seed 42. K=4 is a starting hypothesis. Compare K=2…6, silhouette and interpretability before claiming an optimal K. The silhouette metric uses squared Euclidean distance and is measured on the training data; it is not classification accuracy.
- Cluster IDs are arbitrary. Descriptions derive from cluster-average RFM scores; descriptions can repeat. An inactive customer may still have historically high spending.
- Cosine measures basket proportions, not demographics or causation. One-customer-to-all scoring avoids a quadratic all-pairs comparison.
- Return/cancellation exclusion means **purchase revenue, not net revenue**. Dropping duplicates assumes repeated identical rows are accidental. Recommendations require business testing.
- This 541,909-row dataset fits on a laptop. `local[2]` runs Spark locally with two threads; demonstrate the distributed APIs without claiming a measured cluster speedup. Excel-to-CSV conversion is a one-time streaming ingestion step outside Spark.
- CSV input uses an explicit schema assembled from the header, so `Quantity` and `UnitPrice` are parsed as doubles at read time while identifiers stay strings. Invalid numeric text becomes null and is counted by the cleaning audit. Reordered required columns are supported; header names must still match the project fields.
- Pipeline summaries record Spark version, master, shuffle partition count, and parsed input schema for reproducibility.

## Dataset and attribution

[UCI Online Retail](https://archive.ics.uci.edu/dataset/352/online+retail): 541,909 invoice line items from a UK retailer, December 2010–December 2011. Prices are in sterling. Licensed CC BY 4.0. Cite: **Chen, D. (2015). Online Retail [Dataset]. UCI Machine Learning Repository. https://doi.org/10.24432/C5BW33.** Generated sample data is synthetic and is never represented as UCI findings.

Reference: [Apache Spark 3.5.6 installation documentation](https://spark.apache.org/docs/3.5.6/api/python/getting_started/install.html).

## Troubleshooting

- **Java gateway exits:** confirm `JAVA_HOME` points to JDK 17, not its `bin` directory, and `java -version` resolves to it. Restart your terminal after permanent environment changes.
- **Python worker fails:** use the same Python 3.11 executable for the driver and workers; the pipeline sets both by default. Prefer the run script.
- **Windows native Hadoop errors:** advanced commands build the included Java NIO filesystem adapter with JDK 17; see the advanced guide. For another environment use Docker or WSL. Do not download random `winutils.exe` binaries. A warning about missing native Hadoop alone is not a failed analysis.
- **No results:** run the pipeline successfully and check the dashboard's results-folder setting. A failed run does not create a completion manifest.
- **Download blocked:** download the workbook from UCI manually and pass `--xlsx`. Python dependencies must still be installed.
