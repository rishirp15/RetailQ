"""Spark transformations, SQL aggregation, MLlib clustering and cosine similarity."""
from datetime import timedelta

from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import DataFrame, functions as F

REQUIRED = ["InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate",
            "UnitPrice", "CustomerID", "Country"]


def clean_transactions(raw: DataFrame):
    """Validate transaction columns, audit rejected rows, and calculate revenue."""
    missing = set(REQUIRED) - set(raw.columns)
    if missing:
        raise ValueError(f"Missing columns: {sorted(missing)}")
    typed = raw.select(
        F.trim("InvoiceNo").alias("InvoiceNo"),
        F.trim("StockCode").alias("StockCode"), "Description",
        F.col("Quantity").cast("double").alias("Quantity"),
        F.coalesce(F.to_timestamp("InvoiceDate", "yyyy-MM-dd HH:mm:ss"),
                   F.to_timestamp("InvoiceDate", "M/d/yyyy H:mm"),
                   F.to_timestamp("InvoiceDate", "M/d/yyyy H:mm:ss")).alias("InvoiceDate"),
        F.col("UnitPrice").cast("double").alias("UnitPrice"),
        F.regexp_replace(F.trim("CustomerID"), r"\.0$", "").alias("CustomerID"),
        F.coalesce(F.col("Country"), F.lit("Unknown")).alias("Country"),
    )
    # Mutually exclusive reasons make the audit reconcile to the input count.
    reason = (F.when(F.col("CustomerID").isNull() | (F.col("CustomerID") == ""), "missing_customer")
              .when(F.col("InvoiceNo").isNull() | (F.col("InvoiceNo") == "") |
                    F.col("StockCode").isNull() | (F.col("StockCode") == ""), "missing_identifier")
              .when(F.upper("InvoiceNo").startswith("C"), "cancelled")
              .when(F.col("InvoiceDate").isNull(), "invalid_date")
              .when(F.col("Quantity").isNull() | F.isnan("Quantity") |
                    (F.col("Quantity") <= 0) | (F.abs(F.col("Quantity")) == float("inf")), "invalid_quantity")
              .when(F.col("UnitPrice").isNull() | F.isnan("UnitPrice") |
                    (F.col("UnitPrice") <= 0) | (F.abs(F.col("UnitPrice")) == float("inf")), "invalid_price")
              .otherwise("valid"))
    tagged = typed.withColumn("reason", reason).cache()
    counts = {row.reason: row["count"] for row in tagged.groupBy("reason").count().collect()}
    raw_count = sum(counts.values())
    valid = tagged.filter("reason = 'valid'").drop("reason")
    # Exact duplicate rows are assumed accidental; document this assumption.
    clean = valid.dropDuplicates().withColumn(
        "Revenue", (F.col("Quantity") * F.col("UnitPrice")).cast("decimal(20,2)")
    ).cache()
    cleaned_count = clean.count()
    counts["duplicates"] = counts.pop("valid", 0) - cleaned_count
    counts["cleaned_rows"] = cleaned_count
    counts["raw_rows"] = raw_count
    tagged.unpersist()
    return clean, counts


def compute_rfm(clean: DataFrame, reference_date=None):
    """Aggregate customer recency, frequency, monetary value, and quantile scores."""
    latest = clean.agg(F.max(F.to_date("InvoiceDate"))).first()[0]
    if latest is None:
        raise ValueError("No valid purchase rows remain after cleaning.")
    reference_date = reference_date or (latest + timedelta(days=1))
    if reference_date <= latest:
        raise ValueError("Reference date must be after the latest purchase date.")
    clean.createOrReplaceTempView("purchases")
    rfm = clean.sparkSession.sql("""
        SELECT CustomerID, MAX(InvoiceDate) AS LastPurchase,
               COUNT(DISTINCT InvoiceNo) AS Frequency,
               SUM(Revenue) AS Monetary
        FROM purchases GROUP BY CustomerID
    """).withColumn("Recency", F.datediff(F.lit(reference_date), "LastPurchase"))
    # Quantile thresholds preserve equal values in the same score bucket.
    for column, label, reverse in [("Recency", "R", True), ("Frequency", "F", False), ("Monetary", "M", False)]:
        cuts = rfm.approxQuantile(column, [0.2, 0.4, 0.6, 0.8], 0.001)
        score = F.lit(1)
        for cut in cuts:
            score = score + F.when(F.col(column) > cut, 1).otherwise(0)
        rfm = rfm.withColumn(f"{label}Score", 6 - score if reverse else score)
    rfm = (rfm.withColumn("RFMScore", F.concat("RScore", "FScore", "MScore"))
           .withColumn("HighValue", F.col("MScore") >= 4)
           .withColumn("Inactive", F.col("Recency") >= 90))
    return rfm, reference_date


def cluster_customers(rfm: DataFrame, k: int = 4, seed: int = 42):
    """Fit a log-scaled RFM K-Means pipeline and describe its customer clusters."""
    if k < 2:
        raise ValueError("K must be at least 2.")
    distinct = rfm.select("Recency", "Frequency", "Monetary").distinct().count()
    if distinct <= k:
        raise ValueError(f"K={k} requires more than {k} distinct RFM profiles; found {distinct}.")
    transformed = rfm
    columns = ["Recency", "Frequency", "Monetary"]
    for column in columns:
        transformed = transformed.withColumn(f"log{column}", F.log1p(F.col(column).cast("double")))
    pipeline = Pipeline(stages=[
        VectorAssembler(inputCols=[f"log{c}" for c in columns], outputCol="rawFeatures"),
        StandardScaler(inputCol="rawFeatures", outputCol="features", withMean=True, withStd=True),
        KMeans(k=k, seed=seed, maxIter=50, featuresCol="features", predictionCol="Cluster"),
    ])
    model = pipeline.fit(transformed)
    predictions = model.transform(transformed).cache()
    populated = predictions.select("Cluster").distinct().count()
    silhouette = (ClusteringEvaluator(predictionCol="Cluster").evaluate(predictions)
                  if 1 < populated < predictions.count() else None)
    # Cluster IDs have no business meaning. Derive descriptions from their profiles.
    profiles = predictions.groupBy("Cluster").agg(
        F.count("*").alias("Customers"), F.avg("Recency").alias("AvgRecency"),
        F.avg("Frequency").alias("AvgFrequency"), F.avg("Monetary").alias("AvgMonetary"),
        F.sum("Monetary").alias("Revenue"), F.avg("RScore").alias("AvgRScore"),
        F.avg("FScore").alias("AvgFScore"), F.avg("MScore").alias("AvgMScore"),
    )
    profiles = profiles.withColumn("Segment", F.when(F.col("AvgRScore") < 2.5, "At risk / inactive")
        .when((F.col("AvgMScore") >= 3.5) & (F.col("AvgFScore") >= 3.5), "Champions")
        .when(F.col("AvgFScore") >= 3, "Loyal customers").otherwise("Developing customers"))
    profiles = profiles.withColumn("Recommendation", F.when(F.col("Segment") == "Champions", "Offer VIP rewards and early access.")
        .when(F.col("Segment") == "At risk / inactive", "Test a targeted win-back campaign.")
        .when(F.col("Segment") == "Loyal customers", "Recommend complementary products and bundles.")
        .otherwise("Encourage a second purchase with relevant offers."))
    total = float(rfm.agg(F.sum("Monetary")).first()[0])
    profiles = profiles.withColumn("RevenueShare", F.col("Revenue").cast("double") / total)
    customers = predictions.join(profiles.select("Cluster", "Segment"), "Cluster").select(
        "CustomerID", "Recency", "Frequency", "Monetary", "LastPurchase", "RScore", "FScore", "MScore",
        "RFMScore", "HighValue", "Inactive", "Cluster", "Segment")
    return customers, profiles, model, {"k": k, "populated_clusters": populated,
        "seed": seed, "silhouette_squared_euclidean": silhouette,
        "training_cost": model.stages[-1].summary.trainingCost}


def similar_customers(clean: DataFrame, customer_id: str, limit: int = 10):
    """Exact cosine of product quantity vectors against ONE customer; no all-pairs join."""
    if limit < 1:
        raise ValueError("Similarity limit must be positive.")
    quantities = clean.groupBy("CustomerID", "StockCode").agg(F.sum("Quantity").alias("q"))
    target = quantities.filter(F.col("CustomerID") == str(customer_id)).select(
        "StockCode", F.col("q").alias("target_q"))
    norm = target.agg(F.sqrt(F.sum(F.col("target_q") ** 2))).first()[0]
    if not norm:
        raise ValueError(f"Customer {customer_id} has no valid purchases.")
    norms = quantities.groupBy("CustomerID").agg(F.sqrt(F.sum(F.col("q") ** 2)).alias("norm"))
    dots = quantities.join(F.broadcast(target), "StockCode").groupBy("CustomerID").agg(
        F.sum(F.col("q") * F.col("target_q")).alias("dot"))
    return (norms.join(dots, "CustomerID", "left").fillna({"dot": 0})
            .filter(F.col("CustomerID") != str(customer_id))
            .withColumn("CosineSimilarity", F.least(F.lit(1.0), F.col("dot") / (F.col("norm") * norm)))
            .select("CustomerID", "CosineSimilarity")
            .orderBy(F.desc("CosineSimilarity"), "CustomerID").limit(limit))


def country_revenue_rdd(clean: DataFrame):
    """Explicit RDD map/reduceByKey demonstration; Decimal preserves money precision."""
    return (clean.select("Country", "Revenue").rdd.map(lambda row: (row.Country, row.Revenue))
            .reduceByKey(lambda left, right: left + right)
            .sortBy(lambda item: (-item[1], item[0])).collect())
