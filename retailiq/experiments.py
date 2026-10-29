"""K selection, stability and label-invariant adjusted Rand agreement."""
from collections import defaultdict
from math import comb, ceil
from time import perf_counter

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import functions as F


def adjusted_rand(contingency):
    """Adjusted Rand from (labelA, labelB, count); only K² counts reach the driver."""
    rows, cols = defaultdict(int), defaultdict(int)
    total = joint = 0
    for left, right, count in contingency:
        rows[left] += count
        cols[right] += count
        total += count
        joint += comb(count, 2)
    pairs = comb(total, 2) if total >= 2 else 0
    if not pairs:
        return 1.0
    left_pairs = sum(comb(n, 2) for n in rows.values())
    right_pairs = sum(comb(n, 2) for n in cols.values())
    expected = left_pairs * right_pairs / pairs
    upper = (left_pairs + right_pairs) / 2
    return (joint - expected) / (upper - expected) if upper != expected else 1.0


def log_features(rfm):
    for name in ["Recency", "Frequency", "Monetary"]:
        rfm = rfm.withColumn(f"log{name}", F.log1p(F.col(name).cast("double")))
    return rfm


def compare_k(rfm, ks=range(2, 9), seeds=(42, 7, 21)):
    if not seeds or len(set(seeds)) != len(seeds):
        raise ValueError("Provide distinct, nonempty K-Means seeds.")
    distinct = rfm.select("Recency", "Frequency", "Monetary").distinct().count()
    ks = sorted(set(ks))
    if not ks or min(ks) < 2 or max(ks) >= distinct:
        raise ValueError(f"K values must be between 2 and {distinct - 1}.")
    base = log_features(rfm)
    prep = Pipeline(stages=[VectorAssembler(inputCols=["logRecency", "logFrequency", "logMonetary"], outputCol="rawFeatures"),
        StandardScaler(inputCol="rawFeatures", outputCol="features", withMean=True, withStd=True)]).fit(base)
    features = prep.transform(base).cache()
    count = features.count()
    evaluator = ClusteringEvaluator(predictionCol="Cluster")
    runs, summaries = [], []
    best = None
    for k in ks:
        reference = None
        candidates, stability = [], []
        for seed in seeds:
            start = perf_counter()
            model = KMeans(k=k, seed=seed, maxIter=50, predictionCol="Cluster").fit(features)
            prediction = model.transform(features).cache()
            sizes = [r["count"] for r in prediction.groupBy("Cluster").count().collect()]
            silhouette = evaluator.evaluate(prediction) if len(sizes) > 1 else None
            agreement = None
            if reference is None:
                reference = prediction.select("CustomerID", F.col("Cluster").alias("Reference")).cache()
                reference.count()
            else:
                cells = prediction.join(reference, "CustomerID").groupBy("Cluster", "Reference").count().collect()
                agreement = adjusted_rand([(r.Cluster, r.Reference, r["count"]) for r in cells])
                stability.append(agreement)
            record = {"K": k, "Seed": seed, "Silhouette": silhouette, "MinClusterSize": min(sizes),
                      "MaxClusterSize": max(sizes), "PopulatedClusters": len(sizes),
                      "TrainingCost": model.summary.trainingCost, "StabilityARI": agreement,
                      "Seconds": perf_counter() - start}
            runs.append(record)
            candidates.append((record, model))
            prediction.unpersist()
            print(f"K={k}, seed={seed}: silhouette={silhouette}, ARI={agreement}", flush=True)
        reference.unpersist()
        scores = [r[0]["Silhouette"] for r in candidates if r[0]["Silhouette"] is not None]
        eligible = all(r[0]["PopulatedClusters"] == k and r[0]["MinClusterSize"] >= max(2, ceil(count * .01)) for r in candidates)
        summary = {"K": k, "MeanSilhouette": sum(scores) / len(scores) if scores else -1.0,
                   "MeanStabilityARI": sum(stability) / len(stability) if stability else None,
                   "SmallestCluster": min(r[0]["MinClusterSize"] for r in candidates), "Eligible": eligible}
        summaries.append(summary)
        strongest = max(candidates, key=lambda r: r[0]["Silhouette"] if r[0]["Silhouette"] is not None else -1)
        rank = (eligible, summary["MeanSilhouette"], -k)
        if best is None or rank > best[0]:
            best = (rank, summary, strongest)
    features.unpersist()
    _, selected, (run, model) = best
    selected = {**selected, "Seed": run["Seed"], "Silhouette": run["Silhouette"],
                "SelectionRule": "Highest mean silhouette among K values with all clusters >=1% of customers (minimum 2); fallback to highest mean if none eligible."}
    return runs, summaries, selected, PipelineModel(stages=[*prep.stages, model])


def describe_selected(rfm, model):
    prediction = model.transform(log_features(rfm))
    profiles = prediction.groupBy("Cluster").agg(F.count("*").alias("Customers"),
        F.avg("Recency").alias("AvgRecency"), F.avg("Frequency").alias("AvgFrequency"),
        F.avg("Monetary").alias("AvgMonetary"), F.sum("Monetary").alias("Revenue"),
        F.avg("RScore").alias("AvgRScore"), F.avg("FScore").alias("AvgFScore"), F.avg("MScore").alias("AvgMScore"))
    profiles = profiles.withColumn("Segment", F.when(F.col("AvgRScore") < 2.5, "At risk / inactive")
        .when((F.col("AvgMScore") >= 3.5) & (F.col("AvgFScore") >= 3.5), "Champions")
        .when(F.col("AvgFScore") >= 3, "Loyal customers").otherwise("Developing customers"))
    profiles = profiles.withColumn("Recommendation", F.when(F.col("Segment") == "Champions", "Test VIP retention rewards.")
        .when(F.col("Segment") == "At risk / inactive", "Test a targeted win-back offer.")
        .when(F.col("Segment") == "Loyal customers", "Recommend complementary products.")
        .otherwise("Encourage a relevant repeat purchase."))
    total = float(rfm.agg(F.sum("Monetary")).first()[0])
    profiles = profiles.withColumn("RevenueShare", F.col("Revenue").cast("double") / total)
    customers = prediction.join(profiles.select("Cluster", "Segment"), "Cluster").select(
        "CustomerID", "Recency", "Frequency", "Monetary", "LastPurchase", "RScore", "FScore", "MScore", "RFMScore", "HighValue", "Inactive", "Cluster", "Segment")
    return customers, profiles
