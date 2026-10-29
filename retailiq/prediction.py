"""Time-separated inactivity prediction. A label is observed non-purchase, not proven churn."""
from datetime import timedelta

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

FEATURES = ["logRecency", "logFrequency", "logMonetary", "logTenure", "logProducts", "logAvgOrderValue"]


def snapshot_features(clean, cutoff):
    history = clean.filter(F.col("InvoiceDate") < F.lit(cutoff))
    features = history.groupBy("CustomerID").agg(F.max("InvoiceDate").alias("LastPurchase"),
        F.min("InvoiceDate").alias("FirstPurchase"), F.countDistinct("InvoiceNo").alias("Frequency"),
        F.sum("Revenue").cast("double").alias("Monetary"), F.countDistinct("StockCode").alias("Products"))
    features = (features.withColumn("Recency", F.datediff(F.lit(cutoff), "LastPurchase"))
                .withColumn("Tenure", F.datediff(F.lit(cutoff), "FirstPurchase"))
                .withColumn("AvgOrderValue", F.col("Monetary") / F.col("Frequency"))
                .withColumn("SnapshotDate", F.lit(cutoff)))
    for name in ["Recency", "Frequency", "Monetary", "Tenure", "Products", "AvgOrderValue"]:
        features = features.withColumn(f"log{name}", F.log1p(F.col(name)))
    return features


def labelled_snapshot(clean, cutoff, horizon):
    future = (clean.filter((F.col("InvoiceDate") >= F.lit(cutoff)) &
                          (F.col("InvoiceDate") < F.lit(cutoff + timedelta(days=horizon))))
              .select("CustomerID").distinct().withColumn("Purchased", F.lit(1)))
    return (snapshot_features(clean, cutoff).join(future, "CustomerID", "left")
            .withColumn("label", F.when(F.col("Purchased").isNull(), 1.0).otherwise(0.0)).drop("Purchased"))


def inactivity_prediction(clean, reference_date, horizon=60):
    if horizon < 1:
        raise ValueError("Prediction horizon must be positive.")
    test_cutoff = reference_date - timedelta(days=horizon)
    train_cutoffs = [test_cutoff - timedelta(days=2 * horizon), test_cutoff - timedelta(days=horizon)]
    earliest = clean.agg(F.min(F.to_date("InvoiceDate"))).first()[0]
    if train_cutoffs[0] <= earliest:
        raise ValueError("Need more than three prediction horizons of observed history.")
    training = labelled_snapshot(clean, train_cutoffs[0], horizon).unionByName(
        labelled_snapshot(clean, train_cutoffs[1], horizon)).cache()
    holdout = labelled_snapshot(clean, test_cutoff, horizon).cache()
    if training.select("label").distinct().count() < 2:
        raise ValueError("Inactivity training needs both repeat-purchasing and non-purchasing customers.")
    pipeline = Pipeline(stages=[VectorAssembler(inputCols=FEATURES, outputCol="features"),
        RandomForestClassifier(numTrees=60, maxDepth=5, minInstancesPerNode=5,
                               seed=42, featureSubsetStrategy="sqrt")])
    evaluation_model = pipeline.fit(training)
    prediction = evaluation_model.transform(holdout).withColumn("RiskScore", vector_to_array("probability")[1]).cache()
    groups = {(int(r.label), int(r.prediction)): r["count"] for r in prediction.groupBy("label", "prediction").count().collect()}
    tn, fp, fn, tp = [groups.get(key, 0) for key in [(0, 0), (0, 1), (1, 0), (1, 1)]]
    count = tn + fp + fn + tp
    two_classes = holdout.select("label").distinct().count() == 2
    precision = tp / (tp + fp) if tp + fp else 0
    recall = tp / (tp + fn) if tp + fn else 0
    metrics = {"horizon_days": horizon, "train_snapshots": list(map(str, train_cutoffs)),
        "train_labels_end_exclusive": str(test_cutoff), "holdout_snapshot": str(test_cutoff),
        "holdout_end_exclusive": str(reference_date), "holdout_customers": count,
        "label_definition": f"No observed purchase in the following {horizon} days",
        "roc_auc": BinaryClassificationEvaluator(metricName="areaUnderROC").evaluate(prediction) if two_classes else None,
        "pr_auc": BinaryClassificationEvaluator(metricName="areaUnderPR").evaluate(prediction) if two_classes else None,
        "precision": precision, "recall": recall, "f1": 2 * precision * recall / (precision + recall) if precision + recall else 0,
        "accuracy": (tn + tp) / count, "majority_class_accuracy": max(tn + fp, tp + fn) / count,
        "nonpurchase_prevalence": (tp + fn) / count, "confusion_matrix": {"tn": tn, "fp": fp, "fn": fn, "tp": tp},
        "brier_score": prediction.agg(F.avg((F.col("RiskScore") - F.col("label")) ** 2)).first()[0],
        "score_note": "Random forest scores are uncalibrated. Non-purchase over a fixed horizon is not permanent churn.",
        "latest_snapshot": str(reference_date), "latest_outcomes_observed": False}
    holdout_scores = prediction.select("CustomerID", "SnapshotDate", "Recency", "Frequency", "Monetary", "RiskScore", "label", "prediction").cache()
    holdout_scores.count()
    importance = [{"Feature": name, "Importance": float(value)} for name, value in zip(FEATURES, evaluation_model.stages[-1].featureImportances)]
    # After evaluation, fit all fully labelled snapshots for future-facing scores.
    final_model = pipeline.fit(training.unionByName(holdout))
    latest = final_model.transform(snapshot_features(clean, reference_date))
    latest = (latest.withColumn("RiskScore", vector_to_array("probability")[1])
              .select("CustomerID", "SnapshotDate", "Recency", "Frequency", "Monetary", "RiskScore")
              .orderBy(F.desc("RiskScore"), "CustomerID").cache())
    latest.count()
    for frame in [prediction, training, holdout]:
        frame.unpersist()
    return latest, holdout_scores, importance, metrics, final_model
