"""Time-separated inactivity prediction. A label is observed non-purchase, not proven churn."""
from datetime import timedelta

from pyspark.ml import Pipeline
from pyspark.ml.classification import (
    GBTClassifier,
    LogisticRegression,
    RandomForestClassifier,
)
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

FEATURES = ["logRecency", "logFrequency", "logMonetary", "logTenure", "logProducts", "logAvgOrderValue"]


def inactivity_models():
    """Build comparable linear and tree classifiers for the same future outcome."""
    assembler = VectorAssembler(inputCols=FEATURES, outputCol="rawFeatures")
    scaler = StandardScaler(
        inputCol="rawFeatures", outputCol="features", withMean=True, withStd=True
    )
    return [
        ("Logistic Regression", Pipeline(stages=[
            assembler, scaler,
            LogisticRegression(
                featuresCol="features", labelCol="label", maxIter=100,
                regParam=0.05, elasticNetParam=0.0, threshold=0.5,
            ),
        ])),
        ("Random Forest", Pipeline(stages=[
            assembler, scaler,
            RandomForestClassifier(
                featuresCol="features", labelCol="label", numTrees=60,
                maxDepth=5, minInstancesPerNode=5, seed=42,
                featureSubsetStrategy="sqrt",
            ),
        ])),
        ("Gradient-Boosted Trees", Pipeline(stages=[
            assembler, scaler,
            GBTClassifier(
                featuresCol="features", labelCol="label", maxIter=30,
                maxDepth=4, stepSize=0.1, seed=42,
            ),
        ])),
    ]


def _evaluate_classifier(model, holdout):
    """Return holdout scores and standard classification metrics."""
    prediction = model.transform(holdout).withColumn(
        "RiskScore", vector_to_array("probability")[1]
    ).cache()
    groups = {
        (int(row.label), int(row.prediction)): row["count"]
        for row in prediction.groupBy("label", "prediction").count().collect()
    }
    tn, fp, fn, tp = [groups.get(key, 0) for key in [(0, 0), (0, 1), (1, 0), (1, 1)]]
    count = tn + fp + fn + tp
    precision = tp / (tp + fp) if tp + fp else 0
    recall = tp / (tp + fn) if tp + fn else 0
    two_classes = holdout.select("label").distinct().count() == 2
    metrics = {
        "roc_auc": BinaryClassificationEvaluator(metricName="areaUnderROC").evaluate(prediction) if two_classes else None,
        "pr_auc": BinaryClassificationEvaluator(metricName="areaUnderPR").evaluate(prediction) if two_classes else None,
        "precision": precision,
        "recall": recall,
        "f1": 2 * precision * recall / (precision + recall) if precision + recall else 0,
        "accuracy": (tn + tp) / count if count else 0,
        "brier_score": prediction.agg(
            F.avg((F.col("RiskScore") - F.col("label")) ** 2)
        ).first()[0] if count else None,
        "confusion_matrix": {"tn": tn, "fp": fp, "fn": fn, "tp": tp},
        "holdout_customers": count,
        "nonpurchase_prevalence": (tp + fn) / count if count else 0,
        "majority_class_accuracy": max(tn + fp, tp + fn) / count if count else 0,
    }
    return prediction, metrics


def _feature_importance(model):
    """Explain features with tree importance or normalized absolute LR coefficients."""
    estimator = model.stages[-1]
    if hasattr(estimator, "featureImportances"):
        values = estimator.featureImportances.toArray()
        method = "Tree feature importance"
    else:
        values = estimator.coefficients.toArray()
        values = abs(values)
        total = float(values.sum())
        if total:
            values = values / total
            method = "Normalized absolute standardized Logistic Regression coefficient"
        else:
            values = [1.0 / len(FEATURES)] * len(FEATURES)
            method = "Uniform fallback because all Logistic Regression coefficients are zero"
    return [
        {"Feature": name, "Importance": float(value), "Method": method}
        for name, value in zip(FEATURES, values)
    ]


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

    comparison = []
    evaluated_models = {}
    for name, pipeline in inactivity_models():
        print(f"Evaluating {name} on the same chronological holdout...", flush=True)
        evaluation_model = pipeline.fit(training)
        prediction, model_metrics = _evaluate_classifier(evaluation_model, holdout)
        comparison.append({"Model": name, **{
            key: model_metrics[key]
            for key in ["roc_auc", "pr_auc", "precision", "recall", "f1", "accuracy", "brier_score"]
        }})
        evaluated_models[name] = (pipeline, evaluation_model, prediction, model_metrics)
        print(f"{name}: PR-AUC={model_metrics['pr_auc']}, ROC-AUC={model_metrics['roc_auc']}", flush=True)

    # PR-AUC is primary for a potentially imbalanced no-purchase outcome. Break
    # ties with ROC-AUC and F1; if holdout classes prevent AUC calculation, use F1.
    chosen_name = max(
        comparison,
        key=lambda row: (
            row["pr_auc"] if row["pr_auc"] is not None else float("-inf"),
            row["roc_auc"] if row["roc_auc"] is not None else float("-inf"),
            row["f1"],
        ),
    )["Model"]
    pipeline, evaluation_model, prediction, selected_metrics = evaluated_models[chosen_name]
    holdout_scores = prediction.select(
        "CustomerID", "SnapshotDate", "Recency", "Frequency", "Monetary",
        "RiskScore", "label", "prediction",
    ).cache()
    holdout_scores.count()
    importance = _feature_importance(evaluation_model)
    metrics = {
        **selected_metrics,
        "model": chosen_name,
        "model_comparison": comparison,
        "selection_rule": "Highest PR-AUC on the same chronological holdout; ROC-AUC and F1 break ties. F1 is used when holdout AUC is unavailable.",
        "models_tested": [row["Model"] for row in comparison],
        "horizon_days": horizon,
        "train_snapshots": list(map(str, train_cutoffs)),
        "train_labels_end_exclusive": str(test_cutoff),
        "holdout_snapshot": str(test_cutoff),
        "holdout_end_exclusive": str(reference_date),
        "label_definition": f"No observed purchase in the following {horizon} days",
        "score_note": "Model scores are not assumed calibrated. No purchase in a fixed horizon is not proof of permanent churn.",
        "latest_snapshot": str(reference_date),
        "latest_outcomes_observed": False,
    }
    for name, (_, _, other_prediction, _) in evaluated_models.items():
        if name != chosen_name:
            other_prediction.unpersist()

    # After fair holdout comparison, refit the selected pipeline on every fully
    # labelled snapshot for the latest, still-unlabelled customer risk list.
    final_model = pipeline.fit(training.unionByName(holdout))
    latest = final_model.transform(snapshot_features(clean, reference_date))
    latest = (latest.withColumn("RiskScore", vector_to_array("probability")[1])
              .select("CustomerID", "SnapshotDate", "Recency", "Frequency", "Monetary", "RiskScore")
              .orderBy(F.desc("RiskScore"), "CustomerID").cache())
    latest.count()
    for frame in [prediction, training, holdout]:
        frame.unpersist()
    return latest, holdout_scores, importance, metrics, final_model
