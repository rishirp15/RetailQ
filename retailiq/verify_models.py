"""Load saved UCI models and verify their persisted behavior."""
import json
import math
from pathlib import Path

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.ml.recommendation import ALSModel
from pyspark.sql import functions as F
from retailiq.experiments import log_features
from retailiq.prediction import FEATURES
from retailiq.runtime import create_spark


def main():
    directory = Path("outputs/uci/advanced").resolve()
    metadata = json.loads((directory / "advanced_summary.json").read_text())
    spark = create_spark("RetailIQ-saved-model-check")
    try:
        kmeans = PipelineModel.load(str(directory / "models/kmeans"))
        expected = spark.read.option("header", True).option("inferSchema", True).csv(str(directory / "selected_customers.csv"))
        actual = kmeans.transform(log_features(expected.select("CustomerID", "Recency", "Frequency", "Monetary")))
        differences = actual.select("CustomerID", F.col("Cluster").alias("ReloadedCluster")).join(
            expected.select("CustomerID", "Cluster"), "CustomerID").filter("ReloadedCluster != Cluster").count()
        assert differences == 0, "Reloaded clustering assignments changed."
        als = ALSModel.load(str(directory / "models/als"))
        assert als.userFactors.count() == metadata["customers"] and als.rank == 12
        pair = spark.createDataFrame([(als.userFactors.first().id, als.itemFactors.first().id)], "user int, item int")
        assert math.isfinite(als.transform(pair).first().prediction)
        classifier = PipelineModel.load(str(directory / "models/inactivity"))
        example = spark.range(1).select(*[F.lit(1.0).alias(feature) for feature in FEATURES])
        probability = classifier.transform(example).select(vector_to_array("probability").alias("p")).first().p
        assert len(probability) == 2 and math.isclose(sum(probability), 1)
        print("PASS: saved K-Means reproduces every customer assignment; saved ALS and inactivity models load and predict.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
