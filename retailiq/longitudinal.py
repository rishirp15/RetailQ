"""Observed retention cohorts and fixed-model retrospective segment migration."""
from datetime import date

from pyspark.sql import Window, functions as F
from retailiq.experiments import log_features


def retention_cohorts(clean):
    first = clean.groupBy("CustomerID").agg(F.trunc(F.min("InvoiceDate"), "month").alias("Cohort"))
    activity = clean.select("CustomerID", F.trunc("InvoiceDate", "month").alias("PurchaseMonth")).distinct()
    maximum = activity.agg(F.max("PurchaseMonth")).first()[0]
    sizes = first.groupBy("Cohort").agg(F.count("*").alias("CohortSize"))
    counts = activity.join(first, "CustomerID").groupBy("Cohort", "PurchaseMonth").agg(
        F.countDistinct("CustomerID").alias("ActiveCustomers"))
    grid = sizes.withColumn("PurchaseMonth", F.explode(F.sequence("Cohort", F.lit(maximum), F.expr("interval 1 month"))))
    return (grid.join(counts, ["Cohort", "PurchaseMonth"], "left").fillna({"ActiveCustomers": 0})
            .withColumn("MonthIndex", F.months_between("PurchaseMonth", "Cohort").cast("int"))
            .withColumn("Retention", F.col("ActiveCustomers") / F.col("CohortSize"))
            .withColumn("PartialMonth", F.col("PurchaseMonth") == F.lit(maximum))
            .orderBy("Cohort", "MonthIndex"))


def monthly_migration(clean, model, profiles, reference_date):
    earliest = clean.agg(F.min(F.to_date("InvoiceDate"))).first()[0]
    cutoffs = []
    current = date(earliest.year + (earliest.month == 12), earliest.month % 12 + 1, 1)
    while current < reference_date:
        cutoffs.append(current)
        current = date(current.year + (current.month == 12), current.month % 12 + 1, 1)
    cutoffs.append(reference_date)
    snapshots = clean.sparkSession.createDataFrame([(day,) for day in cutoffs], "SnapshotDate date")
    history = clean.crossJoin(F.broadcast(snapshots)).filter(F.col("InvoiceDate") < F.col("SnapshotDate"))
    rfm = history.groupBy("SnapshotDate", "CustomerID").agg(F.max("InvoiceDate").alias("LastPurchase"),
        F.countDistinct("InvoiceNo").alias("Frequency"), F.sum("Revenue").alias("Monetary"))
    rfm = rfm.withColumn("Recency", F.datediff("SnapshotDate", "LastPurchase"))
    assignments = model.transform(log_features(rfm)).join(profiles.select("Cluster", "Segment"), "Cluster").select(
        "SnapshotDate", "CustomerID", "Cluster", "Segment", "Recency", "Frequency", "Monetary").cache()
    history_order = Window.partitionBy("CustomerID").orderBy("SnapshotDate")
    previous = (assignments.withColumn("FromSnapshot", F.lag("SnapshotDate").over(history_order))
                .withColumn("FromCluster", F.lag("Cluster").over(history_order))
                .withColumn("FromSegment", F.lag("Segment").over(history_order)))
    transitions = (previous.filter(F.col("FromSnapshot").isNotNull())
        .groupBy("FromSnapshot", F.col("SnapshotDate").alias("ToSnapshot"), "FromCluster",
                 F.col("Cluster").alias("ToCluster"), "FromSegment", F.col("Segment").alias("ToSegment"))
        .agg(F.count("*").alias("Customers")).orderBy("ToSnapshot", "FromCluster", "ToCluster"))
    counts = assignments.groupBy("SnapshotDate", "Cluster", "Segment").agg(F.count("*").alias("Customers"))
    return assignments, transitions, counts.orderBy("SnapshotDate", "Cluster")
