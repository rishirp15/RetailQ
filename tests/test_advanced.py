import csv
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from pyspark.sql import functions as F
from retailiq.commerce import market_baskets, ranking_metrics
from retailiq.experiments import adjusted_rand
from retailiq.longitudinal import retention_cohorts, monthly_migration
from retailiq.prediction import labelled_snapshot
from retailiq.streaming import replay_stream
from retailiq.pipeline import export_csv


def purchase(spark, rows):
    records = [(customer, invoice, product, datetime.fromisoformat(day).replace(tzinfo=timezone.utc), "UK", Decimal(value))
               for customer, invoice, product, day, value in rows]
    return spark.createDataFrame(records, "CustomerID string, InvoiceNo string, StockCode string, InvoiceDate timestamp, Country string, Revenue decimal(20,2)")


def test_adjusted_rand_ignores_numbering_and_detects_disagreement():
    assert adjusted_rand([(0, 1, 3), (1, 0, 3)]) == 1
    assert adjusted_rand([(0, 0, 1), (0, 1, 1), (1, 0, 1), (1, 1, 1)]) == pytest.approx(-.5)
    assert adjusted_rand([]) == 1


def test_timestamp_export_preserves_spark_event_time(spark, tmp_path):
    frame = spark.createDataFrame([(datetime(2021, 1, 1, 22, 15, tzinfo=timezone.utc),)], "EventTime timestamp")
    destination = tmp_path / "timestamps.csv"
    export_csv(frame, destination)
    with destination.open(newline="", encoding="utf-8") as handle:
        assert next(csv.DictReader(handle))["EventTime"] == "2021-01-01 22:15:00"


def test_ranking_penalizes_cold_start_and_deduplicates():
    values = ranking_metrics({"A": ["x", "x", "y"]}, {"A": ["x", "z"], "B": ["x"]}, 2)
    assert values["precision_at_2"] == pytest.approx(.25)
    assert values["recall_at_2"] == pytest.approx(.25)
    assert values["hit_rate_at_2"] == pytest.approx(.5)
    assert values["evaluated_customers"] == 2


def test_cohort_zeros_future_cells_and_no_label_leakage(spark):
    clean = purchase(spark, [("A", "1", "X", "2021-01-01", "1"), ("B", "2", "X", "2021-01-05", "1"),
        ("A", "3", "Y", "2021-02-01", "1"), ("C", "4", "X", "2021-02-01", "1"),
        ("A", "5", "FUTURE", "2021-03-01", "1000"), ("B", "6", "X", "2021-03-02", "1")])
    cells = {(str(r.Cohort), r.MonthIndex): r for r in retention_cohorts(clean).collect()}
    assert cells[("2021-01-01", 1)].Retention == .5
    assert cells[("2021-01-01", 2)].Retention == 1
    assert cells[("2021-02-01", 1)].Retention == 0
    assert ("2021-02-01", 2) not in cells
    labels = {r.CustomerID: r for r in labelled_snapshot(clean, date(2021, 3, 1), 30).collect()}
    assert labels["A"].Monetary == 2
    assert labels["A"].Products == 2
    assert labels["A"].label == 0
    assert labels["C"].label == 1


def test_fp_growth_rule_values(spark):
    rows = []
    for invoice, products in enumerate([["A", "B"], ["A", "B"], ["A", "B"], ["A"], ["B"], ["C"]]):
        rows.extend([("1", str(invoice), product, "2021-01-01", "1") for product in products])
    rules, items, metadata = market_baskets(purchase(spark, rows), .2, .5)
    rule = rules.filter(F.col("Antecedent") == '["A"]').first()
    assert metadata["baskets"] == 6
    assert rule.Support == pytest.approx(.5)
    assert rule.Confidence == pytest.approx(.75)
    assert rule.Lift == pytest.approx(1.125)
    rules.unpersist()
    items.unpersist()


def test_migration_preserves_existing_customer_population(spark):
    clean = purchase(spark, [("A", "1", "X", "2021-01-01", "1"), ("B", "2", "Y", "2021-02-10", "1"),
        ("B", "3", "Y", "2021-03-02", "1")])
    class FixedModel:
        def transform(self, frame):
            return frame.withColumn("Cluster", F.when(F.col("Recency") > 45, 1).otherwise(0))
    profiles = spark.createDataFrame([(0, "Active"), (1, "Inactive")], "Cluster int, Segment string")
    assignments, transitions, _ = monthly_migration(clean, FixedModel(), profiles, date(2021, 3, 10))
    changes = transitions.collect()
    assert sum(r.Customers for r in changes if str(r.ToSnapshot) == "2021-03-01") == 1
    assert sum(r.Customers for r in changes if str(r.ToSnapshot) == "2021-03-10") == 2
    assert any(r.FromCluster == 0 and r.ToCluster == 1 for r in changes)
    assignments.unpersist()


def test_structured_streaming_checkpoint_replay_and_extension(spark, tmp_path):
    source = tmp_path / "events.csv"
    with source.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["EventID", "InvoiceDate", "Country", "Revenue"])
        for index in range(6):
            writer.writerow([str(index), f"2021-01-0{index + 1} 10:00:00", "UK", "0.10"])
    destination = tmp_path / "stream"
    first = replay_stream(spark, source, destination, batches=2, batch_rows=2)
    assert first["purchase_rows"] == 4 and first["revenue"] == .4
    assert first["checkpoint_recovery_verified"]
    resumed = replay_stream(spark, source, destination, batches=2, batch_rows=2)
    assert resumed["purchase_rows"] == 4
    extended = replay_stream(spark, source, destination, batches=3, batch_rows=2)
    assert extended["purchase_rows"] == 6 and extended["revenue"] == .6
    with pytest.raises(ValueError, match="batch size changed"):
        replay_stream(spark, source, destination, batches=3, batch_rows=3)
