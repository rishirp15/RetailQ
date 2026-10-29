"""Reconcile advanced UCI exports, novel recommendations, migration and replay event times."""
import csv
import json
import math
from collections import defaultdict
from decimal import Decimal
from pathlib import Path

root = Path(__file__).resolve().parents[1]
base = root / "outputs/uci"
directory = base / "advanced"
baseline = json.loads((base / "summary.json").read_text())
state = json.loads((directory / "advanced_summary.json").read_text())
assert state["complete"] and all(stage["status"] == "completed" for stage in state["features"].values())


def rows(name):
    with (directory / f"{name}.csv").open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


customers, segments = rows("selected_customers"), rows("selected_segments")
assert len(customers) == baseline["customers"] == len({r["CustomerID"] for r in customers})
assert sum(int(r["Customers"]) for r in segments) == len(customers)
assert sum(Decimal(r["Monetary"]) for r in customers) == Decimal(str(baseline["revenue"]))
assert sum(Decimal(r["Revenue"]) for r in segments) == Decimal(str(baseline["revenue"]))
runs = rows("k_runs")
assert len(runs) == 21 and len({(r["K"], r["Seed"]) for r in runs}) == 21
assert all(-1 <= float(r["Silhouette"]) <= 1 for r in runs)

baskets = rows("basket_rules")
assert all(0 < float(r["Support"]) <= 1 and 0 <= float(r["Confidence"]) <= 1 and float(r["Lift"]) > 1 for r in baskets)

seen = set()
with Path(state["input"]).open(newline="", encoding="utf-8") as handle:
    for row in csv.DictReader(handle):
        customer = row["CustomerID"].removesuffix(".0").strip()
        if customer and not row["InvoiceNo"].upper().startswith("C") and float(row["Quantity"] or 0) > 0 and float(row["UnitPrice"] or 0) > 0:
            seen.add((customer, row["StockCode"].strip()))
recommendations = rows("recommendations")
assert len(recommendations) == len(customers) * 5
assert not any((r["CustomerID"], r["StockCode"]) in seen for r in recommendations)
assert len({(r["CustomerID"], r["StockCode"]) for r in recommendations}) == len(recommendations)

cohorts = rows("retention_cohorts")
assert all(0 <= float(r["Retention"]) <= 1 for r in cohorts)
assert all(float(r["Retention"]) == 1 for r in cohorts if int(r["MonthIndex"]) == 0)
assert sum(int(r["CohortSize"]) for r in cohorts if int(r["MonthIndex"]) == 0) == len(customers)

membership = defaultdict(int)
for row in rows("monthly_segments"):
    membership[row["SnapshotDate"]] += int(row["Customers"])
transitions = defaultdict(int)
for row in rows("segment_migration"):
    transitions[(row["FromSnapshot"], row["ToSnapshot"])] += int(row["Customers"])
assert all(count == membership[start] for (start, _), count in transitions.items())
latest = {r["CustomerID"]: r["Cluster"] for r in rows("monthly_customers") if r["SnapshotDate"] == state["reference_date"]}
assert latest == {r["CustomerID"]: r["Cluster"] for r in customers}

risks = rows("inactivity_risk")
assert len(risks) == len(customers) and all(0 <= float(r["RiskScore"]) <= 1 for r in risks)
prediction = state["features"]["prediction"]["metrics"]
assert sum(prediction["confusion_matrix"].values()) == prediction["holdout_customers"]
assert prediction["train_labels_end_exclusive"] <= prediction["holdout_snapshot"]
assert math.isclose(sum(float(r["Importance"]) for r in rows("feature_importance")), 1, abs_tol=1e-9)

stream = json.loads((directory / "streaming/summary.json").read_text())
assert stream["checkpoint_recovery_verified"] and stream["reconciled"]
expected = defaultdict(lambda: [0, Decimal("0")])
with (directory / "stream_events.csv").open(newline="", encoding="utf-8") as handle:
    reader = csv.DictReader(handle)
    for _ in range(stream["expected_rows"]):
        event = next(reader)
        value = expected[(event["InvoiceDate"][:10], event["Country"])]
        value[0] += 1
        value[1] += Decimal(event["Revenue"])
with (directory / "streaming/stream_totals.csv").open(newline="", encoding="utf-8") as handle:
    actual = {(r["WindowStart"][:10], r["Country"]): [int(r["PurchaseRows"]), Decimal(r["Revenue"])] for r in csv.DictReader(handle)}
assert actual == dict(expected), "Streaming event days/countries/revenue must match the source without timezone drift."

performance = rows("performance")
assert len(performance) == 24
assert all(int(r["Rows"]) == state["audit"]["cleaned_rows"] and r["Reconciled"] == "True" for r in performance)
print("PASS: all 8 feature exports reconcile; recommendations are novel; cohort/migration populations agree; stream event times and Decimal totals match.")
