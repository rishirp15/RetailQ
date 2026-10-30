"""Run or resume all advanced RetailIQ analyses with python -m retailiq.advanced."""
import argparse
import hashlib
import json
import traceback
from pathlib import Path
from time import perf_counter

from pyspark.ml import PipelineModel
from pyspark.sql import functions as F
from retailiq.analytics import clean_transactions, compute_rfm
from retailiq.benchmark import performance_study
from retailiq.commerce import market_baskets, train_recommendations
from retailiq.experiments import compare_k, describe_selected
from retailiq.longitudinal import retention_cohorts, monthly_migration
from retailiq.pipeline import export_csv
from retailiq.prediction import inactivity_prediction
from retailiq.runtime import create_spark, read_transactions
from retailiq.storage import file_hash, write_json, write_records
from retailiq.streaming import replay_stream

FEATURES = [
    "clustering",
    "baskets",
    "recommendations",
    "cohorts",
    "migration",
    "prediction",
    "streaming",
    "performance",
]


def main():
    """Run selected advanced stages and persist status for safe resumption."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=Path("data/raw/online_retail.csv"))
    parser.add_argument("--output", type=Path, default=Path("outputs/uci/advanced"))
    parser.add_argument("--features", nargs="+", choices=FEATURES, default=FEATURES)
    parser.add_argument("--k-min", type=int, default=2)
    parser.add_argument("--k-max", type=int, default=8)
    parser.add_argument("--seeds", nargs="+", type=int, default=[42, 7, 21])
    parser.add_argument("--min-support", type=float, default=.02)
    parser.add_argument("--min-confidence", type=float, default=.25)
    parser.add_argument("--horizon", type=int, default=60)
    parser.add_argument("--benchmark-repetitions", type=int, default=2)
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--synthetic", action="store_true")
    parser.add_argument("--rerun", action="store_true", help="Recompute requested stages even if already complete")
    args = parser.parse_args()
    if not args.input.is_file():
        parser.error("Input CSV does not exist. Run python -m retailiq.data first.")
    if args.k_min < 2 or args.k_max < args.k_min or args.horizon < 1 or args.benchmark_repetitions < 1:
        parser.error("Invalid K range, horizon, or benchmark repetition count.")
    config = {"k_min": args.k_min, "k_max": args.k_max, "seeds": args.seeds,
              "min_support": args.min_support, "min_confidence": args.min_confidence,
              "horizon": args.horizon, "benchmark_repetitions": args.benchmark_repetitions,
              "master": args.master, "synthetic": args.synthetic}
    signature = hashlib.sha256((file_hash(args.input) + json.dumps(config, sort_keys=True)).encode()).hexdigest()
    args.output.mkdir(parents=True, exist_ok=True)
    manifest = args.output / "advanced_summary.json"
    state = json.loads(manifest.read_text()) if manifest.exists() else {"features": {}}
    if state.get("signature", signature) != signature:
        parser.error("Input or settings changed. Choose a different --output folder to keep results consistent.")
    state.update({"signature": signature, "input_sha256": file_hash(args.input), "config": config,
                  "input": str(args.input.resolve()), "synthetic": args.synthetic, "schema_version": 1})
    spark = create_spark("RetailIQ-advanced", args.master)
    failures = []
    try:
        clean, audit = clean_transactions(read_transactions(spark, args.input))
        rfm, reference = compute_rfm(clean)
        rfm = rfm.cache()
        state.update({"audit": audit, "reference_date": str(reference), "customers": rfm.count(),
                      "spark_version": spark.version, "master": spark.sparkContext.master})
        export_csv(clean.groupBy("StockCode").agg(F.min("Description").alias("Description")), args.output / "product_catalog.csv")
        write_json(manifest, state)
        selected_model = None
        profiles = None
        for feature in FEATURES:
            if feature not in args.features:
                continue
            prior_stage = state["features"].get(feature, {})
            has_current_prediction_comparison = bool(
                prior_stage.get("metrics", {}).get("model_comparison")
            )
            if (prior_stage.get("status") == "completed" and not args.rerun
                    and (feature != "prediction" or has_current_prediction_comparison)):
                print(f"Reusing completed stage: {feature}", flush=True)
                continue
            start = perf_counter()
            state["complete"] = False
            state["features"][feature] = {"status": "running"}
            write_json(manifest, state)
            print(f"Starting {feature}...", flush=True)
            try:
                if feature == "clustering":
                    runs, summaries, metrics, selected_model = compare_k(rfm, range(args.k_min, args.k_max + 1), args.seeds)
                    customers, profiles = describe_selected(rfm, selected_model)
                    write_records(args.output / "k_runs.csv", runs)
                    write_records(args.output / "k_comparison.csv", summaries)
                    export_csv(customers.orderBy(F.desc("Monetary")), args.output / "selected_customers.csv")
                    export_csv(profiles.orderBy("Cluster"), args.output / "selected_segments.csv")
                    selected_model.write().overwrite().save(str((args.output / "models/kmeans").resolve()))
                elif feature == "baskets":
                    rules, items, metrics = market_baskets(clean, args.min_support, args.min_confidence)
                    export_csv(rules, args.output / "basket_rules.csv")
                    export_csv(items, args.output / "frequent_itemsets.csv")
                    rules.unpersist()
                    items.unpersist()
                elif feature == "recommendations":
                    recommendations, metrics, model = train_recommendations(clean, reference, args.horizon)
                    export_csv(recommendations, args.output / "recommendations.csv")
                    model.write().overwrite().save(str((args.output / "models/als").resolve()))
                    recommendations.unpersist()
                elif feature == "cohorts":
                    cohorts = retention_cohorts(clean)
                    export_csv(cohorts, args.output / "retention_cohorts.csv")
                    metrics = {"cohorts": cohorts.select("Cohort").distinct().count(),
                               "scope": "First observed purchase month; last observed month is partial. Future cells are omitted, observed empty cells are zero."}
                elif feature == "migration":
                    if selected_model is None:
                        if state["features"].get("clustering", {}).get("status") != "completed":
                            raise ValueError("Run the clustering stage before migration.")
                        selected_model = PipelineModel.load(str((args.output / "models/kmeans").resolve()))
                    if profiles is None:
                        profiles = spark.read.option("header", True).option("inferSchema", True).csv(str((args.output / "selected_segments.csv").resolve()))
                    assignments, transitions, counts = monthly_migration(clean, selected_model, profiles, reference)
                    export_csv(assignments.orderBy("SnapshotDate", "CustomerID"), args.output / "monthly_customers.csv")
                    export_csv(transitions, args.output / "segment_migration.csv")
                    export_csv(counts, args.output / "monthly_segments.csv")
                    metrics = {"snapshots": counts.select("SnapshotDate").distinct().count(),
                               "scope": "Retrospective descriptive migration using one frozen full-period model and scaler at all snapshots. Not a historical prediction. New entrants are excluded from transitions; cumulative histories keep existing customers."}
                    assignments.unpersist()
                elif feature == "prediction":
                    latest, holdout, importance, metrics, model = inactivity_prediction(clean, reference, args.horizon)
                    export_csv(latest, args.output / "inactivity_risk.csv")
                    export_csv(holdout, args.output / "inactivity_holdout.csv")
                    write_records(args.output / "feature_importance.csv", importance)
                    write_records(args.output / "model_comparison.csv", metrics["model_comparison"])
                    model.write().overwrite().save(str((args.output / "models/inactivity").resolve()))
                    latest.unpersist()
                    holdout.unpersist()
                elif feature == "streaming":
                    events = clean.select(F.sha2(F.concat_ws("|", "CustomerID", "InvoiceNo", "StockCode", "InvoiceDate", "Quantity", "UnitPrice"), 256).alias("EventID"),
                                          "InvoiceDate", "Country", "Revenue").orderBy("InvoiceDate", "EventID")
                    export_csv(events, args.output / "stream_events.csv")
                    metrics = replay_stream(spark, args.output / "stream_events.csv", args.output / "streaming")
                elif feature == "performance":
                    measurements, metrics = performance_study(clean, args.output / "benchmark_data", args.benchmark_repetitions)
                    write_records(args.output / "performance.csv", measurements)
                state["features"][feature] = {"status": "completed", "seconds": perf_counter() - start, "metrics": metrics}
                print(f"Completed {feature} in {perf_counter() - start:.1f}s", flush=True)
            except Exception as error:
                failures.append(feature)
                state["features"][feature] = {"status": "failed", "error": str(error), "seconds": perf_counter() - start}
                traceback.print_exc()
            write_json(manifest, state)
        state["complete"] = all(state["features"].get(name, {}).get("status") == "completed" for name in FEATURES)
        write_json(manifest, state)
        print(f"Advanced outputs: {args.output}; all features complete: {state['complete']}", flush=True)
    finally:
        spark.stop()
    if failures:
        raise SystemExit(f"Failed stages: {', '.join(failures)}. Rerun the same command to resume.")


if __name__ == "__main__":
    main()
