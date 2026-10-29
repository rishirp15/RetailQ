"""Run with python -m retailiq.pipeline --input data/raw/online_retail.csv."""
import argparse
import csv
import json
import os
import sys
from datetime import date
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from retailiq.analytics import (clean_transactions, compute_rfm, cluster_customers,
                                similar_customers, country_revenue_rdd)


def export_csv(frame, path):
    # Driver exports are appropriate for this small mini-project's aggregated outputs.
    # For large deployments, replace with distributed DataFrame.write.parquet(...).
    # Format timestamps inside Spark, before Python converts them through the host
    # timezone. This keeps replayed event times identical to source event times.
    from pyspark.sql.types import TimestampType
    frame = frame.select(*[F.date_format(F.col(field.name), "yyyy-MM-dd HH:mm:ss").alias(field.name)
                           if isinstance(field.dataType, TimestampType) else F.col(field.name)
                           for field in frame.schema.fields])
    temporary = path.with_suffix(".csv.tmp")
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(frame.columns)
        for row in frame.toLocalIterator():
            writer.writerow(list(row))
    temporary.replace(path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=Path("outputs/uci"))
    parser.add_argument("--k", type=int, default=4)
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--reference-date", type=date.fromisoformat)
    parser.add_argument("--similar-to", help="Customer ID; defaults to highest-spending customer")
    parser.add_argument("--synthetic", action="store_true", help="Label outputs as synthetic demo data")
    args = parser.parse_args()
    if not args.input.is_file():
        parser.error(f"Input file not found: {args.input}. Run python -m retailiq.data first.")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    spark = (SparkSession.builder.appName("RetailIQ").master(args.master)
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.sql.ansi.enabled", "false")
             .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
             .config("spark.driver.bindAddress", "127.0.0.1").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        raw = spark.read.option("header", True).option("multiLine", True).option("escape", '"').csv(str(args.input.resolve()))
        print("Cleaning transaction rows...")
        clean, audit = clean_transactions(raw)
        print(f"Cleaning audit: {audit}")
        rfm, reference = compute_rfm(clean, args.reference_date)
        rfm = rfm.cache()
        print("Training MLlib K-Means on standardized log-RFM features...")
        customers, profiles, _, metrics = cluster_customers(rfm, args.k)
        target = args.similar_to or rfm.orderBy(F.desc("Monetary"), "CustomerID").first().CustomerID
        similarity = similar_customers(clean, target)
        countries = country_revenue_rdd(clean)
        monthly = clean.groupBy(F.date_format("InvoiceDate", "yyyy-MM").alias("Month")).agg(
            F.sum("Revenue").alias("Revenue"), F.countDistinct("InvoiceNo").alias("Orders")).orderBy("Month")
        products = clean.groupBy("StockCode").agg(F.first("Description", ignorenulls=True).alias("Description"),
            F.sum("Revenue").alias("Revenue"), F.sum("Quantity").alias("Units")).orderBy(F.desc("Revenue")).limit(20)
        args.output.mkdir(parents=True, exist_ok=True)
        # Remove the completion manifest first so the dashboard never treats a failed rerun as complete.
        (args.output / "summary.json").unlink(missing_ok=True)
        for frame, name in [(customers.orderBy(F.desc("Monetary")), "customers"),
                            (profiles.orderBy("Cluster"), "segments"), (monthly, "monthly_revenue"),
                            (products, "top_products"), (similarity, "similar_customers")]:
            export_csv(frame, args.output / f"{name}.csv")
        with (args.output / "country_revenue.csv").open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["Country", "Revenue"])
            writer.writerows(countries)
        summary = {"dataset": "Synthetic demonstration" if args.synthetic else "User-provided retail transactions",
                   "input": str(args.input), "synthetic": args.synthetic, "reference_date": str(reference),
                   "audit": audit, "customers": rfm.count(),
                   "orders": clean.select("InvoiceNo").distinct().count(),
                   "revenue": float(rfm.agg(F.sum("Monetary")).first()[0]),
                   "similarity_target": target, "model": metrics,
                   "revenue_definition": "Positive purchase revenue, excluding returns/cancellations; not net revenue."}
        temporary = args.output / "summary.json.tmp"
        temporary.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        temporary.replace(args.output / "summary.json")
        print(json.dumps(summary, indent=2))
        print(f"Finished. Dashboard: python -m streamlit run app.py -- --results {args.output}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
