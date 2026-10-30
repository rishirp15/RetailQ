"""Run a reproducible local row-scaling study without changing baseline outputs."""
import argparse
from pathlib import Path
from time import perf_counter

from pyspark.sql import functions as F

from retailiq.analytics import clean_transactions
from retailiq.runtime import create_spark, read_transactions
from retailiq.storage import write_json, write_records


def run_scale_study(clean, factors=(1, 2, 4), repetitions=2, partitions=4):
    if not factors or min(factors) < 1 or repetitions < 1 or partitions < 1:
        raise ValueError("Scale factors, repetitions, and partition count must be positive.")

    spark = clean.sparkSession
    base = clean.select("Country", "Revenue")
    expected_rows = clean.count()
    expected = {
        row.Country: (int(row.Rows), row.Revenue)
        for row in base.groupBy("Country").agg(
            F.count("*").alias("Rows"), F.sum("Revenue").alias("Revenue")
        ).collect()
    }
    replica_count = max(factors)
    replicas = spark.range(0, replica_count, numPartitions=partitions).select(
        F.col("id").alias("Replica")
    )
    results = []

    for factor in factors:
        expanded = (base.crossJoin(replicas.filter(F.col("Replica") < factor))
                    .repartition(partitions))
        for repetition in range(1, repetitions + 1):
            started = perf_counter()
            actual = {
                row.Country: (int(row.Rows), row.Revenue)
                for row in expanded.groupBy("Country").agg(
                    F.count("*").alias("Rows"), F.sum("Revenue").alias("Revenue")
                ).collect()
            }
            elapsed = perf_counter() - started
            reconciled = set(actual) == set(expected) and all(
                actual[country][0] == rows * factor
                and actual[country][1] == revenue * factor
                for country, (rows, revenue) in expected.items()
            )
            if not reconciled:
                raise AssertionError(f"Scaled aggregation did not reconcile for factor {factor}.")
            results.append({
                "ScaleFactor": factor,
                "Repetition": repetition,
                "Rows": expected_rows * factor,
                "Partitions": partitions,
                "QuerySeconds": round(elapsed, 4),
                "Reconciled": reconciled,
            })
            print(f"Scale {factor}x, repetition {repetition}: "
                  f"{expected_rows * factor:,} rows in {elapsed:.3f}s; reconciled={reconciled}",
                  flush=True)

    return results, {
        "spark_version": spark.version,
        "master": spark.sparkContext.master,
        "partitions": partitions,
        "base_rows": expected_rows,
        "factors": list(factors),
        "repetitions": repetitions,
        "operation": "Cross-join cleaned rows with replica IDs, then aggregate count and Decimal revenue by country.",
        "scope": "Local synthetic row replication. Results demonstrate workload growth and reconciliation, not multi-node scale-out or production performance.",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=Path("data/raw/online_retail.csv"))
    parser.add_argument("--output", type=Path, default=Path("outputs/scale_study"))
    parser.add_argument("--factors", type=int, nargs="+", default=[1, 2, 4])
    parser.add_argument("--repetitions", type=int, default=2)
    parser.add_argument("--partitions", type=int, default=4)
    parser.add_argument("--master", default="local[2]")
    args = parser.parse_args()
    if not args.input.is_file():
        parser.error(f"Input file not found: {args.input}")
    if not args.factors or min(args.factors) < 1 or args.repetitions < 1 or args.partitions < 1:
        parser.error("Factors, repetitions, and partitions must be positive.")

    spark = create_spark("RetailIQ-scale-study", args.master, args.partitions)
    try:
        clean, audit = clean_transactions(read_transactions(spark, args.input))
        results, metadata = run_scale_study(
            clean, args.factors, args.repetitions, args.partitions
        )
        args.output.mkdir(parents=True, exist_ok=True)
        write_records(args.output / "scale_results.csv", results,
                      ["ScaleFactor", "Repetition", "Rows", "Partitions", "QuerySeconds", "Reconciled"])
        write_json(args.output / "scale_summary.json", {**metadata, "audit": audit})
        print(f"Saved scale study to {args.output}", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
