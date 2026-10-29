"""Repeatable local Spark format, partition and cache measurements."""
from time import perf_counter
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DecimalType


def performance_study(clean, directory, repetitions=2, partitions=(1, 2, 4)):
    if repetitions < 1 or not partitions or min(partitions) < 1:
        raise ValueError("Benchmark repetitions and partition counts must be positive.")
    directory.mkdir(parents=True, exist_ok=True)
    spark = clean.sparkSession
    projection = clean.select("CustomerID", "Country", "Quantity", "Revenue")
    paths = {"CSV": directory / "csv", "Parquet": directory / "parquet"}
    projection.write.mode("overwrite").option("header", True).csv(str(paths["CSV"].resolve()))
    projection.write.mode("overwrite").parquet(str(paths["Parquet"].resolve()))
    expected = {r.Country: (int(r.Rows), r.Revenue) for r in projection.groupBy("Country").agg(
        F.count("*").alias("Rows"), F.sum("Revenue").alias("Revenue")).collect()}
    schema = StructType([StructField("CustomerID", StringType()), StructField("Country", StringType()),
                         StructField("Quantity", DoubleType()), StructField("Revenue", DecimalType(20, 2))])
    original_aqe = spark.conf.get("spark.sql.adaptive.enabled")
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    results = []
    plans = []
    try:
        for repetition in range(1, repetitions + 1):
            formats = ["CSV", "Parquet"] if repetition % 2 else ["Parquet", "CSV"]
            for fmt in formats:
                for partitions_count in partitions:
                    for cached in [False, True]:
                        frame = (spark.read.schema(schema).option("header", True).csv(str(paths[fmt].resolve()))
                                 if fmt == "CSV" else spark.read.parquet(str(paths[fmt].resolve())))
                        frame = frame.repartition(partitions_count)
                        warmup = 0.0
                        if cached:
                            start = perf_counter()
                            frame = frame.cache()
                            frame.count()
                            warmup = perf_counter() - start
                        query = frame.groupBy("Country").agg(F.count("*").alias("Rows"), F.sum("Revenue").alias("Revenue"))
                        start = perf_counter()
                        actual = {r.Country: (int(r.Rows), r.Revenue) for r in query.collect()}
                        seconds = perf_counter() - start
                        if actual != expected:
                            raise AssertionError("Benchmark variants produced different results.")
                        results.append({"Format": fmt, "Partitions": partitions_count, "Cached": cached,
                            "Repetition": repetition, "QuerySeconds": seconds, "CacheBuildSeconds": warmup,
                            "TotalSeconds": seconds + warmup, "Rows": sum(v[0] for v in actual.values()),
                            "BytesOnDisk": sum(p.stat().st_size for p in paths[fmt].rglob("*") if p.is_file()),
                            "Reconciled": True})
                        if repetition == 1:
                            plans.append(f"\n{fmt}, partitions={partitions_count}, cached={cached}\n{query._jdf.queryExecution().toString()}\n")
                        if cached:
                            frame.unpersist(blocking=True)
                        print(f"Benchmark {fmt}, partitions={partitions_count}, cached={cached}: {seconds:.3f}s", flush=True)
    finally:
        spark.conf.set("spark.sql.adaptive.enabled", original_aqe)
    (directory / "execution_plans.txt").write_text("\n".join(plans), encoding="utf-8")
    metadata = {"repetitions": repetitions, "partitions": list(partitions), "spark_master": spark.sparkContext.master,
        "adaptive_execution_during_measurement": False, "rows": sum(v[0] for v in expected.values()),
        "notes": "Local warm-OS-cache measurements, not cluster speedup. Alternating format order; query time includes scan/shuffle/aggregate; cached query excludes separately reported cache-build time. Preparation writes are not timed. Other clean-data caches may remain in memory."}
    return results, metadata
