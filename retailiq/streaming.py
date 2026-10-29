"""Replay historical purchases through real Spark Structured Streaming and recover checkpoints."""
import argparse
import csv
import json
import time
from decimal import Decimal
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DecimalType
from retailiq.pipeline import export_csv
from retailiq.runtime import create_spark
from retailiq.storage import file_hash, write_json

EVENT_SCHEMA = StructType([StructField("EventID", StringType()), StructField("InvoiceDate", TimestampType()),
    StructField("Country", StringType()), StructField("Revenue", DecimalType(20, 2))])


def replay_stream(spark, events, directory, batches=3, batch_rows=2000, interval=0, watch=False):
    if batches < 1 or batch_rows < 1 or interval < 0:
        raise ValueError("Batch count and size must be positive; interval must be nonnegative.")
    events, directory = Path(events), Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    inbox, staged = directory / "inbox", directory / "staged"
    inbox.mkdir(exist_ok=True)
    staged.mkdir(exist_ok=True)
    source = {"input_sha256": file_hash(events), "batch_rows": batch_rows}
    source_file = directory / "source.json"
    if source_file.exists() and json.loads(source_file.read_text()) != source:
        raise ValueError("Streaming source or batch size changed. Use a new --directory to preserve checkpoint correctness.")
    write_json(source_file, source)
    expected_rows, expected_revenue, prepared = 0, Decimal("0"), []
    with events.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for batch in range(batches):
            chunk = []
            for _ in range(batch_rows):
                row = next(reader, None)
                if row is None:
                    break
                chunk.append(row)
            if not chunk:
                break
            name = f"batch-{batch:06}.csv"
            destination = staged / name
            if not destination.exists() and not (inbox / name).exists():
                temporary = destination.with_suffix(".csv.tmp")
                with temporary.open("w", newline="", encoding="utf-8") as out:
                    writer = csv.DictWriter(out, fieldnames=reader.fieldnames)
                    writer.writeheader()
                    writer.writerows(chunk)
                temporary.replace(destination)
            prepared.append(name)
            expected_rows += len(chunk)
            expected_revenue += sum(Decimal(row["Revenue"]) for row in chunk)
    # A resume with a lower batch limit must not pretend already-published files vanished.
    published = sorted(p.name for p in inbox.glob("batch-*.csv"))
    if set(published) - set(prepared):
        raise ValueError("--batches cannot be lower than the number already published in this checkpoint.")
    stream = (spark.readStream.schema(EVENT_SCHEMA).option("header", True).option("maxFilesPerTrigger", 1)
              .option("timestampFormat", "yyyy-MM-dd HH:mm:ss").csv(str(inbox.resolve())))
    aggregated = (stream.withWatermark("InvoiceDate", "2 days")
        .groupBy(F.window("InvoiceDate", "1 day"), "Country")
        .agg(F.count("*").alias("PurchaseRows"), F.sum("Revenue").alias("Revenue")))

    def save_batch(frame, batch_id):
        flat = frame.select(F.col("window.start").alias("WindowStart"), F.col("window.end").alias("WindowEnd"),
                            "Country", "PurchaseRows", "Revenue").cache()
        try:
            totals = flat.agg(F.sum("PurchaseRows"), F.sum("Revenue")).first()
            output = directory / "batches"
            output.mkdir(exist_ok=True)
            export_csv(flat.orderBy("WindowStart", "Country"), output / f"snapshot-{batch_id:06}.csv")
            export_csv(flat.orderBy("WindowStart", "Country"), directory / "stream_totals.csv")
            state = {"simulated": True, "batch_id": batch_id, "purchase_rows": int(totals[0] or 0),
                     "revenue": float(totals[1] or 0), "event_window": "1 day", "watermark_delay": "2 days",
                     "checkpoint": str((directory / "checkpoint").resolve()),
                     "notes": "Historical file replay, not a live retailer feed. Complete mode preserves all window totals; watermark does not evict complete-mode state. Snapshot replacement is idempotent by Spark batch ID."}
            write_json(directory / "summary.json", state)
            print(f"Streaming batch {batch_id}: {state['purchase_rows']:,} cumulative purchase rows", flush=True)
        finally:
            flat.unpersist()

    def start():
        return (aggregated.writeStream.outputMode("complete").foreachBatch(save_batch)
                .option("checkpointLocation", str((directory / "checkpoint").resolve())).start())

    query = start()
    try:
        query.processAllAvailable()
        for name in prepared:
            if not (inbox / name).exists():
                (staged / name).replace(inbox / name)
                query.processAllAvailable()
                if interval:
                    time.sleep(interval)
        progress = query.lastProgress
        if progress:
            write_json(directory / "progress.json", progress)
        state = json.loads((directory / "summary.json").read_text())
        if state["purchase_rows"] != expected_rows or Decimal(str(state["revenue"])) != expected_revenue:
            raise AssertionError("Streaming results do not reconcile to the published source events.")
        query.stop()
        # Restart the exact same Structured Streaming plan/checkpoint without new files.
        query = start()
        query.processAllAvailable()
        recovered = json.loads((directory / "summary.json").read_text())
        if recovered["purchase_rows"] != state["purchase_rows"] or recovered["revenue"] != state["revenue"]:
            raise AssertionError("Checkpoint recovery duplicated or lost events.")
        state.update({"checkpoint_recovery_verified": True, "reconciled": True,
                      "published_batches": len(prepared), "batch_rows": batch_rows,
                      "expected_rows": expected_rows, "expected_revenue": float(expected_revenue),
                      "status": "watching" if watch else "completed"})
        write_json(directory / "summary.json", state)
        if watch:
            print("Watching inbox. Press Ctrl+C to stop.", flush=True)
            query.awaitTermination()
        return state
    finally:
        query.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=Path("outputs/uci/advanced/stream_events.csv"))
    parser.add_argument("--directory", type=Path, default=Path("outputs/uci/advanced/streaming"))
    parser.add_argument("--batches", type=int, default=3, help="Total published batch count, including previously processed files")
    parser.add_argument("--batch-rows", type=int, default=2000)
    parser.add_argument("--interval", type=float, default=3)
    parser.add_argument("--watch", action="store_true")
    args = parser.parse_args()
    spark = create_spark("RetailIQ-streaming")
    try:
        print(json.dumps(replay_stream(spark, args.input, args.directory, args.batches,
                                      args.batch_rows, args.interval, args.watch), indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
