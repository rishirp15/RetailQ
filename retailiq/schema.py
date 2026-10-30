"""Input schema and CSV reader shared by baseline and advanced Spark jobs."""
import csv
from pathlib import Path

from pyspark.sql.types import DoubleType, StringType, StructField, StructType


NUMERIC_COLUMNS = {"Quantity", "UnitPrice"}


def transaction_schema(columns):
    """Build an explicit schema in source-column order while preserving extra fields."""
    if len(columns) != len(set(columns)):
        raise ValueError("Transaction CSV contains duplicate column names.")
    return StructType([
        StructField(name, DoubleType() if name in NUMERIC_COLUMNS else StringType(), True)
        for name in columns
    ])


def read_transactions(spark, path):
    """Read a CSV with types chosen by column name, supporting reordered/extra columns."""
    path = Path(path).resolve()
    with path.open("r", newline="", encoding="utf-8-sig") as handle:
        try:
            columns = next(csv.reader(handle))
        except StopIteration as error:
            raise ValueError(f"Transaction CSV is empty: {path}") from error
    if not columns or any(not name for name in columns):
        raise ValueError(f"Transaction CSV has an empty or invalid header: {path}")
    schema = transaction_schema(columns)
    return (spark.read.schema(schema).option("header", True).option("enforceSchema", False)
            .option("multiLine", True).option("escape", '"').csv(str(path)))
