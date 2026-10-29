"""Shared Spark runtime and a source-built adapter for Windows local storage."""
import os
import hashlib
import subprocess
import sys
from pathlib import Path


def portable_filesystem_jar():
    import pyspark
    root = Path(__file__).resolve().parents[1]
    sources = sorted((root / "java/retailiq/fs").glob("*.java"))
    version = hashlib.sha256(b"".join(p.read_bytes() for p in sources)).hexdigest()[:12]
    output = root / f".runtime/retailiq-local-fs-{version}.jar"
    if output.exists():
        return output
    java_home = Path(os.environ.get("JAVA_HOME", ""))
    compiler = java_home / "bin/javac.exe"
    archiver = java_home / "bin/jar.exe"
    if not compiler.is_file():
        raise RuntimeError("The advanced Windows runtime requires JDK 17 (including javac), not just a JRE.")
    classes = root / f".runtime/fs-classes/{version}"
    classes.mkdir(parents=True, exist_ok=True)
    jars = str(Path(pyspark.__file__).parent / "jars/*")
    subprocess.run([str(compiler), "-cp", jars, "-d", str(classes), *map(str, sources)], check=True)
    subprocess.run([str(archiver), "cf", str(output), "-C", str(classes), "."], check=True)
    return output


def create_spark(name="RetailIQ", master="local[2]", partitions=4):
    from pyspark.sql import SparkSession
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    builder = (SparkSession.builder.appName(name).master(master)
               .config("spark.sql.shuffle.partitions", str(partitions))
               .config("spark.sql.ansi.enabled", "false")
               .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.driver.bindAddress", "127.0.0.1")
               .config("spark.ui.showConsoleProgress", "false"))
    if os.name == "nt":
        jar = portable_filesystem_jar()
        if not master.startswith("local"):
            raise ValueError("The Windows filesystem adapter supports local Spark only; use Linux for a cluster.")
        builder = (builder.config("spark.driver.extraClassPath", str(jar))
                   .config("spark.executor.extraClassPath", str(jar))
                   .config("spark.hadoop.fs.file.impl", "retailiq.fs.PortableChecksumFileSystem")
                   .config("spark.hadoop.fs.AbstractFileSystem.file.impl", "retailiq.fs.PortableLocalFs"))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_transactions(spark, path):
    return (spark.read.option("header", True).option("multiLine", True)
            .option("escape", '"').csv(str(Path(path).resolve())))
