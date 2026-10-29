def test_spark_parquet_and_rdd_text_round_trip(spark, tmp_path):
    parquet = str((tmp_path / "parquet").resolve())
    spark.range(5).write.parquet(parquet)
    assert [row.id for row in spark.read.parquet(parquet).orderBy("id").collect()] == list(range(5))
    text = str((tmp_path / "text").resolve())
    spark.sparkContext.parallelize(["model metadata"], 1).saveAsTextFile(text)
    assert spark.sparkContext.textFile(text).collect() == ["model metadata"]
