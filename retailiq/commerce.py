"""Frequent baskets, implicit ALS recommendations and chronological ranking evaluation."""
import math
from datetime import timedelta

from pyspark.ml.fpm import FPGrowth
from pyspark.ml.recommendation import ALS
from pyspark.sql import Window, functions as F


def market_baskets(clean, min_support=.02, min_confidence=.25):
    if not 0 < min_support <= 1 or not 0 <= min_confidence <= 1:
        raise ValueError("Support must be in (0,1] and confidence in [0,1].")
    baskets = clean.groupBy("CustomerID", "InvoiceNo").agg(F.sort_array(F.collect_set("StockCode")).alias("items")).cache()
    count = baskets.count()
    model = FPGrowth(itemsCol="items", minSupport=min_support, minConfidence=min_confidence, numPartitions=4).fit(baskets)
    rules = model.associationRules.filter("lift > 1").select(
        F.to_json("antecedent").alias("Antecedent"), F.to_json("consequent").alias("Consequent"),
        F.col("confidence").alias("Confidence"), F.col("lift").alias("Lift"), F.col("support").alias("Support"))
    rules = rules.orderBy(F.desc("Lift"), F.desc("Support"), "Antecedent", "Consequent").limit(300).cache()
    items = model.freqItemsets.select(F.to_json("items").alias("Items"), F.col("freq").alias("Frequency"),
                                     (F.col("freq") / count).alias("Support"))
    items = items.orderBy(F.desc("Frequency"), "Items").limit(300).cache()
    metadata = {"baskets": count, "min_support": min_support, "min_confidence": min_confidence,
                "exported_rules": rules.count(), "export_limit": 300,
                "meaning": "Positive-purchase invoice baskets; unique products per basket. Rules are associations, not causal effects."}
    items.count()
    baskets.unpersist()
    return rules, items, metadata


def ranking_metrics(recommended, relevant, k=5):
    """Macro-averaged precision/recall/NDCG over customers with novel holdout products."""
    if k < 1:
        raise ValueError("Ranking K must be positive.")
    totals = [0.0, 0.0, 0.0, 0.0]
    count = 0
    for customer, actual in relevant.items():
        actual = set(actual)
        if not actual:
            continue
        predicted = list(dict.fromkeys(recommended.get(customer, [])))[:k]
        hits = [int(item in actual) for item in predicted]
        dcg = sum(hit / math.log2(index + 2) for index, hit in enumerate(hits))
        ideal = sum(1 / math.log2(index + 2) for index in range(min(k, len(actual))))
        values = [sum(hits) / k, sum(hits) / len(actual), dcg / ideal, float(bool(sum(hits)))]
        totals = [a + b for a, b in zip(totals, values)]
        count += 1
    keys = [f"precision_at_{k}", f"recall_at_{k}", f"ndcg_at_{k}", f"hit_rate_at_{k}"]
    return {**dict(zip(keys, [v / count if count else None for v in totals])), "evaluated_customers": count}


def _novel_recommendations(model, users, seen, catalog_count, n=5):
    # Bucket customers by history size so a wholesaler's huge basket does not force
    # thousands of candidates for every customer. Each bucket still guarantees n novel candidates when possible.
    history = seen.select("user", "item").distinct().groupBy("user").count()
    requested = users.select("user").distinct().join(history, "user", "left").fillna({"count": 0})
    caps = sorted(set([min(catalog_count, value) for value in [20, 50, 100, 250, 500, 1000, catalog_count]]))
    bucket = F.lit(catalog_count)
    for cap in reversed(caps):
        bucket = F.when(F.col("count") + n <= cap, cap).otherwise(bucket)
    requested = requested.withColumn("CandidateCount", bucket).cache()
    exploded = None
    for size in sorted(row.CandidateCount for row in requested.select("CandidateCount").distinct().collect()):
        subset = requested.filter(F.col("CandidateCount") == size).select("user")
        candidates = model.recommendForUserSubset(subset, int(size))
        part = candidates.select("user", F.explode("recommendations").alias("r")).select(
            "user", F.col("r.item").alias("item"), F.col("r.rating").alias("Score"))
        exploded = part if exploded is None else exploded.unionByName(part)
    if exploded is None:
        requested.unpersist()
        return seen.sparkSession.createDataFrame([], "user int, item int, Score double, Rank int")
    novel = exploded.join(seen.select("user", "item").distinct(), ["user", "item"], "left_anti")
    order = Window.partitionBy("user").orderBy(F.desc("Score"), "item")
    result = novel.withColumn("Rank", F.row_number().over(order)).filter(F.col("Rank") <= n).cache()
    result.count()
    requested.unpersist()
    return result


def train_recommendations(clean, reference_date, holdout_days=60, n=5):
    cutoff = reference_date - timedelta(days=holdout_days)
    users = clean.select("CustomerID").distinct().withColumn(
        "user", (F.row_number().over(Window.orderBy("CustomerID")) - 1).cast("int")).cache()
    products = clean.select("StockCode").distinct().withColumn(
        "item", (F.row_number().over(Window.orderBy("StockCode")) - 1).cast("int")).cache()
    catalog_count = products.count()
    interactions = clean.join(users, "CustomerID").join(products, "StockCode")
    train = interactions.filter(F.col("InvoiceDate") < F.lit(cutoff)).groupBy("user", "item").agg(
        F.countDistinct("InvoiceNo").cast("float").alias("strength")).cache()
    if not train.take(1):
        raise ValueError("Not enough history for the recommendation holdout window.")
    als = ALS(userCol="user", itemCol="item", ratingCol="strength", implicitPrefs=True,
              rank=12, maxIter=10, regParam=.1, alpha=10, seed=42, coldStartStrategy="drop",
              numUserBlocks=2, numItemBlocks=2, checkpointInterval=-1)
    print("Training implicit ALS on purchases before the holdout cutoff...", flush=True)
    evaluation_model = als.fit(train)
    test = interactions.filter(F.col("InvoiceDate") >= F.lit(cutoff)).select("user", "item").distinct()
    # Evaluate discovery of unseen products. Repeat purchases are intentionally excluded.
    relevant = test.join(train.select("user", "item"), ["user", "item"], "left_anti").cache()
    relevant_map = {r.user: r.actual for r in relevant.groupBy("user").agg(F.collect_set("item").alias("actual")).collect()}
    evaluated = _novel_recommendations(evaluation_model, relevant.select("user"), train, catalog_count, n).cache()
    rows = evaluated.orderBy("user", "Rank").collect()
    recommendation_map = {}
    for row in rows:
        recommendation_map.setdefault(row.user, []).append(row.item)
    metrics = ranking_metrics(recommendation_map, relevant_map, n)
    popular = [r.item for r in train.groupBy("item").agg(F.sum("strength").alias("Popularity"))
               .orderBy(F.desc("Popularity"), "item").collect()]
    seen = {r.user: set(r.seen) for r in train.groupBy("user").agg(F.collect_set("item").alias("seen")).collect()}
    baseline = {user: [item for item in popular if item not in seen.get(user, set())][:n] for user in relevant_map}
    metrics = {**metrics, "popularity_baseline": ranking_metrics(baseline, relevant_map, n),
               "cutoff": str(cutoff), "holdout_end_exclusive": str(reference_date), "holdout_days": holdout_days,
               "customers_with_recommendations": len(recommendation_map),
               "recommendation_customer_coverage": len(recommendation_map) / len(relevant_map) if relevant_map else 0,
               "novel_holdout_pairs": relevant.count(),
               "known_holdout_pairs": relevant.join(train.select("user").distinct(), "user").join(train.select("item").distinct(), "item").count(),
               "rank": 12, "implicit_feedback": "Distinct purchase invoice count per customer/product",
               "scope": "Novel-product ranking. Cold-start customers remain in the denominator with zero hits. ALS scores are not probabilities."}
    print("Refitting ALS on all observed purchases for the dashboard...", flush=True)
    all_pairs = interactions.groupBy("user", "item").agg(F.countDistinct("InvoiceNo").cast("float").alias("strength")).cache()
    final_model = als.fit(all_pairs)
    candidates = _novel_recommendations(final_model, users, all_pairs, catalog_count, n)
    recommendations = candidates.join(users, "user").join(products, "item").select(
        "CustomerID", "StockCode", "Rank", "Score").orderBy("CustomerID", "Rank").cache()
    metrics["recommendation_rows"] = recommendations.count()
    candidates.unpersist()
    for frame in [evaluated, relevant, train, all_pairs, users, products]:
        frame.unpersist()
    return recommendations, metrics, final_model
