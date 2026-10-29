"""Advanced presentation pages. This module reads reports and never starts Spark jobs."""
import json
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

PAGES = {"Model selection": "clustering", "Basket analysis": "baskets", "Product recommendations": "recommendations",
         "Retention cohorts": "cohorts", "Segment migration": "migration", "Inactivity prediction": "prediction",
         "Streaming replay": "streaming", "Spark performance": "performance"}


def report(folder, name):
    return pd.read_csv(Path(folder) / f"{name}.csv", dtype={"CustomerID": str, "StockCode": str, "RFMScore": str})


def load_advanced(folder, baseline):
    path = Path(folder) / "advanced/advanced_summary.json"
    if not path.exists():
        return None
    state = json.loads(path.read_text(encoding="utf-8"))
    if (state.get("reference_date") != baseline["reference_date"] or state.get("customers") != baseline["customers"]
            or state.get("audit") != baseline["audit"] or state.get("synthetic") != baseline["synthetic"]):
        st.sidebar.warning("Advanced results do not match this baseline. Rebuild them from the same input.")
        return None
    return state


def render_page(page, folder, state):
    folder = Path(folder) / "advanced"
    feature = PAGES[page]
    stage = (state or {}).get("features", {}).get(feature, {})
    if stage.get("status") != "completed":
        status = stage.get("status", "not generated")
        st.info(f"{page}: {status}.")
        if stage.get("error"):
            st.error(stage["error"])
        st.code("python -m retailiq.advanced")
        if state:
            st.dataframe(pd.DataFrame([{"Feature": key, "Status": value["status"]} for key, value in state["features"].items()]), hide_index=True)
        return
    metrics = stage["metrics"]
    if page == "Model selection":
        comparison = report(folder, "k_comparison")
        runs = report(folder, "k_runs")
        cols = st.columns(3)
        cols[0].metric("Selected K", metrics["K"])
        cols[1].metric("Mean silhouette", f"{metrics['MeanSilhouette']:.3f}")
        stability = metrics.get("MeanStabilityARI")
        cols[2].metric("Mean seed stability (ARI)", f"{stability:.3f}" if stability is not None else "N/A")
        st.caption(metrics["SelectionRule"])
        st.plotly_chart(px.line(comparison, x="K", y="MeanSilhouette", markers=True, title="Cluster separation across K values"), width="stretch")
        st.plotly_chart(px.scatter(runs, x="K", y="Silhouette", color=runs.Seed.astype(str),
            hover_data=["MinClusterSize", "TrainingCost"], title="Each seeded experiment", labels={"color": "Seed"}), width="stretch")
        st.dataframe(comparison, hide_index=True, width="stretch")
        st.caption("ARI compares assignments against the first seed for the same K and ignores arbitrary cluster numbering. In-sample silhouette does not measure future predictive accuracy.")
        with st.expander("All experiment details"):
            st.dataframe(runs, hide_index=True, width="stretch")
    elif page == "Basket analysis":
        rules = report(folder, "basket_rules")
        catalog = report(folder, "product_catalog").fillna({"Description": "Unnamed product"})
        names = dict(zip(catalog.StockCode, catalog.Description))
        def describe(value):
            return "; ".join(f"{names.get(code, code)} [{code}]" for code in json.loads(value))
        st.caption(f"{metrics['baskets']:,} invoice baskets • fitted support ≥ {metrics['min_support']:.0%} • up to {metrics['export_limit']} rules exported, ranked by lift.")
        confidence = st.slider("Minimum confidence", float(metrics["min_confidence"]), 1.0, float(metrics["min_confidence"]), .05)
        lift = st.number_input("Minimum lift", min_value=1.0, value=1.0, step=.1)
        search = st.text_input("Find a product in a rule")
        rules["Bought together"] = rules.Antecedent.map(describe)
        rules["Associated product"] = rules.Consequent.map(describe)
        filtered = rules[(rules.Confidence >= confidence) & (rules.Lift >= lift)]
        if search:
            filtered = filtered[filtered["Bought together"].str.contains(search, case=False, regex=False) |
                                filtered["Associated product"].str.contains(search, case=False, regex=False)]
        st.metric("Matching exported rules", len(filtered))
        st.caption("Confidence is P(consequent | antecedent). Lift compares that probability with the consequent's overall popularity. Association does not prove causation.")
        display = filtered[["Bought together", "Associated product", "Support", "Confidence", "Lift"]]
        st.dataframe(display, hide_index=True, width="stretch")
        st.download_button("Download basket rules", display.to_csv(index=False), "basket_rules.csv", "text/csv")
        with st.expander("Frequent product sets"):
            itemsets = report(folder, "frequent_itemsets")
            itemsets["Products"] = itemsets.Items.map(describe)
            st.dataframe(itemsets[["Products", "Frequency", "Support"]], hide_index=True, width="stretch")
    elif page == "Product recommendations":
        recommendations = report(folder, "recommendations")
        catalog = report(folder, "product_catalog")
        st.caption("Implicit ALS learns from distinct purchase invoices. Recommendations exclude each customer's previously purchased products.")
        if recommendations.empty:
            st.info("No unseen-product candidates were available.")
            return
        customer = st.selectbox("Choose a customer", sorted(recommendations.CustomerID.unique()))
        chosen = recommendations[recommendations.CustomerID == customer].merge(catalog, on="StockCode", how="left").sort_values("Rank")
        for item in chosen.itertuples():
            with st.container(border=True):
                st.markdown(f"**{int(item.Rank)}. {item.Description or item.StockCode}**")
                st.caption(f"Product {item.StockCode} · preference score {item.Score:.3f}")
        st.subheader("Performance on later purchases")
        baseline = metrics["popularity_baseline"]
        evaluation = pd.DataFrame([{"Metric": label, "ALS": metrics[key], "Popularity baseline": baseline[key]}
            for label, key in [("Precision@5", "precision_at_5"), ("Recall@5", "recall_at_5"), ("NDCG@5", "ndcg_at_5"), ("Hit rate@5", "hit_rate_at_5")]])
        st.dataframe(evaluation, hide_index=True, width="stretch")
        st.caption(f"Train before {metrics['cutoff']}; test until {metrics['holdout_end_exclusive']} (exclusive). {metrics['evaluated_customers']:,} customers with novel holdout products. Customer coverage: {metrics['recommendation_customer_coverage']:.1%}. {metrics['scope']}")
        st.caption("After evaluation, the dashboard model is refitted on all observed data. Its scores indicate preference strength, not purchase probability.")
        st.download_button("Download this customer's recommendations", chosen.to_csv(index=False), f"recommendations_{customer}.csv", "text/csv")
    elif page == "Retention cohorts":
        cohorts = report(folder, "retention_cohorts")
        matrix = cohorts.pivot(index="Cohort", columns="MonthIndex", values="Retention")
        sizes = cohorts.groupby("Cohort").CohortSize.first()
        matrix.index = [f"{cohort[:7]} (n={sizes[cohort]:,})" for cohort in matrix.index]
        matrix.columns = [f"+{int(value)}" for value in matrix.columns]
        st.plotly_chart(px.imshow(matrix, text_auto=".0%", aspect="auto", range_color=[0, 1],
            color_continuous_scale="Blues", labels={"x": "Months since first observed purchase", "y": "First purchase cohort", "color": "Retention"},
            title="Monthly customer retention"), width="stretch")
        st.caption(metrics["scope"])
        st.caption("Month 0 is 100% by definition. A customer can return after missing a month. This measures observed repeat purchasing, not continuous subscription retention.")
        st.dataframe(cohorts, hide_index=True, width="stretch")
    elif page == "Segment migration":
        transitions = report(folder, "segment_migration")
        counts = report(folder, "monthly_segments")
        snapshots = report(folder, "monthly_customers")
        if transitions.empty:
            st.info("More than one snapshot is needed for a migration diagram.")
            return
        period = st.selectbox("Transition ending at snapshot", sorted(transitions.ToSnapshot.unique()), index=len(transitions.ToSnapshot.unique()) - 1)
        chosen = transitions[transitions.ToSnapshot == period]
        left = {int(r.FromCluster): r.FromSegment for r in chosen.itertuples()}
        right = {int(r.ToCluster): r.ToSegment for r in chosen.itertuples()}
        nodes = [("from", cluster) for cluster in sorted(left)] + [("to", cluster) for cluster in sorted(right)]
        lookup = {node: i for i, node in enumerate(nodes)}
        labels = [f"{'Previous' if side == 'from' else 'Current'} C{cluster} · {(left if side == 'from' else right)[cluster]}" for side, cluster in nodes]
        figure = go.Figure(go.Sankey(node={"label": labels, "pad": 20}, link={
            "source": [lookup[("from", int(r.FromCluster))] for r in chosen.itertuples()],
            "target": [lookup[("to", int(r.ToCluster))] for r in chosen.itertuples()],
            "value": chosen.Customers.tolist()}))
        figure.update_layout(title=f"Customer movement into {period}", height=500)
        st.plotly_chart(figure, width="stretch")
        counts["ClusterLabel"] = "C" + counts.Cluster.astype(str) + " · " + counts.Segment
        st.plotly_chart(px.area(counts, x="SnapshotDate", y="Customers", color="ClusterLabel", title="Segment membership over time"), width="stretch")
        st.caption(metrics["scope"])
        customer = st.selectbox("Inspect a customer's monthly history", sorted(snapshots.CustomerID.unique()))
        st.dataframe(snapshots[snapshots.CustomerID == customer], hide_index=True, width="stretch")
    elif page == "Inactivity prediction":
        risks = report(folder, "inactivity_risk")
        importance = report(folder, "feature_importance")
        cols = st.columns(4)
        for col, label, key in zip(cols, ["Holdout ROC AUC", "Holdout PR AUC", "Precision", "Recall"], ["roc_auc", "pr_auc", "precision", "recall"]):
            value = metrics[key]
            col.metric(label, f"{value:.3f}" if value is not None else "N/A")
        st.caption(f"Target: {metrics['label_definition']}. Evaluation snapshot {metrics['holdout_snapshot']}; outcomes observed until {metrics['holdout_end_exclusive']} (exclusive).")
        st.write(f"Accuracy: **{metrics['accuracy']:.1%}** · majority-class baseline: **{metrics['majority_class_accuracy']:.1%}** · non-purchase prevalence: **{metrics['nonpurchase_prevalence']:.1%}**")
        st.caption(metrics["score_note"])
        threshold = st.slider("Minimum future inactivity risk score", 0.0, 1.0, .7, .05)
        chosen = risks[risks.RiskScore >= threshold]
        st.metric("Customers above selected threshold", len(chosen))
        st.caption(f"Latest scores use the {metrics['latest_snapshot']} snapshot. Future outcomes for these scores are not observed in this dataset.")
        st.dataframe(chosen, hide_index=True, width="stretch")
        st.download_button("Download selected risk scores", chosen.to_csv(index=False), "inactivity_risk.csv", "text/csv")
        st.plotly_chart(px.bar(importance.sort_values("Importance"), x="Importance", y="Feature", orientation="h", title="Historical evaluation model feature importance"), width="stretch")
        confusion = metrics["confusion_matrix"]
        st.plotly_chart(px.imshow([[confusion["tn"], confusion["fp"]], [confusion["fn"], confusion["tp"]]],
            x=["Predict purchase", "Predict no purchase"], y=["Actual purchase", "Actual no purchase"], text_auto=True,
            color_continuous_scale="Blues", title="Holdout confusion matrix (threshold 0.5)"), width="stretch")
        with st.expander("Time split and evaluation details"):
            st.json(metrics)
    elif page == "Streaming replay":
        render_stream(folder)
    elif page == "Spark performance":
        measurements = report(folder, "performance")
        view = measurements.groupby(["Format", "Partitions", "Cached"], as_index=False).agg(
            MedianQuerySeconds=("QuerySeconds", "median"), MedianCacheBuildSeconds=("CacheBuildSeconds", "median"),
            MedianTotalSeconds=("TotalSeconds", "median"), BytesOnDisk=("BytesOnDisk", "first"))
        view["Cache"] = view.Cached.map({True: "Warm Spark cache", False: "No Spark cache"})
        st.plotly_chart(px.line(view, x="Partitions", y="MedianQuerySeconds", color="Format", line_dash="Cache", markers=True,
            title="Median country-revenue query time", labels={"MedianQuerySeconds": "Seconds"}), width="stretch")
        st.dataframe(view, hide_index=True, width="stretch")
        st.caption(metrics["notes"])
        st.caption("Cache-build time must be included when deciding whether caching helps a one-off query. Small local jobs can become slower with more partitions.")
        with st.expander("Measured repetitions"):
            st.dataframe(measurements, hide_index=True, width="stretch")
        with st.expander("Spark logical and physical execution plans"):
            st.code((folder / "benchmark_data/execution_plans.txt").read_text(encoding="utf-8"), language="text")


@st.fragment(run_every=5)
def render_stream(folder):
    directory = folder / "streaming"
    summary = json.loads((directory / "summary.json").read_text())
    totals = report(directory, "stream_totals")
    st.info("SIMULATED HISTORICAL STREAM — transactions are replayed from UCI files through Spark Structured Streaming.")
    cols = st.columns(3)
    cols[0].metric("Processed purchase rows", f"{summary['purchase_rows']:,}")
    cols[1].metric("Stream purchase revenue", f"£{summary['revenue']:,.2f}")
    cols[2].metric("Published micro-batches", summary.get("published_batches", summary["batch_id"] + 1))
    daily = totals.groupby("WindowStart", as_index=False).Revenue.sum()
    st.plotly_chart(px.line(daily, x="WindowStart", y="Revenue", markers=True, title="Event-time daily revenue in the replay"), width="stretch")
    if summary.get("checkpoint_recovery_verified"):
        st.success("Checkpoint restart verified: processed events were neither lost nor counted twice.")
    else:
        st.info("Streaming checkpoint is being checked.")
    st.caption("This page refreshes every 5 seconds while open. The bundled demonstration is finite; extend it with the replay command to observe more batches.")
    with st.expander("Replay controls and checkpoint details"):
        st.code("python -m retailiq.streaming --batches 6 --interval 3 --watch")
        st.caption("The batch count is the total to publish, including previously processed batches. Keep the same source and batch size when resuming. Stop watch mode with Ctrl+C.")
        st.json(summary)
