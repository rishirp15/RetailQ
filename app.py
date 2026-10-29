"""Presentation dashboard. Spark runs separately so opening the UI never retrains a model."""
import argparse
import json
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
from retailiq.dashboard import PAGES, load_advanced, render_page

st.set_page_config(page_title="RetailIQ | Customer Intelligence", page_icon="🛍️", layout="wide", initial_sidebar_state="collapsed")
st.markdown("""<style>
.stApp {background: #f5f7fb;}
[data-testid="stMetric"] {background:white; padding:20px; border-radius:14px; border:1px solid #e5e9f2;}
h1,h2,h3 {color:#172b4d;}
.block-container {padding-left:1.5rem; padding-right:1.5rem;}
[data-testid="stMetricValue"] {font-size:1.6rem;}
</style>""", unsafe_allow_html=True)
parser = argparse.ArgumentParser()
parser.add_argument("--results", default="outputs/uci")
args, _ = parser.parse_known_args()
st.sidebar.title("RetailIQ")
st.sidebar.caption("BIG DATA ANALYTICS • MINI PROJECT")
folder = Path(st.sidebar.text_input("Results folder", args.results))
manifest = folder / "summary.json"
if not manifest.exists():
    st.title("Customer intelligence")
    st.info("Run the Spark pipeline first to generate customer analytics.")
    st.code("python -m retailiq.data\npython -m retailiq.pipeline --input data/raw/online_retail.csv")
    st.caption("Offline demo: python -m retailiq.sample, then run the pipeline with --input data/raw/sample.csv --output outputs/demo --synthetic.")
    st.stop()
try:
    summary = json.loads(manifest.read_text(encoding="utf-8"))
    def read(name):
        return pd.read_csv(folder / f"{name}.csv", dtype={"CustomerID": str, "RFMScore": str})
    customers, segments = read("customers"), read("segments")
    monthly, countries = read("monthly_revenue"), read("country_revenue")
    products, similar = read("top_products"), read("similar_customers")
except (OSError, ValueError) as error:
    st.error(f"Results could not be read. Rerun the pipeline. Details: {error}")
    st.stop()
advanced = load_advanced(folder, summary)
page = st.sidebar.selectbox("Explore", ["Core dashboard", *PAGES])
selected = (advanced or {}).get("features", {}).get("clustering", {})
if selected.get("status") == "completed":
    use_selected = st.sidebar.checkbox("Use automatically selected clusters", value=True)
    if use_selected:
        customers = pd.read_csv(folder / "advanced/selected_customers.csv", dtype={"CustomerID": str, "RFMScore": str})
        segments = pd.read_csv(folder / "advanced/selected_segments.csv")
        summary["model"] = {**summary["model"], "k": selected["metrics"]["K"],
                            "silhouette_squared_euclidean": selected["metrics"]["Silhouette"]}
if summary["synthetic"]:
    st.warning("SYNTHETIC DEMO DATA — these figures are for demonstration, not UCI findings.")
st.sidebar.write(f"Snapshot: {summary['reference_date']}")
st.sidebar.write(f"K-Means: {summary['model']['k']} clusters")
silhouette = summary["model"]["silhouette_squared_euclidean"]
st.sidebar.metric("Silhouette (squared Euclidean)", f"{silhouette:.3f}" if silhouette is not None else "N/A")
st.sidebar.caption("A descriptive in-sample metric, not prediction accuracy.")
st.title("Customer intelligence" if page == "Core dashboard" else page)
if page != "Core dashboard":
    st.caption(f"{summary['customers']:,} customers · {summary['orders']:,} orders · £{summary['revenue']:,.2f} purchase revenue · snapshot {summary['reference_date']}")
    render_page(page, folder, advanced)
    st.stop()
st.caption("Understand who buys, what drives revenue, and where to act next.")
for column, label, value in zip(st.columns(4), ["Customers", "Orders", "Purchase revenue", "Clean purchase rows"],
    [f"{summary['customers']:,}", f"{summary['orders']:,}", f"£{summary['revenue'] / 1_000_000:.2f}M", f"{summary['audit']['cleaned_rows']:,}"]):
    column.metric(label, value)
st.caption(f"Total purchase revenue: £{summary['revenue']:,.2f}. " + summary["revenue_definition"])
overview, explorer, behavior, quality = st.tabs(["Segment overview", "Customer explorer", "Purchase behavior", "Method & data quality"])
with overview:
    left, right = st.columns(2)
    chart = segments.copy()
    chart["ClusterLabel"] = "C" + chart.Cluster.astype(str) + " · " + chart.Segment
    left.plotly_chart(px.bar(chart, x="ClusterLabel", y="Revenue", color="Segment", title="Revenue by customer cluster",
                              labels={"Revenue": "Purchase revenue (£)", "ClusterLabel": "Cluster"}), width='stretch')
    right.plotly_chart(px.pie(chart, names="ClusterLabel", values="Customers", hole=.65, title="Customer distribution"), width='stretch')
    points = customers.sample(min(len(customers), 3000), random_state=42)
    st.plotly_chart(px.scatter(points, x="Recency", y="Monetary", color="Segment", size="Frequency",
        hover_data=["CustomerID", "Cluster"], log_y=True, title="Customer value versus days since last purchase",
        labels={"Recency": "Days since last purchase", "Monetary": "Total purchase value (£)"}), width='stretch')
    st.caption("Scatter shows up to 3,000 customers. All other totals use the complete results.")
    st.subheader("Suggested business actions")
    st.dataframe(segments[["Cluster", "Segment", "Customers", "AvgRecency", "AvgFrequency", "AvgMonetary", "RevenueShare", "Recommendation"]], hide_index=True, width='stretch')
with explorer:
    options = sorted(customers.Segment.unique())
    chosen = st.multiselect("Segments", options, default=options)
    only_high = st.checkbox("High-value customers only (M score ≥ 4)")
    only_inactive = st.checkbox("Inactive customers only (90+ days)")
    filtered = customers[customers.Segment.isin(chosen)]
    if only_high:
        filtered = filtered[filtered.MScore >= 4]
    if only_inactive:
        filtered = filtered[filtered.Recency >= 90]
    st.write(f"{len(filtered):,} matching customers")
    st.dataframe(filtered, hide_index=True, width='stretch')
    st.download_button("Download selected customers", filtered.to_csv(index=False), "retailiq_customers.csv", "text/csv")
    st.subheader(f"Similar purchase baskets to customer {summary['similarity_target']}")
    st.caption("Cosine similarity of product quantities: 1 means the same product proportions; 0 means no product overlap. Change the target with the pipeline's --similar-to option.")
    st.dataframe(similar, hide_index=True, width='stretch')
with behavior:
    st.plotly_chart(px.line(monthly, x="Month", y="Revenue", markers=True, title="Monthly purchase revenue (£)"), width='stretch')
    left, right = st.columns(2)
    left.plotly_chart(px.bar(countries.head(10), x="Revenue", y="Country", orientation="h", title="Top countries • RDD aggregation"), width='stretch')
    right.plotly_chart(px.bar(products.head(10), x="Revenue", y="Description", orientation="h", title="Top products by purchase revenue"), width='stretch')
with quality:
    st.subheader("Cleaning audit")
    st.json(summary["audit"])
    st.markdown("""
1. **Cleaning:** cast types, remove unidentified customers, cancellations, invalid dates, non-positive quantities/prices and exact duplicates.
2. **RFM with Spark SQL:** recency = snapshot date − last purchase; frequency = distinct invoices; monetary = sum of positive purchase values.
3. **RFM scores:** approximate quintile boundaries; recent, frequent, higher-spending customers receive higher scores. Ties stay together, so bucket sizes can differ.
4. **MLlib:** log(1 + RFM), standardize, then seeded K-Means. Cluster descriptions come from average RFM scores; multiple clusters may share a description.
5. **Similarity:** distributed product-quantity aggregation, dot product and cosine normalization against one customer.
6. **RDD operations:** map → reduceByKey → sortBy for country revenue.

**Limitations:** returns are excluded, so revenue is not net revenue. Exact duplicate removal is an assumption. A 90-day inactivity threshold is a business rule. K=4 is the retained baseline; automatic model selection compares K=2–8 when advanced results are available. Segment recommendations are hypotheses to test. This dataset fits on one computer; local Spark demonstrates APIs, not cluster speedup.
""")
    st.json(summary["model"])
