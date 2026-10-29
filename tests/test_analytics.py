from datetime import date

import pytest
from retailiq.analytics import clean_transactions, compute_rfm, similar_customers, country_revenue_rdd, cluster_customers
from retailiq.data import HEADERS


def row(invoice, customer="1", product="A", quantity="2", price="5", day="01"):
    return [invoice, product, "Gift", quantity, f"2011-12-{day} 10:00:00", price, customer, "UK"]


def test_cleaning_rfm_and_rdd(spark):
    rows = [row("1"), row("1"), row("1", product="B"), row("2", day="03"),
            row("C3"), row("4", customer=None), row("5", quantity="-1"), row("6", price="0")]
    clean, audit = clean_transactions(spark.createDataFrame(rows, HEADERS))
    assert audit == {"cancelled": 1, "missing_customer": 1, "invalid_quantity": 1, "invalid_price": 1,
                     "duplicates": 1, "cleaned_rows": 3, "raw_rows": 8}
    rfm, reference = compute_rfm(clean)
    customer = rfm.first()
    assert reference == date(2011, 12, 4)
    assert (customer.Recency, customer.Frequency, float(customer.Monetary)) == (1, 2, 30.0)
    assert float(country_revenue_rdd(clean)[0][1]) == 30.0
    with pytest.raises(ValueError, match="Reference date"):
        compute_rfm(clean, date(2011, 12, 1))
    with pytest.raises(ValueError, match="distinct RFM"):
        cluster_customers(rfm, 4)
    clean.unpersist()


def test_cosine_uses_full_basket_norm_and_excludes_self(spark):
    rows = [row("1", "1", "A", "2"), row("2", "2", "A", "4"),
            row("3", "3", "B", "2"), row("4", "4", "A", "2"), row("4", "4", "B", "2")]
    clean, _ = clean_transactions(spark.createDataFrame(rows, HEADERS))
    scores = {r.CustomerID: r.CosineSimilarity for r in similar_customers(clean, "1").collect()}
    assert "1" not in scores
    assert scores["2"] == pytest.approx(1)
    assert scores["3"] == pytest.approx(0)
    assert scores["4"] == pytest.approx(2 ** -0.5)
    with pytest.raises(ValueError, match="no valid purchases"):
        similar_customers(clean, "unknown")
    clean.unpersist()
