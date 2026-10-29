import csv
from datetime import datetime

from openpyxl import Workbook

from retailiq.data import HEADERS, convert_workbook


def test_workbook_conversion_preserves_identifiers_and_dates(tmp_path):
    workbook = Workbook()
    workbook.active.append(HEADERS)
    workbook.active.append(["C123", "001A", 'Gift, "blue"', -2,
                            datetime(2011, 12, 9, 10, 30), 2.5, None, "UK"])
    source = tmp_path / "retail.xlsx"
    workbook.save(source)
    workbook.close()
    target = tmp_path / "retail.csv"
    assert convert_workbook(source, target) == 1
    with target.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["StockCode"] == "001A"
    assert rows[0]["Description"] == 'Gift, "blue"'
    assert rows[0]["InvoiceDate"] == "2011-12-09 10:30:00"
    assert rows[0]["CustomerID"] == ""
