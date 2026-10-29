"""Verify every populated dashboard page against a completed UCI run."""
import json
from pathlib import Path

from streamlit.testing.v1 import AppTest

root = Path(__file__).resolve().parents[1]
manifest = root / "outputs/uci/advanced/advanced_summary.json"
state = json.loads(manifest.read_text(encoding="utf-8"))
assert state["complete"], "Complete the advanced pipeline before running this check."
app = AppTest.from_file(str(root / "app.py")).run(timeout=60)
assert not app.exception, list(app.exception)
pages = list(app.sidebar.selectbox[0].options)
for page in pages:
    app.sidebar.selectbox[0].select(page).run(timeout=60)
    assert not app.exception, (page, list(app.exception))
    print(f"PASS: {page}", flush=True)
    if page == "Basket analysis":
        search = next(widget for widget in app.text_input if widget.label == "Find a product in a rule")
        search.set_value("NONEXISTENT_PRODUCT_123").run(timeout=60)
        assert not app.exception, list(app.exception)
    elif page == "Inactivity prediction":
        app.slider[0].set_value(1.0).run(timeout=60)
        assert not app.exception, list(app.exception)
app.sidebar.selectbox[0].select("Core dashboard").run(timeout=60)
app.sidebar.checkbox[0].uncheck().run(timeout=60)
assert not app.exception, list(app.exception)
assert app.sidebar.metric[0].value == "0.501", "Original K=4 baseline must remain available."
print("PASS: original baseline switch", flush=True)
