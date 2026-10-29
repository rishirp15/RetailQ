"""Generate clearly labelled synthetic transactions for a quick offline demonstration."""
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

from retailiq.data import HEADERS


def generate(path: Path = Path("data/raw/sample.csv")):
    rng = random.Random(42)
    path.parent.mkdir(parents=True, exist_ok=True)
    invoice = 100000
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(HEADERS)
        for customer in range(10001, 10121):
            group = (customer - 10001) // 30
            orders = [rng.randint(15, 25), rng.randint(6, 12), rng.randint(1, 3), rng.randint(2, 5)][group]
            for _ in range(orders):
                invoice += 1
                age = rng.randint(*[(1, 60), (10, 120), (1, 90), (180, 360)][group])
                date = datetime(2011, 12, 10) - timedelta(days=age)
                for product in rng.sample(range(1, 21), rng.randint(1, 5)):
                    writer.writerow([invoice, f"P{product:03}", f"Gift product {product}",
                                     rng.randint(1, [30, 10, 3, 5][group]),
                                     date.strftime("%Y-%m-%d %H:%M:%S"),
                                     round(rng.uniform(1, 20), 2), customer,
                                     rng.choice(["United Kingdom", "France", "Germany"])])
        # Deliberate dirty rows to demonstrate the cleaning audit.
        writer.writerow(["C999", "P001", "Cancelled", -2, "2011-12-01 10:00:00", 5, 10001, "United Kingdom"])
        writer.writerow(["999", "P001", "Missing customer", 2, "2011-12-01 10:00:00", 5, "", "United Kingdom"])
    print(f"Created SYNTHETIC demo data: {path}")


if __name__ == "__main__":
    generate()
