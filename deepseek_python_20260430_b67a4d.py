import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Generate messy Shopify Orders CSV
def messy_shopify_orders(n=200):
    np.random.seed(42)
    dates = pd.date_range(end=datetime(2024,3,31), periods=n, freq='3h')
    df = pd.DataFrame({
        "Order ID": [f"SHOP-{i}" for i in range(n)],
        "Created at": dates.strftime('%m/%d/%Y %H:%M'),
        "Customer Email": [f"user{random.randint(1,50)}@mail.com" for _ in range(n)],
        "Total": np.round(np.random.uniform(20,500, n), 2),
        "Fees": 0,
        "Refund": np.where(np.random.rand(n)>0.9, np.round(np.random.uniform(20,100,n),2), 0),
        "Net": 0,  # will be derived
        "Currency": "USD",
        "Status": np.random.choice(["paid","refunded","partial"], n),
        "SKU": [', '.join(random.sample(['SKU101','SKU202','SKU303','SKU404'], k=random.randint(1,3))) for _ in range(n)],
        "Quantity": np.random.randint(1,4,n)
    })
    # Insert blanks, duplicates, malformed dates
    df.loc[5:8, "Created at"] = "bad_date_format"
    df = pd.concat([df, df.iloc[10:15]])  # duplicates
    df.to_csv("shopify_orders.csv", index=False)

# Similar functions for other CSVs (shopify_payouts, tiktok_orders, tiktok_settlements, stripe_payouts)
# Include realistic discrepancies for benchmark
def messy_shopify_payouts(n=50):
    # …
    pass
# (full generator code omitted for brevity; implement realistic messy CSVs following same pattern)

if __name__ == "__main__":
    messy_shopify_orders()
    # generate others