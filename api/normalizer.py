import pandas as pd
import numpy as np
from datetime import datetime, timezone
import hashlib, re
from dateutil import parser as dateparser

CANONICAL_COLS = ["date", "platform", "source_file", "order_id",
                  "customer_hash", "gross_amount", "fee_amount",
                  "refund_amount", "net_amount", "currency",
                  "status", "sku", "quantity", "payout_id"]

def clean_currency(val):
    if pd.isna(val) or val == '':
        return None
    if isinstance(val, (int, float)):
        return float(val)
    # Remove currency symbols and commas
    cleaned = re.sub(r'[^\d\.\-]', '', str(val).replace(',', ''))
    try:
        return float(cleaned)
    except:
        return None

def parse_date(val):
    try:
        return dateparser.parse(str(val)).replace(tzinfo=timezone.utc)
    except:
        return None

def hash_email(email):
    if not email or pd.isna(email):
        return None
    return hashlib.sha256(email.encode()).hexdigest()[:12]

def normalize_all(raw_dfs, mappings):
    """Transform raw_dfs into a single canonical DataFrame."""
    rows = []
    source_platform = {
        "shopify_orders": "Shopify",
        "shopify_payouts": "Shopify",
        "tiktok_orders": "TikTok Shop",
        "tiktok_settlements": "TikTok Shop",
        "stripe_payouts": "Stripe"
    }

    for source, df in raw_dfs.items():
        if df is None or df.empty:
            continue
        m = mappings.get(source, {})
        platform = source_platform.get(source, "Unknown")
        for _, row in df.iterrows():
            # Extract canonical values
            r = {}
            r["source_file"] = source
            r["platform"] = platform
            r["date"] = parse_date(row.get(m.get("date"))) if "date" in m else None
            r["order_id"] = row.get(m.get("order_id")) if "order_id" in m else None
            r["customer_hash"] = hash_email(row.get(m.get("customer_email"))) if "customer_email" in m else None
            r["gross_amount"] = clean_currency(row.get(m.get("gross_amount"))) if "gross_amount" in m else None
            r["fee_amount"] = clean_currency(row.get(m.get("fee_amount"))) if "fee_amount" in m else None
            r["refund_amount"] = clean_currency(row.get(m.get("refund_amount"))) if "refund_amount" in m else None
            r["net_amount"] = clean_currency(row.get(m.get("net_amount"))) if "net_amount" in m else None
            r["currency"] = row.get(m.get("currency")) if "currency" in m else None
            r["status"] = row.get(m.get("status")) if "status" in m else None
            r["sku"] = row.get(m.get("sku")) if "sku" in m else None
            r["quantity"] = clean_currency(row.get(m.get("quantity"))) if "quantity" in m else None
            r["payout_id"] = row.get(m.get("payout_id")) if "payout_id" in m else None
            rows.append(r)

    df = pd.DataFrame(rows, columns=CANONICAL_COLS)

    # Remove empty rows (no order_id and no amounts)
    df = df.dropna(subset=["order_id"], how="all")
    # Drop fully duplicate rows
    df = df.drop_duplicates()
    # If net_amount missing but gross/fee/refund exist, compute expected net
    df["expected_net"] = df["gross_amount"].fillna(0) - df["fee_amount"].fillna(0) - df["refund_amount"].fillna(0)
    mask_net_missing = df["net_amount"].isna()
    df.loc[mask_net_missing, "net_amount"] = df.loc[mask_net_missing, "expected_net"]
    return df
