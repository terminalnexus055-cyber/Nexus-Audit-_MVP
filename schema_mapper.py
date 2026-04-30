import pandas as pd
from fuzzywuzzy import process, fuzz

CANONICAL_COLS = [
    "date", "platform", "order_id", "external_order_id",
    "customer_email", "customer_name", "gross_amount", "fee_amount",
    "refund_amount", "net_amount", "currency", "status",
    "sku", "quantity", "settlement_batch", "payout_id"
]

# Aliases / fuzzy targets
ALIASES = {
    "date": ["date", "created at", "order date", "timestamp", "time"],
    "order_id": ["order id", "order number", "transaction id", "id", "number"],
    "gross_amount": ["total", "total sales", "gross", "amount", "subtotal price"],
    "fee_amount": ["fees", "charges", "fee", "processing fee"],
    "refund_amount": ["refund", "refunded", "return amount"],
    "net_amount": ["net", "payout", "settlement amount", "net sales"],
    "customer_email": ["email", "customer email", "buyer email"],
    "customer_name": ["customer", "name", "customer name", "buyer"],
    "currency": ["currency", "currency code"],
    "status": ["status", "order status", "financial status"],
    "sku": ["sku", "lineitem sku", "variant sku", "product sku"],
    "quantity": ["quantity", "qty", "items"],
    "payout_id": ["payout id", "settlement id", "batch id", "payout reference"],
    "settlement_batch": ["settlement batch", "batch", "payout batch"],
    "platform": ["platform", "source"],  # we'll hardcode per file
}

def fuzzy_match_column(col_name, targets, threshold=75):
    """Return best match if score >= threshold, else None."""
    best = process.extractOne(col_name, targets, scorer=fuzz.token_sort_ratio)
    if best and best[1] >= threshold:
        return best[0]
    return None

def detect_and_map_columns(df):
    """Return a dict mapping canonical column -> actual column in df."""
    mapping = {}
    # Build a flat list of targets with canonical names
    target_to_canonical = {}
    for canon, aliases in ALIASES.items():
        for alias in aliases:
            target_to_canonical[alias.lower()] = canon

    for col in df.columns:
        clean = col.strip().lower()
        found = fuzzy_match_column(clean, list(target_to_canonical.keys()))
        if found:
            canon = target_to_canonical[found]
            if canon not in mapping:  # first come
                mapping[canon] = col
    # Hardcode platform based on file source (will be added during normalization)
    return mapping
