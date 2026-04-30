import pandas as pd
import numpy as np
from datetime import timedelta

def run_forensic_rules(ledger):
    findings = []
    findings.extend(detect_duplicate_orders(ledger))
    findings.extend(detect_payout_mismatch(ledger))
    findings.extend(detect_ghost_fees(ledger))
    findings.extend(detect_refund_timing(ledger))
    findings.extend(detect_revenue_drift(ledger))
    findings.extend(detect_margin_erosion(ledger))
    return findings

def detect_duplicate_orders(df):
    findings = []
    # Only consider rows with customer_hash and amounts
    candidates = df.dropna(subset=["customer_hash", "gross_amount", "order_id"])
    if candidates.empty:
        return []

    # Simple cross-join on similar amounts (±1%) and same hash
    pairs = []
    for i, row1 in candidates.iterrows():
        for j, row2 in candidates.iterrows():
            if i >= j:
                continue
            if row1["customer_hash"] != row2["customer_hash"]:
                continue
            if abs(row1["gross_amount"] - row2["gross_amount"]) > 0.01 * max(row1["gross_amount"], row2["gross_amount"]):
                continue
            # Score signals
            score = 35  # same customer hash
            if row1["gross_amount"] == row2["gross_amount"]:
                score += 25
            # SKU overlap
            sku1 = set(str(row1.get("sku", "")).split(';'))
            sku2 = set(str(row2.get("sku", "")).split(';'))
            if sku1 and sku2 and sku1.intersection(sku2):
                score += 20
            # Time within 2h
            if pd.notna(row1["date"]) and pd.notna(row2["date"]):
                diff = abs((row1["date"] - row2["date"]).total_seconds())
                if diff <= 7200:
                    score += 15
            # Same platform? implicit
            score += 5  # same source file (could add shipping country if available)

            if score >= 85:
                confidence = "High"
            elif score >= 70:
                confidence = "Medium"
            else:
                confidence = "Needs Review"
            if score >= 70:
                findings.append({
                    "type": "duplicate_order",
                    "confidence": confidence,
                    "estimated_value": round(row1["gross_amount"], 2),
                    "summary": f"Duplicate order pair: {row1['order_id']} & {row2['order_id']}",
                    "duplicate_pair": [row1["order_id"], row2["order_id"]],
                    "score": score
                })
                break  # avoid multiple entries for same pair
    return findings[:20]  # limit

def detect_payout_mismatch(df):
    # Compare total expected net vs actual net per payout batch
    findings = []
    if "payout_id" not in df.columns:
        return []
    batches = df.groupby("payout_id")
    for batch, group in batches:
        gross = group["gross_amount"].sum()
        fees = group["fee_amount"].sum()
        refunds = group["refund_amount"].sum()
        expected = gross - fees - refunds
        actual = group["net_amount"].sum()
        variance = abs(expected - actual)
        threshold = max(0.01 * expected, 25)  # 1% or $25
        if variance > threshold and expected != 0:
            findings.append({
                "type": "payout_mismatch",
                "confidence": "High" if variance > 50 else "Medium",
                "batch": batch,
                "expected_net": round(expected, 2),
                "actual_net": round(actual, 2),
                "variance": round(variance, 2),
                "summary": f"Payout {batch} mismatch: expected {expected}, actual {actual}"
            })
    return findings

def detect_ghost_fees(df):
    # Fees without linked order (order_id missing or NaN)
    findings = []
    ghost = df[df["fee_amount"] > 0 & (df["order_id"].isna() | (df["order_id"] == ''))]
    if not ghost.empty:
        total = ghost["fee_amount"].sum()
        findings.append({
            "type": "ghost_fees",
            "confidence": "Medium",
            "fee_count": len(ghost),
            "estimated_loss": round(total, 2),
            "summary": f"Found {len(ghost)} fee entries with no linked order, total {total}"
        })
    # Repeated same unusual fee
    fee_counts = df[df["fee_amount"] > 0].groupby("fee_amount").size()
    unusual = fee_counts[fee_counts > 5]
    for fee_val, count in unusual.items():
        # Flag if fee is round number and not standard processing fee
        if fee_val % 10 == 0:
            findings.append({
                "type": "ghost_fees",
                "confidence": "Needs Review",
                "fee_amount": fee_val,
                "occurrences": count,
                "summary": f"Recurring fee {fee_val} appears {count} times"
            })
    return findings

def detect_refund_timing(df):
    # Refund date far from order date and affects later payout
    findings = []
    refunds = df[df["refund_amount"] > 0].dropna(subset=["date", "order_id"])
    if refunds.empty:
        return []
    for _, row in refunds.iterrows():
        # Find original order date (same order_id but no refund)
        orig = df[(df["order_id"] == row["order_id"]) & (df["refund_amount"] == 0) & (df["date"] < row["date"])]
        if not orig.empty:
            delay = (row["date"] - orig["date"].max()).days
            if delay > 30:  # more than a month later
                findings.append({
                    "type": "refund_timing_distortion",
                    "confidence": "Medium",
                    "order_id": row["order_id"],
                    "delay_days": delay,
                    "refund_amount": row["refund_amount"],
                    "summary": f"Refund after {delay} days may distort later payout"
                })
    return findings

def detect_revenue_drift(df):
    # Compare sum of gross from orders vs sum of settled net plus fees/refunds
    orders_net = df[df["source_file"].isin(["shopify_orders", "tiktok_orders"])]
    settlements = df[df["source_file"].isin(["shopify_payouts", "tiktok_settlements", "stripe_payouts"])]
    total_gross = orders_net["gross_amount"].sum()
    total_net = settlements["net_amount"].sum()
    total_fees = settlements["fee_amount"].sum() + orders_net["fee_amount"].sum()
    total_refunds = settlements["refund_amount"].sum() + orders_net["refund_amount"].sum()
    expected_net = total_gross - total_fees - total_refunds
    drift = total_gross - (total_net + total_fees + total_refunds)
    threshold = max(0.01 * total_gross, 25)
    if abs(drift) > threshold and total_gross > 0:
        findings.append({
            "type": "attribution_drift",
            "confidence": "Medium",
            "drift_amount": round(drift, 2),
            "gross_reported": round(total_gross, 2),
            "actual_settled_net": round(total_net, 2),
            "summary": f"Revenue drift detected: reported gross exceeds realized settlements by {round(drift,2)}. Possible reporting inflation."
        })
    return findings

def detect_margin_erosion(df):
    # Trend warning if net/gross ratio declines over time
    df_monthly = df[df["gross_amount"] > 0].set_index("date").resample("M").agg(
        total_gross=("gross_amount", "sum"),
        total_net=("net_amount", "sum")
    ).dropna()
    if len(df_monthly) < 2:
        return []
    df_monthly["margin"] = df_monthly["total_net"] / df_monthly["total_gross"]
    trend = df_monthly["margin"].iloc[-1] - df_monthly["margin"].iloc[0]
    if trend < -0.02:  # 2% erosion
        findings.append({
            "type": "margin_erosion",
            "confidence": "Medium",
            "start_margin": round(df_monthly["margin"].iloc[0], 3),
            "end_margin": round(df_monthly["margin"].iloc[-1], 3),
            "summary": "Net retention rate declining while gross rising — possible fee increases or pricing issues."
        })
    return findings