import json
from datetime import datetime

def generate_report(ledger, findings, dq_score, dq_details):
    # Calculate recoverable value (duplicate + ghost fees + payout mismatches)
    recoverable = 0.0
    for f in findings:
        if f["type"] == "duplicate_order" and f.get("estimated_value"):
            recoverable += f["estimated_value"]
        elif f["type"] == "ghost_fees" and f.get("estimated_loss"):
            recoverable += f["estimated_loss"]
        elif f["type"] == "payout_mismatch" and f.get("variance"):
            recoverable += f["variance"]
    recoverable = round(recoverable, 2)

    # Leakage risk score: combination of data quality inverse + findings severity
    high_conf = len([f for f in findings if f["confidence"] == "High"])
    medium_conf = len([f for f in findings if f["confidence"] == "Medium"])
    risk_score = min(100, max(0, int(100 - dq_score + high_conf*10 + medium_conf*5)))

    report = {
        "data_quality_score": dq_score,
        "leakage_risk_score": risk_score,
        "estimated_recoverable_value": recoverable,
        "findings": findings,
        "data_quality_details": dq_details,
        "generated_at": datetime.utcnow().isoformat()
    }
    return report
