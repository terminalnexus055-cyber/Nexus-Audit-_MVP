from fastapi import FastAPI, UploadFile, File, Form
import pandas as pd
import tempfile
import os
from schema_mapper import detect_and_map_columns
from normalizer import normalize_all
from data_quality import compute_quality_score
from forensic_engine import run_forensic_rules
from report_generator import generate_report
from grok_narrator import maybe_narrate

app = FastAPI(title="NexusAudit MVP")

@app.post("/audit")
async def audit(
    shopify_orders: UploadFile = File(None),
    shopify_payouts: UploadFile = File(None),
    tiktok_orders: UploadFile = File(None),
    tiktok_settlements: UploadFile = File(None),
    stripe_payouts: UploadFile = File(None),
    grok_api_key: str = Form(None)
):
    file_map = {
        "shopify_orders": shopify_orders,
        "shopify_payouts": shopify_payouts,
        "tiktok_orders": tiktok_orders,
        "tiktok_settlements": tiktok_settlements,
        "stripe_payouts": stripe_payouts,
    }

    raw_dfs = {}
    for source_name, upload_file in file_map.items():
        if upload_file and upload_file.filename:
            content = await upload_file.read()
            with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
                tmp.write(content)
                tmp_path = tmp.name
            df = pd.read_csv(tmp_path, dtype=str, encoding_errors='ignore').fillna('')
            os.unlink(tmp_path)
            raw_dfs[source_name] = df
        else:
            raw_dfs[source_name] = None

    mappings = {}
    for source, df in raw_dfs.items():
        if df is not None and not df.empty:
            mappings[source] = detect_and_map_columns(df)

    ledger = normalize_all(raw_dfs, mappings)
    dq_score, dq_details = compute_quality_score(ledger, mappings)
    findings = run_forensic_rules(ledger)
    report = generate_report(ledger, findings, dq_score, dq_details)

    if grok_api_key:
        report = maybe_narrate(report, grok_api_key)

    return report