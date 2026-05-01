from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import os
import tempfile
import shutil
import pandas as pd
from schema_mapper import detect_and_map_columns
from normalizer import normalize_all
from data_quality import compute_quality_score
from forensic_engine import run_forensic_rules
from report_generator import generate_report
from grok_narrator import maybe_narrate

app = FastAPI(title="NexusAudit MVP")

# Serve the frontend static files (HTML, CSS, JS)
app.mount("/static", StaticFiles(directory="frontend"), name="static")

@app.get("/")
async def root():
    with open("frontend/index.html", "r") as f:
        return HTMLResponse(f.read())

@app.post("/audit")
async def audit(
    shopify_orders: UploadFile = File(None),
    shopify_payouts: UploadFile = File(None),
    tiktok_orders: UploadFile = File(None),
    tiktok_settlements: UploadFile = File(None),
    stripe_payouts: UploadFile = File(None),
    grok_api_key: str = Form(None)
):
    """
    Single endpoint: receives all CSV files, runs the full audit pipeline,
    returns the JSON report.
    """
    # Map incoming files to expected names
    file_map = {
        "shopify_orders": shopify_orders,
        "shopify_payouts": shopify_payouts,
        "tiktok_orders": tiktok_orders,
        "tiktok_settlements": tiktok_settlements,
        "stripe_payouts": stripe_payouts,
    }

    # Read uploaded files into pandas DataFrames
    raw_dfs = {}
    for source_name, upload_file in file_map.items():
        if upload_file and upload_file.filename:
            content = await upload_file.read()
            # Write to a temp file because pandas needs a path
            with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
                tmp.write(content)
                tmp_path = tmp.name
            df = pd.read_csv(tmp_path, dtype=str, encoding_errors='ignore').fillna('')
            os.unlink(tmp_path)  # clean up
            raw_dfs[source_name] = df
        else:
            raw_dfs[source_name] = None

    # STEP 1: Column detection & mapping
    mappings = {}
    for source, df in raw_dfs.items():
        if df is not None and not df.empty:
            mappings[source] = detect_and_map_columns(df)

    # STEP 2: Normalization to canonical ledger
    ledger = normalize_all(raw_dfs, mappings)

    # STEP 3: Data quality score
    dq_score, dq_details = compute_quality_score(ledger, mappings)

    # STEP 4: Forensic rules
    findings = run_forensic_rules(ledger)

    # STEP 5 & 6: Assemble report
    report = generate_report(ledger, findings, dq_score, dq_details)

    # STEP 7: Optional AI narration
    if grok_api_key:
        report = maybe_narrate(report, grok_api_key)

    return report
