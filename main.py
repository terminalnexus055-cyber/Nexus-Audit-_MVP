from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
import shutil, os, json, tempfile
from datetime import datetime
from schema_mapper import detect_and_map_columns
from normalizer import normalize_all
from data_quality import compute_quality_score
from forensic_engine import run_forensic_rules
from report_generator import generate_report
from grok_narrator import maybe_narrate

app = FastAPI(title="NexusAudit MVP")

# Mount frontend
app.mount("/static", StaticFiles(directory="../frontend"), name="static")

@app.get("/")
async def root():
    return HTMLResponse(open("../frontend/index.html").read())

@app.post("/upload")
async def upload_files(shopify_orders: UploadFile = File(None),
                       shopify_payouts: UploadFile = File(None),
                       tiktok_orders: UploadFile = File(None),
                       tiktok_settlements: UploadFile = File(None),
                       stripe_payouts: UploadFile = File(None)):
    """
    Receive CSV files, save to temp directory, return file paths.
    """
    temp_dir = tempfile.mkdtemp()
    saved = {}
    for name, file in [("shopify_orders", shopify_orders),
                       ("shopify_payouts", shopify_payouts),
                       ("tiktok_orders", tiktok_orders),
                       ("tiktok_settlements", tiktok_settlements),
                       ("stripe_payouts", stripe_payouts)]:
        if file and file.filename:
            path = os.path.join(temp_dir, f"{name}.csv")
            with open(path, "wb") as buffer:
                shutil.copyfileobj(file.file, buffer)
            saved[name] = path
    # Store temp_dir in session or return it; for MVP just pass mapping
    return {"temp_dir": temp_dir, "files": saved}

@app.post("/run-audit")
async def run_audit(temp_dir: str = Form(...), grok_api_key: str = Form(None)):
    """
    Execute the full audit pipeline on uploaded CSVs.
    Returns JSON report.
    """
    # Load raw dataframes
    raw_dfs = {}
    for name in ["shopify_orders", "shopify_payouts", "tiktok_orders", "tiktok_settlements", "stripe_payouts"]:
        path = os.path.join(temp_dir, f"{name}.csv")
        if os.path.exists(path):
            raw_dfs[name] = pd.read_csv(path, dtype=str, encoding_errors='ignore').fillna('')
        else:
            raw_dfs[name] = None

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

    # Clean temp files (optional)
    shutil.rmtree(temp_dir, ignore_errors=True)

    return report
