def compute_quality_score(ledger_df, mappings):
    """
    Returns (score, details dict) for data quality.
    Penalties for missing required columns, duplicates, malformed dates, null amounts, mixed currencies, partial months.
    """
    score = 100
    details = {"penalties": [], "warnings": []}

    # Required columns (minimum needed)
    required = ["order_id", "gross_amount", "date"]
    for col in required:
        if col not in ledger_df.columns or ledger_df[col].isna().all():
            score -= 20
            details["penalties"].append(f"Missing required column: {col}")

    # Duplicate rows (already deduped in normalization, but detect high duplication rate)
    dup_rate = ledger_df.duplicated().mean()
    if dup_rate > 0.1:
        score -= 15
        details["penalties"].append(f"High duplicate rate ({dup_rate:.1%})")

    # Malformed dates
    if "date" in ledger_df.columns:
        malformed = ledger_df["date"].isna().mean()
        if malformed > 0.3:
            score -= 15
            details["penalties"].append(f"Many malformed dates ({malformed:.1%})")

    # Null amounts in gross
    null_gross = ledger_df["gross_amount"].isna().mean()
    if null_gross > 0.2:
        score -= 10
        details["penalties"].append(f"High missing gross amounts ({null_gross:.1%})")

    # Mixed currencies
    currencies = ledger_df["currency"].dropna().unique()
    if len(currencies) > 1:
        score -= 10
        details["penalties"].append(f"Multiple currencies found: {list(currencies)}")

    # Partial month? Check min and max date
    if "date" in ledger_df.columns and not ledger_df["date"].isna().all():
        min_date = ledger_df["date"].min()
        max_date = ledger_df["date"].max()
        span = (max_date - min_date).days
        if span < 28:
            score -= 10
            details["warnings"].append(f"Data covers only {span} days (possible partial month)")

    return max(0, score), details