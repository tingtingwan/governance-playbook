import os
from pathlib import Path
from .config import get_workspace_client, UC_CATALOG, UC_SCHEMA
from .data import query_uc

SAMPLE_DOCS_DIR = Path(__file__).parent.parent / "sample_docs"
VOLUME_PATH = f"/Volumes/{UC_CATALOG}/{UC_SCHEMA}/sample_docs"


def list_sample_docs() -> list[dict]:
    docs = []
    for f in sorted(SAMPLE_DOCS_DIR.glob("*.pdf")):
        doc_type = "Unknown"
        if "kiid" in f.name.lower():
            doc_type = "KIID"
        elif "factsheet" in f.name.lower():
            doc_type = "Fund Factsheet"
        elif "trade" in f.name.lower():
            doc_type = "Trade Confirmation"
        docs.append({"filename": f.name, "doc_type": doc_type, "size": f.stat().st_size})
    return docs


def upload_to_volume(filename: str) -> str:
    local_path = SAMPLE_DOCS_DIR / filename
    if not local_path.exists():
        raise FileNotFoundError(f"Sample doc not found: {filename}")

    w = get_workspace_client()
    volume_file_path = f"{VOLUME_PATH}/{filename}"
    with open(local_path, "rb") as f:
        w.files.upload(volume_file_path, f, overwrite=True)
    return volume_file_path


def parse_document(filename: str) -> dict:
    """Parse a document using ai_parse_document SQL function."""
    # Ensure volume exists and file is uploaded
    try:
        _ensure_volume()
        volume_path = upload_to_volume(filename)
    except Exception as e:
        # Fallback: read the PDF text directly (for demo purposes)
        return _fallback_parse(filename, str(e))

    try:
        result = query_uc(
            f"SELECT ai_parse_document('{volume_path}', array('text')) AS parsed"
        )
        if result and result[0].get("parsed"):
            return {"source": "ai_parse_document", "text": result[0]["parsed"], "filename": filename}
    except Exception as e:
        return _fallback_parse(filename, str(e))

    return _fallback_parse(filename, "No result from ai_parse_document")


def _ensure_volume():
    """Create the UC Volume if it doesn't exist."""
    try:
        query_uc(f"CREATE VOLUME IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}.sample_docs")
    except Exception:
        pass


def _fallback_parse(filename: str, error: str) -> dict:
    """Fallback: return hardcoded text for sample docs (demo reliability)."""
    texts = {
        "kiid_global_equity.pdf": """Key Investor Information Document (KIID)

Global Equity Growth Fund
A sub-fund of European Investment SICAV
ISIN: LU0292096186
Management Company: FundCo Asset Management S.A.
Share Class: A (Accumulating) EUR

Objectives and Investment Policy
The fund aims to achieve long-term capital growth by investing primarily in global equities. The fund may invest up to 20% in emerging markets. Minimum recommended holding period: 5 years.

Risk and Reward Profile
Risk Indicator: 5 out of 7 (Medium-High Risk)
Historical returns do not guarantee future performance.

Charges
Entry charge: 5.00% (maximum)
Exit charge: 0.00%
Ongoing charges: 1.45% per annum
Performance fee: None

Past Performance
2023: +14.2%  2022: -8.7%  2021: +22.1%  2020: +6.3%

Practical Information
Depositary: Northern Trust Luxembourg S.A.
Regulator: CSSF Luxembourg
Country of Registration: Luxembourg
Date of Publication: 15 January 2024""",

        "factsheet_euro_bond.pdf": """Fund Factsheet - Euro Corporate Bond Fund
As at 29 February 2024

Fund Details
ISIN: IE00BK5BQT80
Fund Manager: EuroBond Capital Management Ltd
Fund Size: EUR 2.4 billion
Base Currency: EUR
Launch Date: 15 March 2018
Benchmark: Bloomberg Euro Aggregate Corporate Index
Risk Rating: 3 out of 7
Ongoing Charges (OCF): 0.85%
Distribution Frequency: Semi-annual
SFDR Classification: Article 8

Investment Objective
The fund aims to provide income and capital appreciation through investment in Euro-denominated investment grade corporate bonds. Maximum portfolio duration: 8 years.

Performance
Fund YTD: +1.2%  1Y: +4.8%  3Y: -2.1%  5Y: +1.5%
Benchmark YTD: +1.0%  1Y: +4.5%  3Y: -2.8%  5Y: +1.1%

Top Holdings
1. Deutsche Bank AG 2.75% 2028 - 3.2%
2. BNP Paribas SA 3.125% 2029 - 2.8%
3. Volkswagen AG 2.625% 2027 - 2.5%""",

        "trade_confirmation.pdf": """TRADE CONFIRMATION - CONFIDENTIAL

Transaction Details
Trade Reference: TC-2024-FX-00847291
Trade Date: 05 March 2024
Settlement Date: 07 March 2024 (T+2)
Transaction Type: FX Forward
Direction: BUY

Client Information
Client Name: David Park
Client ID: CLI-US-20198
Account Number: US-TRD-006789
Client Email: d.park@us-advisory.com
Client Phone: +1-212-555-0199
Jurisdiction: United States

Instrument Details
Instrument: EUR/USD FX Forward 3M
Notional Amount: EUR 500,000.00
Forward Rate: 1.0842
USD Equivalent: USD 542,100.00
Maturity Date: 05 June 2024

Fees and Charges
Commission: EUR 250.00
Spread Cost: EUR 125.00
Total Charges: EUR 375.00

Compliance
MiFID II Classification: Professional Client
Best Execution: Confirmed - venue: EBS
LEI: 529900HNOAA1KXQJUQ27
Compliance Status: Approved"""
    }

    text = texts.get(filename, f"[Could not parse {filename}]")
    return {"source": "fallback", "text": text, "filename": filename, "note": f"ai_parse_document unavailable: {error}"}
