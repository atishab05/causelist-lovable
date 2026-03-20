"""
Chhattisgarh High Court – Causelist Tracker + WhatsApp Notifier

Downloads causelist PDFs directly using known URL patterns — no browser needed.

File naming (confirmed from live URLs):
  Daily List        : CG<DDMMYYYY>.pdf          e.g. CG19032026.pdf
  Supplementary List: CG<DDMMYYYY>-SUP1.pdf     e.g. CG18032026-SUP1.pdf
  Weekly List       : CG<DDMMYYYY>-WKL.pdf      e.g. CG16032026-WKL.pdf

Setup:
  pip install pdfplumber requests twilio pytz
"""

from twilio.rest import Client
import pdfplumber
import requests
import time
import sys
import re
import json
import os
from datetime import date, timedelta, datetime
import pytz

# ─── CONFIG ───────────────────────────────────────────────────────────────────
PDF_PATH           = "causelist.pdf"
RESULTS_JSON_PATH  = "results.json"   # written after every run; committed by CI

import os as _cred_os
TWILIO_ACCOUNT_SID = _cred_os.environ.get("TWILIO_ACCOUNT_SID", "")
TWILIO_AUTH_TOKEN  = _cred_os.environ.get("TWILIO_AUTH_TOKEN",  "")
TWILIO_FROM        = "whatsapp:+14155238886"
WHATSAPP_TO   = "whatsapp:" + _cred_os.environ.get("WHATSAPP_TO", "")
LAWYER_NAME   = _cred_os.environ.get("LAWYER_NAME", "")

MAX_RETRIES  = 3
RETRY_DELAY  = 15
LIST_TYPES   = ["DAILY LIST", "SUPPLEMENTARY LIST", "WEEKLY LIST"]

import os as _os
_override = _os.environ.get("CL_LIST_TYPE", "").strip()
if _override and _override in LIST_TYPES:
    LIST_TYPES = [_override]

SEARCH_ONLY      = False
DEBUG_PARSE      = False
RAW_DEBUG        = False
DEBUG_PAGES      = [27, 137, 153]
WHATSAPP_ENABLED = _cred_os.environ.get("WHATSAPP_ENABLED", "false").lower() == "true"
LIST_PREFIX = {
    "DAILY LIST":         "Daily",
    "SUPPLEMENTARY LIST": "Supplementary",
    "WEEKLY LIST":        "Weekly",
}
# ─────────────────────────────────────────────────────────────────────────────


IST = pytz.timezone("Asia/Kolkata")

def now_ist():
    return datetime.now(IST)

def pdf_filename_and_url(list_type, upload_date):
    lt = list_type.upper()
    if "SUPPLEMENT" in lt:
        file_date = upload_date + timedelta(days=1)
        suffix    = "-SUP1"
    elif "WEEK" in lt:
        current_monday = upload_date - timedelta(days=upload_date.weekday())
        file_date = current_monday + timedelta(days=7)
        suffix    = "-WKL"
    else:
        file_date = upload_date + timedelta(days=2)
        suffix    = ""

    filename = f"CG{file_date.strftime('%d%m%Y')}{suffix}.pdf"
    url      = f"https://highcourt.cg.gov.in/clists/causelists/pdf/{filename}"
    return filename, url, file_date

def run_once(list_type):
    today    = now_ist().date()
    headers  = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/122.0.0.0 Safari/537.36",
        "Referer"   : "https://highcourt.cg.gov.in/clists/courtlist.php",
        "Accept"    : "application/pdf,*/*",
    }

    for days_back in range(6):
        candidate = today - timedelta(days=days_back)
        if candidate.weekday() == 6:
            continue
        if "WEEK" in list_type.upper():
            days_to_wed = (2 - candidate.weekday()) % 7
            wed = candidate + timedelta(days=days_to_wed)
            if abs((candidate - wed).days) > 6:
                continue

        filename, url, file_date = pdf_filename_and_url(list_type, candidate)
        date_str = candidate.strftime("%d %b,%Y")
        print(f"  Trying {list_type}: {filename}  ({url})")

        try:
            resp = requests.get(url, headers=headers, timeout=30)
            if resp.status_code == 200 and resp.content[:5].startswith(b"%PDF"):
                with open(PDF_PATH, "wb") as f:
                    f.write(resp.content)
                print(f"  ✅ Downloaded {filename} ({len(resp.content)//1024} KB)")
                return True, list_type, date_str, url
            else:
                print(f"     HTTP {resp.status_code} — not available yet")
        except Exception as e:
            print(f"     Error: {e}")

    print(f"  ❌ Could not download {list_type} for any recent date.")
    return False, list_type, None, None


def parse_pdf(pdf_path, lawyer_name):
    name_upper = lawyer_name.upper()

    honble_re  = re.compile(r"HON['\u2018\u2019\u02bc]?BLE\b", re.IGNORECASE)
    court_re   = re.compile(r"COURT\s+NO\.", re.IGNORECASE)
    list_no_re = re.compile(r"^\s*LIST\s*[-\u2013]\s*\d+", re.IGNORECASE)
    entry_re   = re.compile(r"^\s*(\d+)\.\s")

    def should_skip(line):
        u = line.strip().upper()
        t = line.strip()
        if not t or t in ("-", "–", ".", ":", "[]"):
            return True
        if "BY ORDER OF" in u and "CHIEF JUSTICE" in u:
            return True
        if "THIS CAUSE LIST IS PUBLISHED" in u and "CHIEF JUSTICE" in u:
            return True
        if re.match(r'^SD/[-–]', u):
            return True
        if u == "PRINT":
            return True
        if re.match(r'^(MONDAY|TUESDAY|WEDNESDAY|THURSDAY|FRIDAY|SATURDAY|SUNDAY)'
                    r'\s+THE\s+\d+', u):
            return True
        if "VC LINK" in u or t.startswith("[VC"):
            return True
        if re.match(r'^\[?DISCLAIMERS', u):
            return True
        if re.match(r'^\[?\s*\(I+\)', u):
            return True
        if "ARCHIVAL DATA" in u and "OFFICIAL RECORD" in u:
            return True
        if "UNLESS OTHERWISE DIRECTED BY THE JUDGE" in u:
            return True
        if re.match(r'^INFORMATION AND DISCLAIMER', u):
            return True
        if re.match(r'^S\s*NO\.?\s+CASE\s+NO\.?\s+PARTY', u):
            return True
        if re.match(r'^(DAILY|WEEKLY|SUPPLEMENTARY)\s+CAUSE\s+LIST\s+FOR\b', u):
            return True
        if re.match(r'^(FRESH MATTERS|OLD MATTERS|PART HEARD MATTERS'
                    r'|ADMISSION MATTERS|REGULAR HEARING)\s*$', u):
            return True
        return False

    def clean_line(line):
        return re.sub(
            r'\s*\(?\s*Live\s+Stream\s*[-\u2013]?\s*(Yes|No)\s*\)?',
            '', line, flags=re.IGNORECASE
        ).strip()

    all_lines  = []
    sections   = []
    total_pages = 0

    pending_judges = []
    pending_court  = ""

    with pdfplumber.open(pdf_path) as pdf:
        total_pages = len(pdf.pages)
        print(f"  Total pages: {total_pages}")
        for page_num, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            for raw in text.splitlines():
                if should_skip(raw):
                    continue
                cleaned = clean_line(raw)
                if not cleaned:
                    continue
                s = cleaned.strip()
                idx = len(all_lines)
                all_lines.append({"page": page_num, "text": cleaned})

                if honble_re.search(s):
                    upper_s = s.upper()
                    if "TIED UP" not in upper_s and "EXCEPTION" not in upper_s:
                        pending_judges.append(s)
                elif court_re.search(s):
                    pending_court = s
                elif "CHIEF JUSTICE" in s.upper() and "COURT" in s.upper():
                    pending_court = s
                elif list_no_re.match(s):
                    if not pending_court and any(
                            "CHIEF JUSTICE" in j.upper() for j in pending_judges):
                        pending_court = "THE CHIEF JUSTICE'S COURT"
                    sections.append({
                        "line_idx": idx,
                        "judges"  : list(pending_judges) or ["Unknown"],
                        "court"   : pending_court or "Unknown",
                        "list_no" : s,
                    })
                    pending_judges = []
                    pending_court  = ""
                else:
                    if pending_judges and not pending_court:
                        if entry_re.match(cleaned):
                            pending_judges = []
                            pending_court  = ""

    def section_for_line(idx):
        active = {"judges": ["Unknown"], "court": "Unknown", "list_no": "Unknown"}
        for s in sections:
            if s["line_idx"] <= idx:
                active = s
            else:
                break
        return active

    results            = []
    current_block      = []
    current_block_start = 0
    carry_purpose = []

    def extract_purpose(block):
        purpose_lines = list(carry_purpose)
        in_p = len(carry_purpose) > 0
        for l in block:
            t = l["text"].strip()
            if not t:
                continue
            if t.startswith("*") or t.startswith("["):
                purpose_lines.append(t)
                in_p = True
            elif in_p:
                if entry_re.match(t) or honble_re.search(t) or court_re.search(t):
                    in_p = False
                else:
                    purpose_lines.append(t)
        return " ".join(purpose_lines)

    def update_carry(block):
        nonlocal carry_purpose
        carry_purpose = []
        in_p   = False
        pending = []
        for l in block:
            t = l["text"].strip()
            if not t:
                continue
            if t.startswith("*") or t.startswith("["):
                pending = [t]
                in_p = True
            elif in_p:
                if entry_re.match(t) or honble_re.search(t) or court_re.search(t):
                    in_p = False
                    pending = []
                else:
                    pending.append(t)
        if in_p and pending:
            carry_purpose = pending

    def flush(block, bstart):
        nonlocal carry_purpose
        if not block:
            return
        block_text = "\n".join(l["text"] for l in block)
        if name_upper in block_text.upper() or carry_purpose:
            if name_upper in block_text.upper():
                sec  = section_for_line(bstart)
                page = block[0]["page"]
                first_line = block[0]["text"].strip()
                em = entry_re.match(first_line)
                sno     = em.group(1) if em else ""
                tokens  = first_line.split()
                case_no = tokens[1] if len(tokens) > 1 else ""
                purpose = extract_purpose(block)
                carry_purpose = []
                results.append({
                    "page"    : page,
                    "judges"  : sec["judges"],
                    "court"   : sec["court"],
                    "list_no" : sec["list_no"],
                    "sno"     : sno,
                    "case_no" : case_no,
                    "purpose" : purpose,
                })
            else:
                carry_purpose = []
        else:
            update_carry(block)

    for idx, line_obj in enumerate(all_lines):
        if entry_re.match(line_obj["text"]):
            flush(current_block, current_block_start)
            current_block       = [line_obj]
            current_block_start = idx
        else:
            current_block.append(line_obj)

    flush(current_block, current_block_start)
    return results, total_pages


def upload_pdf(pdf_path):
    print("  Uploading PDF to tmpfiles.org …")
    try:
        with open(pdf_path, "rb") as f:
            resp = requests.post(
                "https://tmpfiles.org/api/v1/upload",
                files={"file": (pdf_path, f, "application/pdf")},
                timeout=60
            )
        data = resp.json()
        url = (data.get("data") or {}).get("url", "")
        if url:
            url = url.replace("tmpfiles.org/", "tmpfiles.org/dl/", 1)
            print(f"  ✅ Uploaded: {url}")
            return url
        print(f"  ⚠️  tmpfiles.org failed: {data}")
    except Exception as e:
        print(f"  ⚠️  tmpfiles.org error: {e}")

    print("  Trying fallback: 0x0.st …")
    try:
        with open(pdf_path, "rb") as f:
            resp = requests.post(
                "https://0x0.st",
                files={"file": (pdf_path, f, "application/pdf")},
                timeout=60
            )
        url = resp.text.strip()
        if url.startswith("http"):
            print(f"  ✅ Uploaded: {url}")
            return url
        print(f"  ⚠️  0x0.st failed: {resp.text[:100]}")
    except Exception as e:
        print(f"  ⚠️  0x0.st error: {e}")

    return None


# ─── RESULTS JSON ─────────────────────────────────────────────────────────────

def load_existing_results():
    """Load existing results.json if it exists (to preserve history)."""
    if os.path.exists(RESULTS_JSON_PATH):
        try:
            with open(RESULTS_JSON_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"runs": []}

def save_results_json(all_runs):
    """Write results.json keeping last 30 runs."""
    payload = {
        "lawyer_name": LAWYER_NAME,
        "updated_at" : now_ist().strftime("%Y-%m-%dT%H:%M:%S+05:30"),
        "runs"       : all_runs[-30:]
    }
    with open(RESULTS_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"  ✅ results.json written ({len(all_runs)} run(s) stored)")


# ─── WHATSAPP ─────────────────────────────────────────────────────────────────

def send_whatsapp_text(client, body):
    chunks = [body[i:i+1500] for i in range(0, len(body), 1500)]
    for i, chunk in enumerate(chunks, 1):
        msg = client.messages.create(
            from_=TWILIO_FROM, to=WHATSAPP_TO, body=chunk)
        print(f"  ✅ Text chunk {i}/{len(chunks)}  SID={msg.sid}")
        time.sleep(1)

def send_whatsapp_media(client, media_url, caption):
    msg = client.messages.create(
        from_=TWILIO_FROM, to=WHATSAPP_TO,
        body=caption, media_url=[media_url])
    print(f"  ✅ Media sent  SID={msg.sid}")

def format_entries_for_whatsapp(matches, lawyer_name, list_type, date_str, pdf_url=None):
    prefix = LIST_PREFIX.get(list_type.upper(), list_type.title())
    if not matches:
        lines = [
            f"⚖️ *{prefix} Causelist Alert*",
            f"Lawyer : {lawyer_name}",
            f"Date   : {date_str}",
            "",
            "❌ No hearings found.",
        ]
        if pdf_url:
            lines += ["", f"📄 Full Causelist PDF:\n{pdf_url}"]
        return "\n".join(lines)

    lines = [
        f"⚖️ *{prefix} Causelist Alert*",
        f"Lawyer : {lawyer_name}",
        f"Date   : {date_str}",
        f"Cases  : {len(matches)}",
    ]
    if pdf_url:
        lines += [f"📄 PDF: {pdf_url}"]
    lines += ["─" * 35]

    for i, m in enumerate(matches, 1):
        judge_str = "\n   ".join(m["judges"])
        lines += [
            f"\n*Case {i}  (Page {m['page']})*",
            f"⚖️  {judge_str}",
            f"🏛️  {m['court']}",
            f"📋  {m['list_no']}",
            f"• *SNo*     : {m['sno']}",
            f"• *Case No* : {m['case_no']}",
            f"• *Purpose* : {m['purpose']}",
            "─" * 35,
        ]
    return "\n".join(lines)


# ─── MAIN ─────────────────────────────────────────────────────────────────────

twilio_client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN) if WHATSAPP_ENABLED else None

existing = load_existing_results()
all_runs = existing.get("runs", [])

for list_type in LIST_TYPES:
    print(f"\n{'═'*55}")
    print(f"  Processing: {list_type}")
    print(f"{'═'*55}")

    # ── Download ──────────────────────────────────────────────────────────────
    if SEARCH_ONLY:
        if os.path.exists(PDF_PATH):
            used_list = list_type
            used_date = "cached"
            used_pdf_url = None
            pdf_ok = True
            print(f"  SEARCH_ONLY mode — using existing {PDF_PATH}")
        else:
            print(f"  ❌ SEARCH_ONLY=True but {PDF_PATH} not found. Skipping.")
            continue
    else:
        pdf_ok = False
        used_list = list_type
        used_date = None
        used_pdf_url = None
        for attempt in range(1, MAX_RETRIES + 1):
            print(f"  Attempt {attempt} / {MAX_RETRIES} …")
            ok, lt_used, date_used, pdf_url_used = run_once(list_type)
            if ok:
                pdf_ok       = True
                used_date    = date_used
                used_pdf_url = pdf_url_used
                break
            if attempt < MAX_RETRIES:
                print(f"  Waiting {RETRY_DELAY}s …")
                time.sleep(RETRY_DELAY)

        if not pdf_ok:
            print(f"  ❌ Could not download {list_type}. Skipping.")
            continue

    if RAW_DEBUG:
        import pdfplumber as _plumber
        with _plumber.open(PDF_PATH) as _pdf:
            for _pg in DEBUG_PAGES:
                if _pg <= len(_pdf.pages):
                    _text = _pdf.pages[_pg-1].extract_text() or ""
                    print(f"\n{'='*60}\nRAW PAGE {_pg}\n{'='*60}")
                    for _line in _text.splitlines():
                        marker = " <-- BANJARE" if "BANJARE" in _line.upper() else ""
                        print(f"  {_line!r}{marker}")
        continue

    # ── Parse ─────────────────────────────────────────────────────────────────
    print(f"\n  Parsing PDF for '{LAWYER_NAME}' …")
    matches, total_pages = parse_pdf(PDF_PATH, LAWYER_NAME)

    if matches:
        print(f"\n✅  Found in {len(matches)} case(s) [{list_type}]:\n")
        for i, m in enumerate(matches, 1):
            print(f"  ── Case {i} | Page {m['page']} ──────────────────────────────────────")
            for j in m["judges"]:
                print(f"  ⚖️   {j}")
            print(f"  🏛️   {m['court']}")
            print(f"  📋  {m['list_no']}")
            print(f"  SNo     : {m['sno']}")
            print(f"  Case No : {m['case_no']}")
            print(f"  Purpose : {m['purpose']}")
            print()
    else:
        print(f"\n❌  '{LAWYER_NAME}' not found in {list_type}.")

    # ── Upload PDF for public link ────────────────────────────────────────────
    print(f"\n  Uploading PDF for public link …")
    pdf_public_url = upload_pdf(PDF_PATH)

    # ── Write results.json ────────────────────────────────────────────────────
    run_entry = {
        "list_type"  : list_type,
        "date_str"   : used_date or "",
        "ran_at"     : now_ist().strftime("%Y-%m-%dT%H:%M:%S+05:30"),
        "pdf_url"    : pdf_public_url or used_pdf_url or "",
        "pdf_pages"  : total_pages,
        "match_count": len(matches),
        "matches"    : matches,
    }
    # Replace existing run for same list_type+date, or append
    replaced = False
    for i, r in enumerate(all_runs):
        if r.get("list_type") == list_type and r.get("date_str") == used_date:
            all_runs[i] = run_entry
            replaced = True
            break
    if not replaced:
        all_runs.append(run_entry)

    save_results_json(all_runs)

    # ── WhatsApp ──────────────────────────────────────────────────────────────
    if not WHATSAPP_ENABLED:
        print("  WhatsApp disabled. Set WHATSAPP_ENABLED=true to send.")
        continue

    prefix = LIST_PREFIX.get(used_list.upper(), used_list.title())

    if pdf_public_url:
        print("  Sending PDF …")
        try:
            send_whatsapp_media(
                twilio_client,
                pdf_public_url,
                f"📄 *{prefix} Causelist*\n{used_date} — Chhattisgarh High Court"
            )
        except Exception as e:
            print(f"  ⚠️  PDF send failed: {e}")

    time.sleep(2)

    print("  Sending case entries …")
    wa_text = format_entries_for_whatsapp(matches, LAWYER_NAME, used_list, used_date,
                                          pdf_url=pdf_public_url)
    try:
        send_whatsapp_text(twilio_client, wa_text)
    except Exception as e:
        err = str(e)
        if "exceeded" in err.lower() and "daily messages limit" in err.lower():
            print(f"\n⚠️  Twilio daily limit reached. Upgrade at twilio.com.")
        else:
            print(f"  ⚠️  WhatsApp send failed: {e}")

    time.sleep(3)

print(f"\n{'═'*55}")
print(f"  ✅ All done!")
print(f"{'═'*55}")
