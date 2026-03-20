"""
Chhattisgarh High Court – Causelist Tracker + WhatsApp Notifier

Downloads causelist PDFs directly using known URL patterns — no browser needed.

File naming (confirmed from live URLs):
  Daily List        : CG<DDMMYYYY>.pdf          e.g. CG19032026.pdf
  Supplementary List: CG<DDMMYYYY>-SUP1.pdf     e.g. CG18032026-SUP1.pdf  (date = upload day + 1)
  Weekly List       : CG<DDMMYYYY>-WKL.pdf      e.g. CG16032026-WKL.pdf   (date = Wednesday of that week)

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
from datetime import date, timedelta, datetime
import pytz

# ─── CONFIG ───────────────────────────────────────────────────────────────────
PDF_PATH           = "causelist.pdf"
RESULTS_JSON_PATH  = "results.json"   # committed back to repo after every run

# Credentials — read from environment variables when running in the cloud,
# fall back to hardcoded values when running locally on your PC.
import os as _cred_os
TWILIO_ACCOUNT_SID = _cred_os.environ.get("TWILIO_ACCOUNT_SID", "ACdaaaaab8004a7a18d3e9fa592aacf4df")
TWILIO_AUTH_TOKEN  = _cred_os.environ.get("TWILIO_AUTH_TOKEN",  "92f931df1ef9524cfd285762b0f066db")
TWILIO_FROM        = "whatsapp:+14155238886"
WHATSAPP_TO        = "whatsapp:" + _cred_os.environ.get("WHATSAPP_TO", "+919826474009")
LAWYER_NAME        = _cred_os.environ.get("LAWYER_NAME", "SANJEEV BANJARE")

MAX_RETRIES  = 3
RETRY_DELAY  = 15
LIST_TYPES   = ["DAILY LIST", "SUPPLEMENTARY LIST", "WEEKLY LIST"]

# If launched by the scheduler, it sets CL_LIST_TYPE to run just one list
import os as _os
_override = _os.environ.get("CL_LIST_TYPE", "").strip()
if _override and _override in LIST_TYPES:
    LIST_TYPES = [_override]


SEARCH_ONLY = False  # set True to skip download and re-parse existing causelist.pdf
DEBUG_PARSE = False  # set True to print every line containing lawyer name as kept/skipped
RAW_DEBUG   = False  # set True to dump raw pdfplumber text from DEBUG_PAGES
DEBUG_PAGES = [27, 137, 153]  # pages to dump when RAW_DEBUG=True
WHATSAPP_ENABLED = _cred_os.environ.get("WHATSAPP_ENABLED", "false").lower() == "true"  # overridden by env var in GitHub Actions
LIST_PREFIX = {
    "DAILY LIST":         "Daily",
    "SUPPLEMENTARY LIST": "Supplementary",
    "WEEKLY LIST":        "Weekly",
}
# ─────────────────────────────────────────────────────────────────────────────


# ══════════════════════════════════════════════════════════════════════════════
#  BROWSER / DOWNLOAD
# ══════════════════════════════════════════════════════════════════════════════

# ── PDF URL builder ──────────────────────────────────────────────────────────

IST = pytz.timezone("Asia/Kolkata")

def now_ist():
    from datetime import datetime
    return datetime.now(IST)

def is_holiday_saturday(d):
    """Returns True if date is a 2nd or 3rd Saturday of the month (court holiday)."""
    if d.weekday() != 5:   # not a Saturday
        return False
    sat_count = sum(
        1 for day in range(1, d.day + 1)
        if date(d.year, d.month, day).weekday() == 5
    )
    return sat_count in (2, 3)

def is_court_working_day(d):
    """Court works Mon–Sat except 2nd and 3rd Saturdays."""
    if d.weekday() == 6:          # Sunday
        return False
    if is_holiday_saturday(d):    # 2nd or 3rd Saturday
        return False
    return True

def pdf_filename_and_url(list_type, upload_date):
    """
    Build the PDF filename and full URL from upload_date and list type.

    Confirmed naming from live URL CG19032026.pdf (Daily, uploaded 18 Mar, covers 19 Mar):
      Daily        : upload_date + 2 days  → CG<DDMMYYYY>.pdf
      Supplementary: upload_date + 1 day   → CG<DDMMYYYY>-SUP1.pdf
      Weekly       : Wednesday of that week → CG<DDMMYYYY>-WKL.pdf
    """
    lt = list_type.upper()
    if "SUPPLEMENT" in lt:
        file_date = upload_date + timedelta(days=1)
        suffix    = "-SUP1"
    elif "WEEK" in lt:
        # Uploaded every Wednesday; file covers the COMING week.
        # File name = Monday of the next week after upload_date.
        # e.g. uploaded Wed 18 Mar → coming week starts Mon 23 Mar → CG23032026-WKL.pdf
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
    """
    Download the causelist PDF for list_type using direct HTTP (no browser).
    Tries today's date first, then falls back one working day at a time.
    Returns (True, list_type, date_str) on success, (False, ...) on failure.
    """
    today    = now_ist().date()
    headers  = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/122.0.0.0 Safari/537.36",
        "Referer"   : "https://highcourt.cg.gov.in/clists/courtlist.php",
        "Accept"    : "application/pdf,*/*",
    }

    # Try today and the last 7 days (need more range to skip holidays)
    for days_back in range(8):
        candidate = today - timedelta(days=days_back)

        # Skip non-working days (Sunday, 2nd & 3rd Saturday)
        if not is_court_working_day(candidate):
            continue

        # Weekly list only makes sense on/near a Wednesday
        if "WEEK" in list_type.upper():
            days_to_wed = (2 - candidate.weekday()) % 7
            wed = candidate + timedelta(days=days_to_wed)
            if abs((candidate - wed).days) > 6:
                continue

        filename, url, file_date = pdf_filename_and_url(list_type, candidate)

        # Also skip if the file_date itself is a holiday (no list released for that day)
        if not is_court_working_day(file_date):
            print(f"  Skipping {filename} — file date {file_date} is a court holiday")
            continue

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



# ══════════════════════════════════════════════════════════════════════════════
#  PDF PARSER  —  two-pass approach
#
#  PASS 1: Read every line of the entire PDF into a flat list with page numbers.
#          While scanning, whenever we detect a complete section header
#          (judges + court + list_no), record a "section" with its
#          starting line index.
#
#  PASS 2: Walk the flat line list again. Split into case-entry blocks
#          (each starts with "N.  "). For each block check which section
#          it belongs to (the most recently started section before it).
#          If the block contains the lawyer name → add to results.
# ══════════════════════════════════════════════════════════════════════════════

def parse_pdf(pdf_path, lawyer_name):
    """
    Simple two-pass parser:
    PASS 1 : Build flat line list, detect section headers (judges/court/list).
    PASS 2 : Split into entry blocks by serial number lines ("72." etc).
             Return raw text block as-is for every block containing lawyer_name.
    """
    name_upper = lawyer_name.upper()

    honble_re  = re.compile(r"HON['\u2018\u2019\u02bc]?BLE\b", re.IGNORECASE)
    court_re   = re.compile(r"COURT\s+NO\.", re.IGNORECASE)
    list_no_re = re.compile(r"^\s*LIST\s*[-\u2013]\s*\d+", re.IGNORECASE)
    entry_re   = re.compile(r"^\s*(\d+)\.\s")   # "72. " etc.

    def should_skip(line):
        u = line.strip().upper()
        t = line.strip()

        # Empty or trivial
        if not t or t in ("-", "–", ".", ":", "[]"):
            return True

        # Signatures
        if "BY ORDER OF" in u and "CHIEF JUSTICE" in u:
            return True
        if "THIS CAUSE LIST IS PUBLISHED" in u and "CHIEF JUSTICE" in u:
            return True
        # Sd/- line (standalone)
        if re.match(r'^SD/[-–]', u):
            return True

        # "Print" alone
        if u == "PRINT":
            return True

        # Date header line — only skip when it's JUST the date
        # e.g. "THURSDAY THE 19TH MARCH 2026"  (no other content)
        if re.match(r'^(MONDAY|TUESDAY|WEDNESDAY|THURSDAY|FRIDAY|SATURDAY|SUNDAY)'
                    r'\s+THE\s+\d+', u):
            return True

        # VC Link
        if "VC LINK" in u or t.startswith("[VC"):
            return True

        # Disclaimer lines — only skip lines that are purely disclaimer text
        if re.match(r'^\[?DISCLAIMERS', u):
            return True
        if re.match(r'^\[?\s*\(I+\)', u):   # (i), (ii), (iii) items
            return True
        if "ARCHIVAL DATA" in u and "OFFICIAL RECORD" in u:
            return True
        if "UNLESS OTHERWISE DIRECTED BY THE JUDGE" in u:
            return True
        if re.match(r'^INFORMATION AND DISCLAIMER', u):
            return True

        # Column header row — exact match only
        if re.match(r'^S\s*NO\.?\s+CASE\s+NO\.?\s+PARTY', u):
            return True

        # Cause list title (standalone header, not inside a purpose block)
        if re.match(r'^(DAILY|WEEKLY|SUPPLEMENTARY)\s+CAUSE\s+LIST\s+FOR\b', u):
            return True

        # Section sub-headers that appear standalone between entries
        if re.match(r'^(FRESH MATTERS|OLD MATTERS|PART HEARD MATTERS'
                    r'|ADMISSION MATTERS|REGULAR HEARING)\s*$', u):
            return True

        return False


    def clean_line(line):
        """Remove Live Stream info from a line."""
        return re.sub(
            r'\s*\(?\s*Live\s+Stream\s*[-\u2013]?\s*(Yes|No)\s*\)?',
            '', line, flags=re.IGNORECASE
        ).strip()

    # ── PASS 1: flat line list + section map ─────────────────────────────────
    all_lines  = []   # list of {"page": int, "text": str}
    sections   = []   # list of {"line_idx", "judges", "court", "list_no"}
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
                    if DEBUG_PARSE and "BANJARE" in raw.upper():
                        print(f"  [SKIP] {raw!r}")
                    continue
                cleaned = clean_line(raw)
                if not cleaned:
                    continue
                if DEBUG_PARSE and "BANJARE" in cleaned.upper():
                    print(f"  [KEEP] {cleaned!r}")
                s = cleaned.strip()
                idx = len(all_lines)
                all_lines.append({"page": page_num, "text": cleaned})

                if honble_re.search(s):
                    # Only treat as a judge line if it does NOT contain
                    # "TIED UP" or "EXCEPTION" context markers — those are
                    # references to other benches, not the current bench header
                    upper_s = s.upper()
                    if "TIED UP" not in upper_s and "EXCEPTION" not in upper_s:
                        pending_judges.append(s)
                elif court_re.search(s):
                    # COURT NO. X confirms the pending judges are the real bench
                    pending_court = s
                elif "CHIEF JUSTICE" in s.upper() and "COURT" in s.upper():
                    # "THE CHIEF JUSTICE'S COURT" — no COURT NO. line in this section
                    pending_court = s
                elif list_no_re.match(s):
                    # Record section — use pending_court if found, else mark as
                    # Chief Justice's Court if judge line says Chief Justice
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
                    # A case entry line resets any incomplete pending header
                    if pending_judges and not pending_court:
                        if entry_re.match(cleaned):
                            pending_judges = []
                            pending_court  = ""

    # ── PASS 2: split into entry blocks, match lawyer name ───────────────────
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

    # carry_purpose accumulates purpose lines that spill across a page break.
    # When a block ends mid-purpose, those lines are carried into the next block.
    carry_purpose = []

    def extract_purpose(block):
        """Collect all purpose lines from block, prepending any carried lines."""
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
        """
        For non-matching blocks: if the block ends while still inside a purpose
        block (i.e. purpose text runs off the page), carry those lines forward.
        """
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
                # Not a match but had carry — reset carry
                carry_purpose = []
        else:
            # Not a match — check if purpose spills to next block
            update_carry(block)

    # ── Run pass 2 ───────────────────────────────────────────────────────────
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
    """
    Upload PDF and return a public URL.
    Tries multiple free hosts in order:
      1. tmpfiles.org  — returns plain-text URL, reliable
      2. 0x0.st        — fallback, also plain-text URL
    """
    # ── Host 1: tmpfiles.org ─────────────────────────────────────────────────
    print("  Uploading PDF to tmpfiles.org …")
    try:
        with open(pdf_path, "rb") as f:
            resp = requests.post(
                "https://tmpfiles.org/api/v1/upload",
                files={"file": (pdf_path, f, "application/pdf")},
                timeout=60
            )
        data = resp.json()
        # tmpfiles returns {"status":"success","data":{"url":"https://tmpfiles.org/..."}}
        url = (data.get("data") or {}).get("url", "")
        if url:
            # Convert browse URL to direct download URL
            # e.g. https://tmpfiles.org/1234/file.pdf
            #   -> https://tmpfiles.org/dl/1234/file.pdf
            url = url.replace("tmpfiles.org/", "tmpfiles.org/dl/", 1)
            print(f"  ✅ Uploaded: {url}")
            return url
        print(f"  ⚠️  tmpfiles.org failed: {data}")
    except Exception as e:
        print(f"  ⚠️  tmpfiles.org error: {e}")

    # ── Host 2: 0x0.st (fallback) ────────────────────────────────────────────
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


# ══════════════════════════════════════════════════════════════════════════════
#  RESULTS JSON  — written after every run so Lovable dashboard can fetch it
# ══════════════════════════════════════════════════════════════════════════════

def load_existing_results():
    if os.path.exists(RESULTS_JSON_PATH):
        try:
            with open(RESULTS_JSON_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"runs": []}

def save_results_json(all_runs):
    payload = {
        "lawyer_name": LAWYER_NAME,
        "updated_at" : now_ist().strftime("%Y-%m-%dT%H:%M:%S+05:30"),
        "runs"       : all_runs[-30:]   # keep last 30 runs
    }
    with open(RESULTS_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"  ✅ results.json written ({len(all_runs)} run(s) stored)")


# ══════════════════════════════════════════════════════════════════════════════
#  WHATSAPP
# ══════════════════════════════════════════════════════════════════════════════

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

def format_entries_for_whatsapp(matches, lawyer_name, list_type, date_str,
                                      pdf_url=None):
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


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

# ── Process ALL list types in order ──────────────────────────────────────────
import os

twilio_client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN) if WHATSAPP_ENABLED else None

existing  = load_existing_results()
all_runs  = existing.get("runs", [])

for list_type in LIST_TYPES:
    print(f"\n{'═'*55}")
    print(f"  Processing: {list_type}")
    print(f"{'═'*55}")

    # ── Download ──────────────────────────────────────────────────────────────
    if SEARCH_ONLY:
        if os.path.exists(PDF_PATH):
            used_list = list_type
            used_date = "cached"
            print(f"  SEARCH_ONLY mode — using existing {PDF_PATH}")
            pdf_ok = True
        else:
            print(f"  ❌ SEARCH_ONLY=True but {PDF_PATH} not found. Skipping.")
            continue
    else:
        pdf_ok    = False
        used_list = list_type
        used_date = None
        used_pdf_source_url = None
        for attempt in range(1, MAX_RETRIES + 1):
            print(f"  Attempt {attempt} / {MAX_RETRIES} …")
            ok, lt_used, date_used, pdf_src = run_once(list_type)
            if ok:
                pdf_ok    = True
                used_date = date_used
                used_pdf_source_url = pdf_src
                break
            if attempt < MAX_RETRIES:
                print(f"  Waiting {RETRY_DELAY}s …")
                time.sleep(RETRY_DELAY)

        if not pdf_ok:
            print(f"  ❌ Could not download {list_type}. Skipping.")
            continue

    # ── RAW_DEBUG ─────────────────────────────────────────────────────────────
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
        continue   # skip parse/send in RAW_DEBUG mode

    # ── Parse ─────────────────────────────────────────────────────────────────
    print(f"\n  Parsing PDF for '{LAWYER_NAME}' …")
    matches, total_pages = parse_pdf(PDF_PATH, LAWYER_NAME)

    if matches:
        print(f"\n✅  Found in {len(matches)} case(s) [{list_type}]:\n")
        for i, m in enumerate(matches, 1):
            print(f"  ── Case {i} | Page {m['page']} ────────────────────────────────────")
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

    # ── Upload PDF for public download link ───────────────────────────────────
    print(f"\n  Uploading PDF for public link …")
    pdf_public_url = upload_pdf(PDF_PATH)

    # ── Write results.json ────────────────────────────────────────────────────
    run_entry = {
        "list_type"  : list_type,
        "date_str"   : used_date or "",
        "ran_at"     : now_ist().strftime("%Y-%m-%dT%H:%M:%S+05:30"),
        "pdf_url"    : pdf_public_url or used_pdf_source_url or "",
        "pdf_pages"  : total_pages,
        "match_count": len(matches),
        "matches"    : matches,
    }
    # Replace existing entry for same list_type + date, or append
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

    # Send PDF (already uploaded above)
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

    # Send case entries
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

    time.sleep(3)   # pause between list types

print(f"\n{'═'*55}")
print(f"  ✅ All done!")
print(f"{'═'*55}")


# ══════════════════════════════════════════════════════════════════════════════
#  DIAGNOSTIC — run this block manually to calibrate column boundaries
#  Usage: set DIAG_PAGES below and run the script with DIAG_MODE = True
# ══════════════════════════════════════════════════════════════════════════════
DIAG_MODE   = False  # set True to print word coordinates instead of running main
DIAG_PAGES  = [27, 137, 153]   # pages to inspect

if DIAG_MODE:
    import pdfplumber
    with pdfplumber.open(PDF_PATH) as pdf:
        for pg in DIAG_PAGES:
            page = pdf.pages[pg - 1]
            pw   = page.width
            print(f"\n{'='*70}")
            print(f"PAGE {pg}  (width={pw:.1f})")
            print(f"{'='*70}")
            words = page.extract_words(x_tolerance=3, y_tolerance=3)
            # Group by row
            from collections import defaultdict
            rows = defaultdict(list)
            for w in words:
                rows[round(w["top"]/4)*4].append(w)
            for y, rw in sorted(rows.items()):
                rw_s = sorted(rw, key=lambda w: w["x0"])
                for w in rw_s:
                    pct = w["x0"] / pw * 100
                    print(f"  y={y:5.1f}  x={w['x0']:6.1f} ({pct:4.1f}%)  {w['text']!r}")
    import sys; sys.exit(0)
