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
TWILIO_ACCOUNT_SID = _cred_os.environ.get("TWILIO_ACCOUNT_SID", "")
TWILIO_AUTH_TOKEN  = _cred_os.environ.get("TWILIO_AUTH_TOKEN",  "")
TWILIO_FROM        = "whatsapp:+14155238886"
WHATSAPP_TO        = "whatsapp:" + _cred_os.environ.get("WHATSAPP_TO", "")
LAWYER_NAME        = _cred_os.environ.get("LAWYER_NAME", "")

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

# ── HC Holiday Calendar 2026 ─────────────────────────────────────────────────
# Source: High Court of Chhattisgarh Calendar 2026
# Includes: gazette holidays, summer vacation, winter holidays
HC_HOLIDAYS_2026 = {
    date(2026, 1, 1),   # New Year Day
    date(2026, 1, 26),  # Republic Day
    date(2026, 3, 4),   # Holi
    date(2026, 3, 5),   # Holi
    date(2026, 3, 26),  # Ram Navami
    date(2026, 3, 31),  # Mahaveer Jayanti
    date(2026, 4, 3),   # Good Friday
    date(2026, 4, 14),  # Dr. Ambedkar Jayanti
    date(2026, 5, 27),  # Id-Ul-Zuha (Bakrid)
    date(2026, 6, 26),  # Muharram
    date(2026, 8, 26),  # Milad-Un-Nabi
    date(2026, 8, 28),  # Raksha Bandhan
    date(2026, 9, 4),   # Krishna Janmashtami
    date(2026, 9, 14),  # Ganesh Chaturthi
    date(2026, 10, 2),  # Gandhi Jayanti
    date(2026, 10, 19), # Dashera Holidays
    date(2026, 10, 20),
    date(2026, 10, 21),
    date(2026, 10, 22),
    date(2026, 10, 23),
    date(2026, 11, 6),  # Deepawali Holidays
    date(2026, 11, 7),
    date(2026, 11, 8),
    date(2026, 11, 9),
    date(2026, 11, 10),
    date(2026, 11, 11),
    date(2026, 11, 12),
    date(2026, 11, 13),
    date(2026, 11, 24), # Gurunanak Jayanti
    date(2026, 12, 18), # Guru Ghasidas Jayanti
    date(2026, 12, 25), # Christmas
    # Summer Vacation: 18 May – 12 Jun 2026
    *[date(2026, 5, d) for d in range(18, 32)],
    *[date(2026, 6, d) for d in range(1, 13)],
    # Winter Holidays: 24 Dec – 31 Dec 2026
    *[date(2026, 12, d) for d in range(24, 32)],
}

def is_holiday_saturday(d):
    """Returns True if date is a 2nd or 3rd Saturday of the month (court holiday)."""
    if d.weekday() != 5:
        return False
    sat_count = sum(
        1 for day in range(1, d.day + 1)
        if date(d.year, d.month, day).weekday() == 5
    )
    return sat_count in (2, 3)

def is_court_holiday(d):
    """
    Returns True if the court is closed on date d.
    Closed on: Sundays, 2nd & 3rd Saturdays, and HC gazette holidays.
    """
    if d.weekday() == 6:          # Sunday
        return True
    if is_holiday_saturday(d):    # 2nd or 3rd Saturday
        return True
    if d in HC_HOLIDAYS_2026:     # gazette/vacation holiday
        return True
    return False

def is_court_working_day(d):
    return not is_court_holiday(d)

def next_working_day(d):
    """Return the next court working day after d (skips series of holidays)."""
    d = d + timedelta(days=1)
    while is_court_holiday(d):
        d += timedelta(days=1)
    return d

def get_file_dates(list_type, upload_date):
    """
    Return list of candidate file_dates for a given upload_date and list type.

    Rules:
      Daily        : file_date = upload_date + 2 calendar days, then advance
                     past any holidays/weekends to next working day.
                     Friday/Saturday: also try the Tuesday of next week.
      Supplementary: file_date = upload_date + 1 calendar day, then advance
                     past any holidays/weekends to next working day.
      Weekly       : Monday of next week after upload Wednesday.

    If the computed file_date is a holiday, it is pushed forward to the
    next working day (handles series of consecutive holidays).
    """
    lt = list_type.upper()

    def advance_to_working(d):
        """Push d forward until it lands on a court working day."""
        while is_court_holiday(d):
            d += timedelta(days=1)
        return d

    if "SUPPLEMENT" in lt:
        suffix     = "-SUP1"
        fd         = advance_to_working(upload_date + timedelta(days=1))
        file_dates = [fd]

    elif "WEEK" in lt:
        suffix         = "-WKL"
        current_monday = upload_date - timedelta(days=upload_date.weekday())
        file_dates     = [current_monday + timedelta(days=7)]

    else:
        suffix     = ""
        fd         = advance_to_working(upload_date + timedelta(days=2))
        file_dates = [fd]
        # Friday upload: also try the next Tuesday (+4 days)
        if upload_date.weekday() == 4:
            tue = advance_to_working(upload_date + timedelta(days=4))
            if tue not in file_dates:
                file_dates.append(tue)
        # Saturday upload: also try the next Tuesday (+3 days)
        elif upload_date.weekday() == 5:
            tue = advance_to_working(upload_date + timedelta(days=3))
            if tue not in file_dates:
                file_dates.append(tue)

    return suffix, file_dates



def find_all_available(list_type, already_processed=None):
    """
    Find and download ALL available causelist PDFs for list_type.

    Instead of returning on the first success, this probes every candidate
    file_date and returns a list of all that are available.  This handles
    pre-holiday scenarios where the HC uploads multiple causelists on the
    same day (e.g. Apr 2 AND Apr 6 both available before Good Friday).

    Args:
        already_processed: set of date_str values already in results.json for this
                           list_type, so we can skip re-downloading them.

    Returns list of (list_type, date_str, url, pdf_bytes) for every available PDF.
    """
    today   = now_ist().date()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/122.0.0.0 Safari/537.36",
        "Referer"   : "https://highcourt.cg.gov.in/clists/courtlist.php",
        "Accept"    : "application/pdf,*/*",
    }

    if already_processed is None:
        already_processed = set()

    # ── Build candidate file_dates ────────────────────────────────────────────
    candidates = []

    if "WEEK" in list_type.upper():
        suffix = "-WKL"
        current_monday = today - timedelta(days=today.weekday())
        for week_offset in [1, 0, 2]:
            raw_monday = current_monday + timedelta(weeks=week_offset)
            # Advance past gazette holidays / non-working days to the next
            # working day — mirrors get_file_dates() logic for Daily/Suppl.
            file_date = raw_monday
            while is_court_holiday(file_date):
                file_date += timedelta(days=1)
            candidates.append(file_date)
    else:
        suffix = "-SUP1" if "SUPPLEMENT" in list_type.upper() else ""
        future = []
        d = today
        while len(future) < 7 and d <= today + timedelta(days=15):
            if is_court_working_day(d):
                future.append(d)
            d += timedelta(days=1)

        past = []
        d = today - timedelta(days=1)
        while len(past) < 5 and d >= today - timedelta(days=10):
            if is_court_working_day(d):
                past.append(d)
            d -= timedelta(days=1)

        candidates = future + past

    # Deduplicate preserving order
    seen = set()
    unique = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            unique.append(c)

    # ── Probe each candidate URL ──────────────────────────────────────────────
    found = []
    for file_date in unique:
        date_str = file_date.strftime("%d %b,%Y")

        if date_str in already_processed:
            print(f"  Skipping {list_type} for {date_str} — already processed")
            continue

        base_name = f"CG{file_date.strftime('%d%m%Y')}{suffix}"
        # HC server is case-sensitive: some files are .pdf, some .PDF
        # Try lowercase first (most common), then uppercase as fallback
        for ext in (".pdf", ".PDF"):
            filename = f"{base_name}{ext}"
            url      = f"https://highcourt.cg.gov.in/clists/causelists/pdf/{filename}"
            print(f"  Trying {list_type}: {filename}  ({url})")

            try:
                resp = requests.get(url, headers=headers, timeout=30)
                if resp.status_code == 200 and resp.content[:5].startswith(b"%PDF"):
                    print(f"  ✅ Available {filename} ({len(resp.content)//1024} KB)")
                    found.append((list_type, date_str, url, resp.content))
                    break   # found with this extension, no need to try the other
                else:
                    print(f"     HTTP {resp.status_code} — not found")
            except Exception as e:
                print(f"     Error: {e}")

    if not found:
        print(f"  ❌ No new {list_type} causelists found.")
    else:
        print(f"  📋 Found {len(found)} new {list_type} causelist(s)")
    return found



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

def _clean_parser_line(line):
    return re.sub(
        r'\s*\(?\s*Live\s+Stream\s*[-\u2013]?\s*(Yes|No)\s*\)?',
        '', line, flags=re.IGNORECASE
    ).strip()


def _should_skip_parser_line(line):
    u = line.strip().upper()
    t = line.strip()

    if not t or t in ("-", "â€“", ".", ":", "[]"):
        return True
    if "BY ORDER OF" in u and "CHIEF JUSTICE" in u:
        return True
    if "THIS CAUSE LIST IS PUBLISHED" in u and "CHIEF JUSTICE" in u:
        return True
    if re.match(r'^SD/[-â€“]', u):
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


def _build_visual_lines(page):
    words = page.extract_words(
        x_tolerance=2,
        y_tolerance=2,
        keep_blank_chars=False,
        use_text_flow=False,
    ) or []
    if not words:
        return []

    words = sorted(words, key=lambda w: (float(w["top"]), float(w["x0"])))
    rows = []
    current = []
    line_top = None

    for word in words:
        top = float(word["top"])
        if current and line_top is not None and abs(top - line_top) > 4:
            rows.append(current)
            current = [word]
            line_top = top
        else:
            current.append(word)
            line_top = top if line_top is None else (line_top + top) / 2
    if current:
        rows.append(current)

    visual_lines = []
    for row in rows:
        row = sorted(row, key=lambda w: float(w["x0"]))
        text = " ".join(w["text"] for w in row).strip()
        if not text:
            continue
        visual_lines.append({
            "text": text,
            "top": min(float(w["top"]) for w in row),
            "bottom": max(float(w["bottom"]) for w in row),
            "x0": min(float(w["x0"]) for w in row),
            "x1": max(float(w["x1"]) for w in row),
            "words": row,
        })
    return visual_lines


def _parse_pdf_legacy(pdf_path, lawyer_name):
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


def _parse_pdf_structured(pdf_path, lawyer_name):
    name_upper = lawyer_name.upper()
    honble_re = re.compile(r"HON['\u2018\u2019\u02bc]?BLE\b", re.IGNORECASE)
    court_re = re.compile(r"COURT\s+NO\.", re.IGNORECASE)
    list_no_re = re.compile(r"^\s*LIST\s*[-\u2013]\s*\d+", re.IGNORECASE)
    sno_only_re = re.compile(r"^\s*(\d+)\.\s*$")
    entry_line_re = re.compile(r"^\s*(\d+)\.\s")
    case_no_re = re.compile(r"^[A-Z0-9./()-]+/\d{1,6}/\d{4}[A-Z0-9./()-]*$", re.IGNORECASE)

    sections = []
    all_lines = []
    cases = []
    total_pages = 0
    pending_judges = []
    pending_court = ""
    current_case = None
    active_template = None

    def section_for_line(idx):
        active = {"judges": ["Unknown"], "court": "Unknown", "list_no": "Unknown"}
        for s in sections:
            if s["line_idx"] <= idx:
                active = s
            else:
                break
        return active

    def flush_case():
        nonlocal current_case, active_template
        if not current_case:
            return

        anchors = dict(active_template or {})
        lane_parts = {"party_detail": [], "pet_advocate": [], "res_advocate": []}
        purpose_lines = []
        anchor_threshold = 55.0

        for line in current_case["lines"]:
            text = line["text"].strip()
            if not text:
                continue
            if text.startswith("*") or text.startswith("["):
                purpose_lines.append(text)
                continue

            fragments = []
            current_words = []
            for word in sorted(line["words"], key=lambda w: float(w["x0"])):
                if not current_words:
                    current_words = [word]
                    continue
                gap = float(word["x0"]) - float(current_words[-1]["x1"])
                if gap > 28:
                    fragments.append(current_words)
                    current_words = [word]
                else:
                    current_words.append(word)
            if current_words:
                fragments.append(current_words)

            pieces = []
            for frag in fragments:
                frag_text = " ".join(w["text"] for w in frag).strip()
                if not frag_text:
                    continue
                pieces.append({
                    "text": frag_text,
                    "x0": float(frag[0]["x0"]),
                })

            if line["is_case_start"] and pieces:
                if sno_only_re.match(pieces[0]["text"]):
                    pieces = pieces[1:]
                if pieces and case_no_re.match(pieces[0]["text"]):
                    pieces = pieces[1:]

            if not pieces:
                continue

            if len(pieces) >= 2:
                anchors["party_detail"] = pieces[0]["x0"]
                anchors["pet_advocate"] = pieces[1]["x0"]
                if len(pieces) >= 3:
                    anchors["res_advocate"] = pieces[2]["x0"]
                active_template = dict(anchors)

            for piece in pieces:
                piece_text = piece["text"]
                if piece_text.startswith("*") or piece_text.startswith("["):
                    purpose_lines.append(piece_text)
                    continue

                lane = None
                if re.search(r'\bVS\.?\b', piece_text, re.IGNORECASE):
                    lane = "party_detail"
                elif anchors:
                    distances = {
                        name: abs(piece["x0"] - anchor_x)
                        for name, anchor_x in anchors.items()
                    }
                    lane = min(distances, key=distances.get)
                    if distances[lane] > anchor_threshold:
                        if "res_advocate" not in anchors and piece["x0"] > anchors.get("pet_advocate", 0) + 90:
                            anchors["res_advocate"] = piece["x0"]
                            lane = "res_advocate"
                            active_template = dict(anchors)
                        elif "pet_advocate" not in anchors and piece["x0"] > anchors.get("party_detail", 0) + 90:
                            anchors["pet_advocate"] = piece["x0"]
                            lane = "pet_advocate"
                            active_template = dict(anchors)

                if lane is None:
                    if not lane_parts["party_detail"]:
                        lane = "party_detail"
                    elif not lane_parts["pet_advocate"]:
                        lane = "pet_advocate"
                        anchors.setdefault("pet_advocate", piece["x0"])
                    else:
                        lane = "res_advocate"
                        anchors.setdefault("res_advocate", piece["x0"])
                    active_template = dict(anchors)

                lane_parts[lane].append(piece_text)

        combined_text = " ".join(
            lane_parts["party_detail"]
            + lane_parts["pet_advocate"]
            + lane_parts["res_advocate"]
            + purpose_lines
        )
        if name_upper in combined_text.upper():
            sec = section_for_line(current_case["start_idx"])
            cases.append({
                "page": current_case["page"],
                "judges": sec["judges"],
                "court": sec["court"],
                "list_no": sec["list_no"],
                "sno": current_case["sno"],
                "case_no": current_case["case_no"],
                "purpose": " ".join(purpose_lines).strip(),
                "party_detail": " ".join(lane_parts["party_detail"]).strip(),
                "pet_advocate": " ".join(lane_parts["pet_advocate"]).strip(),
                "res_advocate": " ".join(lane_parts["res_advocate"]).strip(),
            })
        current_case = None

    with pdfplumber.open(pdf_path) as pdf:
        total_pages = len(pdf.pages)
        print(f"  Total pages: {total_pages}")
        for page_num, page in enumerate(pdf.pages, start=1):
            visual_lines = _build_visual_lines(page)
            for line in visual_lines:
                line["text"] = _clean_parser_line(line["text"])
                if not line["text"]:
                    continue
                line["page"] = page_num
                line["is_case_start"] = False

                if not _should_skip_parser_line(line["text"]):
                    idx = len(all_lines)
                    all_lines.append({"page": page_num, "text": line["text"]})
                    s = line["text"]
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
                            "judges": list(pending_judges) or ["Unknown"],
                            "court": pending_court or "Unknown",
                            "list_no": s,
                        })
                        pending_judges = []
                        pending_court = ""
                    elif pending_judges and not pending_court and entry_line_re.match(line["text"]):
                        pending_judges = []
                        pending_court = ""

                row_words = sorted(line["words"], key=lambda w: float(w["x0"]))
                if len(row_words) >= 2:
                    left_word = row_words[0]["text"].strip()
                    case_word = row_words[1]["text"].strip()
                    is_case_start = (
                        sno_only_re.match(left_word)
                        and case_no_re.match(case_word)
                        and float(row_words[0]["x0"]) < page.width * 0.12
                        and float(row_words[1]["x0"]) < page.width * 0.32
                    )
                    if is_case_start:
                        flush_case()
                        line["is_case_start"] = True
                        current_case = {
                            "page": page_num,
                            "start_idx": max(len(all_lines) - 1, 0),
                            "sno": left_word.rstrip("."),
                            "case_no": case_word,
                            "lines": [line],
                        }
                        continue

                if current_case:
                    current_case["lines"].append(line)

        flush_case()

    return cases, total_pages


def parse_pdf(pdf_path, lawyer_name):
    try:
        results, total_pages = _parse_pdf_structured(pdf_path, lawyer_name)
        if results:
            return results, total_pages
        print("  Structured parser found no lawyer matches; falling back to legacy parser.")
    except Exception as e:
        print(f"  Structured parser failed: {e}")
        print("  Falling back to legacy parser.")
    return _parse_pdf_legacy(pdf_path, lawyer_name)


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
        "runs"       : all_runs[-30:]
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
        entry = [
            f"\n*Case {i}  (Page {m['page']})*",
            f"⚖️  {judge_str}",
            f"🏛️  {m['court']}",
            f"📋  {m['list_no']}",
            f"• *SNo*          : {m['sno']}",
            f"• *Case No*      : {m['case_no']}",
        ]
        # Include structured fields only when present (structured parser)
        if m.get("party_detail"):
            entry.append(f"• *Party Detail* : {m['party_detail']}")
        if m.get("pet_advocate"):
            entry.append(f"• *Pet Advocate* : {m['pet_advocate']}")
        if m.get("res_advocate"):
            entry.append(f"• *Res Advocate* : {m['res_advocate']}")
        entry += [
            f"• *Purpose*      : {m['purpose']}",
            "─" * 35,
        ]
        lines += entry
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

# ── Process ALL list types in order ──────────────────────────────────────────
import os

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
            print(f"  SEARCH_ONLY mode — using existing {PDF_PATH}")
            # Parse, upload, WhatsApp for the cached file (single-file mode)
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
        else:
            print(f"  ❌ SEARCH_ONLY=True but {PDF_PATH} not found. Skipping.")
        continue

    # ── RAW_DEBUG ─────────────────────────────────────────────────────────────
    if RAW_DEBUG:
        # In raw debug mode we still need to download one file first
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

    # ── Find ALL available causelists ─────────────────────────────────────────
    already_processed = {
        r["date_str"] for r in all_runs
        if r.get("list_type") == list_type and r.get("date_str")
    }

    available = []
    for attempt in range(1, MAX_RETRIES + 1):
        print(f"  Attempt {attempt} / {MAX_RETRIES} …")
        available = find_all_available(list_type, already_processed)
        if available:
            break
        if attempt < MAX_RETRIES:
            print(f"  Waiting {RETRY_DELAY}s …")
            time.sleep(RETRY_DELAY)

    if not available:
        print(f"  ❌ Could not download {list_type}. Skipping.")
        continue

    # ── Process EACH available causelist ───────────────────────────────────────
    for pdf_idx, (lt_used, date_used, pdf_src, pdf_bytes) in enumerate(available):
        print(f"\n  ── Processing {list_type} for {date_used} ({pdf_idx+1}/{len(available)}) ──")

        # Save PDF to disk
        with open(PDF_PATH, "wb") as f:
            f.write(pdf_bytes)
        print(f"  ✅ Saved {len(pdf_bytes)//1024} KB")

        # ── Parse ─────────────────────────────────────────────────────────────
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
            print(f"\n❌  '{LAWYER_NAME}' not found in {list_type} for {date_used}.")

        # ── Upload PDF for public download link ──────────────────────────────
        print(f"\n  Uploading PDF …")
        pdf_public_url = upload_pdf(PDF_PATH)

        # ── Write results.json ───────────────────────────────────────────────
        run_entry = {
            "list_type"  : list_type,
            "date_str"   : date_used or "",
            "ran_at"     : now_ist().strftime("%Y-%m-%dT%H:%M:%S+05:30"),
            "pdf_url"    : pdf_public_url or pdf_src or "",
            "pdf_pages"  : total_pages,
            "match_count": len(matches),
            "matches"    : matches,
        }
        replaced = False
        for i, r in enumerate(all_runs):
            if r.get("list_type") == list_type and r.get("date_str") == date_used:
                all_runs[i] = run_entry
                replaced = True
                break
        if not replaced:
            all_runs.append(run_entry)
        save_results_json(all_runs)

        # ── WhatsApp ─────────────────────────────────────────────────────────
        if not WHATSAPP_ENABLED:
            print("  WhatsApp disabled. Set WHATSAPP_ENABLED=true to send.")
            continue

        prefix = LIST_PREFIX.get(lt_used.upper(), lt_used.title())

        if pdf_public_url:
            print("  Sending PDF …")
            try:
                send_whatsapp_media(
                    twilio_client,
                    pdf_public_url,
                    f"📄 *{prefix} Causelist*\n{date_used} — Chhattisgarh High Court"
                )
            except Exception as e:
                print(f"  ⚠️  PDF send failed: {e}")

        time.sleep(2)

        print("  Sending case entries …")
        wa_text = format_entries_for_whatsapp(matches, LAWYER_NAME, lt_used, date_used,
                                              pdf_url=pdf_public_url)
        try:
            send_whatsapp_text(twilio_client, wa_text)
        except Exception as e:
            err = str(e)
            if "exceeded" in err.lower() and "daily messages limit" in err.lower():
                print(f"\n⚠️  Twilio daily limit reached. Upgrade at twilio.com.")
            else:
                print(f"  ⚠️  WhatsApp send failed: {e}")

        time.sleep(3)   # pause between causelists

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
