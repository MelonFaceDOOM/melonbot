from __future__ import annotations
import csv, os, sys, time, tempfile
from typing import List, Tuple, Optional
from contextlib import closing

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import psycopg2
from psycopg2.extras import execute_values

from config import PSQL_CREDENTIALS  # expects a dict


URL = "https://cloud.google.com/text-to-speech/docs/list-voices-and-types"

REQUIRED_HEADERS = {"language", "voice name", "ssml gender"}  # case-insensitive

def _norm(s: str) -> str:
    return " ".join((s or "").split()).strip()

def _find_table(page) -> Optional[object]:
    page.wait_for_selector("table", timeout=90_000)
    tables = page.query_selector_all("table")
    for tbl in tables:
        headers = []
        thead = tbl.query_selector("thead")
        if thead:
            headers = [_norm(th.inner_text()) for th in thead.query_selector_all("th")]
        if not headers:
            first_tr = tbl.query_selector("tr")
            if first_tr:
                headers = [_norm(x.inner_text()) for x in first_tr.query_selector_all("th,td")]
        lower = {h.lower() for h in headers}
        if REQUIRED_HEADERS.issubset(lower):
            return tbl
    return None

def _extract_rows(table) -> List[Tuple[str, str, str]]:
    header_cells = []
    used_first_row_as_header = False
    thead = table.query_selector("thead")
    if thead:
        header_cells = thead.query_selector_all("th")
    if not header_cells:
        first_tr = table.query_selector("tr")
        if first_tr:
            header_cells = first_tr.query_selector_all("th,td")
            used_first_row_as_header = True

    headers = [_norm(h.inner_text()) for h in header_cells]
    idx = {h.lower(): i for i, h in enumerate(headers)}

    def col(name: str) -> int:
        name = name.lower()
        if name in idx:
            return idx[name]
        for k, v in idx.items():
            if k.startswith(name):
                return v
        raise KeyError(f"Missing column: {name!r} in {headers!r}")

    lang_i   = col("language")
    vname_i  = col("voice name")
    gender_i = col("ssml gender")

    if table.query_selector("tbody"):
        trs = table.query_selector("tbody").query_selector_all("tr")
    else:
        all_trs = table.query_selector_all("tr")
        trs = all_trs[1:] if used_first_row_as_header and len(all_trs) > 1 else all_trs

    out: List[Tuple[str, str, str]] = []
    for tr in trs:
        tds = tr.query_selector_all("td")
        if not tds: 
            continue
        mx = max(lang_i, vname_i, gender_i)
        if len(tds) <= mx:
            continue
        language = _norm(tds[lang_i].inner_text())
        voice    = _norm(tds[vname_i].inner_text())
        gender   = _norm(tds[gender_i].inner_text())
        if language and voice and gender:
            out.append((language, voice, gender))
    return out

def scrape() -> List[Tuple[str, str, str]]:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            context = browser.new_context()
            page = context.new_page()
            page.goto(URL, wait_until="networkidle", timeout=120_000)
            time.sleep(2)  # hydration grace

            table = _find_table(page)
            if not table:
                time.sleep(3)
                table = _find_table(page)
            if not table:
                raise RuntimeError("Could not find voices table with Language / Voice name / SSML Gender headers.")
            rows = _extract_rows(table)
            if not rows:
                raise RuntimeError("Found table but extracted 0 rows.")
            return rows
        finally:
            browser.close()

def update_db_from_csv(csv_path: str) -> None:
    """
    Transactional refresh:
      - Create a temp staging table
      - COPY CSV into staging
      - DELETE FROM main + INSERT FROM staging
      - Commit
    On any error: raise; caller decides whether to keep the temp CSV.
    """
    ddl = """
    CREATE TABLE IF NOT EXISTS google_tts_voices (
        id SERIAL PRIMARY KEY,
        language CITEXT NOT NULL,
        voice_name VARCHAR(128) NOT NULL,
        gender VARCHAR(16) NOT NULL,
        UNIQUE (voice_name)
    );
    """
    with closing(psycopg2.connect(**PSQL_CREDENTIALS)) as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute(ddl)
            cur.execute("""CREATE INDEX IF NOT EXISTS google_tts_voices_lang_idx
                        ON google_tts_voices (language)""")
            cur.execute("""CREATE INDEX IF NOT EXISTS google_tts_voices_gender_idx
                        ON google_tts_voices (gender)""")
            cur.execute("DROP TABLE IF EXISTS google_tts_voices_staging")
            cur.execute("""
                CREATE TEMP TABLE google_tts_voices_staging (
                    language CITEXT NOT NULL,
                    voice_name VARCHAR(128) NOT NULL,
                    gender VARCHAR(16) NOT NULL
                ) ON COMMIT DROP
            """)
            with open(csv_path, "r", encoding="utf-8", newline="") as f:
                cur.copy_expert(
                    "COPY google_tts_voices_staging (language, voice_name, gender) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)",
                    f,
                )
            # Replace
            cur.execute("DELETE FROM google_tts_voices")
            cur.execute("""
                INSERT INTO google_tts_voices (language, voice_name, gender)
                SELECT language, voice_name, gender
                FROM google_tts_voices_staging
            """)
        conn.commit()

def main():
    # 1) Scrape
    try:
        rows = scrape()
    except PlaywrightTimeoutError as e:
        print(f"[ERROR] Timed out: {e}", file=sys.stderr)
        sys.exit(2)
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(1)

    # 2) Write to temp CSV
    tmp = tempfile.NamedTemporaryFile(delete=False, prefix="google_tts_voices_", suffix=".csv")
    tmp_path = tmp.name
    tmp.close()
    try:
        with open(tmp_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["Language", "Voice Name", "Gender"])
            w.writerows(rows)

        # 3) Try DB update
        try:
            update_db_from_csv(tmp_path)
        except Exception as e:
            print(f"[ERROR] DB update failed: {e}", file=sys.stderr)
            print(f"Left temp CSV for troubleshooting: {tmp_path}")
            sys.exit(3)

        # 4) Success → remove temp CSV
        os.remove(tmp_path)
        print(f"Updated google_tts_voices with {len(rows)} voices.")
    except Exception as e:
        # If writing CSV itself failed (unlikely), just surface it.
        print(f"[ERROR] Failed during CSV write/update: {e}", file=sys.stderr)
        # Do not remove tmp_path here: it may not even exist or be partial.
        sys.exit(4)

if __name__ == "__main__":
    main()
    