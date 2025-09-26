# -*- coding: utf-8 -*-
from __future__ import annotations
import os, re, sys, json
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# ---------------- Logging ----------------
LOG_DIR = Path("analyze_report")

def _ensure_logdir():
    # analyze_report가 '파일'이면 삭제 후 폴더 생성
    if LOG_DIR.exists() and not LOG_DIR.is_dir():
        try: LOG_DIR.unlink()
        except Exception: pass
    if not LOG_DIR.exists():
        LOG_DIR.mkdir(parents=True, exist_ok=True)

_ensure_logdir()

RUN_LOG = LOG_DIR / f"run-{datetime.now(ZoneInfo('Asia/Seoul')).strftime('%Y%m%dT%H%M%S%z')}.log"
LATEST = LOG_DIR / "latest.log"
WRITTEN = LOG_DIR / "where_written.txt"

def _t():
    return datetime.now(ZoneInfo("Asia/Seoul")).strftime("[%H:%M:%S]")

def _w(line: str):
    print(line)
    try:
        with RUN_LOG.open("a", encoding="utf-8") as f: f.write(line+"\n")
        with LATEST.open("a", encoding="utf-8") as f: f.write(line+"\n")
    except Exception:
        pass

def log(msg: str):
    _w(f"{_t()} {msg}")

def log_error(msg: str, exc: Optional[BaseException]=None):
    _w(f"{_t()} [ERROR] {msg}")
    if exc:
        import traceback
        tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        print(tb, file=sys.stderr)
        try:
            with RUN_LOG.open("a", encoding="utf-8") as f: f.write(tb+"\n")
            with LATEST.open("a", encoding="utf-8") as f: f.write(tb+"\n")
        except Exception:
            pass

def note_written(s: str):
    try:
        with WRITTEN.open("a", encoding="utf-8") as f:
            f.write(s.rstrip()+"\n")
    except Exception:
        pass

# ---------------- Helpers ----------------

# 파일명 예: "전국 2506_250926.xlsx" -> (y=2025, m=6, day=26)
FN_RE = re.compile(r".*?(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})\.xlsx$")

def parse_filename(fname: str) -> Tuple[int,int,int]:
    m = FN_RE.match(fname)
    if not m:
        raise ValueError(f"unexpected filename: {fname}")
    y = 2000 + int(m.group(1))
    mth = int(m.group(2))
    day = int(m.group(5))
    return y, mth, day

# 탭명(전국/서울 25년 6월 등)에서 (yy, m)
TAB_RE = re.compile(r"(?:전국|서울)\s*(\d{2})년\s*(\d{1,2})월")
def parse_tab_ym(title: str) -> Optional[Tuple[int,int]]:
    t = title.replace(" ", "")
    m = TAB_RE.match(t)
    if not m: return None
    y = 2000 + int(m.group(1))
    mth = int(m.group(2))
    return y, mth

def norm(s: str) -> str:
    return (s or "").strip().replace(" ", "").replace("\u00A0","")

# ‘YYYY. M. D’ 문자열
def kdate_str(d: date) -> str:
    return f"{d.year}. {d.month}. {d.day}"

def parse_any_date(s: str) -> Optional[date]:
    if s is None: return None
    s = str(s).strip()
    if not s: return None
    # 주로 사용하는 포맷들
    for fmt in ("%Y-%m-%d", "%Y.%m.%d", "%Y.%m. %d", "%Y. %m. %d"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    m = re.match(r"^\s*(\d{4})\.\s*(\d{1,2})\.\s*(\d{1,2})\s*$", s)
    if m:
        y,mn,dd = map(int, m.groups())
        return date(y,mn,dd)
    return None

# 날짜열에서 target 날짜의 행 찾기(헤더=1행)
def find_date_row(ws, target: date, date_col_idx: int = 1, header_row: int = 1) -> Optional[int]:
    rng = ws.range(header_row+1, date_col_idx, ws.row_count, date_col_idx)
    for off, cell in enumerate(rng, start=header_row+1):
        v = str(cell.value).strip()
        if not v: continue
        d = parse_any_date(v)
        if d and d == target:
            return off
    return None

def find_date_col_idx(ws) -> int:
    header = ws.row_values(1)
    for i, v in enumerate(header, start=1):
        if str(v).strip() == "날짜":
            return i
    return 1

# ---------------- Data read & agg ----------------

def read_molit_xlsx(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name="data", dtype=str, engine="openpyxl")
    df = df.fillna("")
    must = ["광역","구","계약년","계약월","계약일"]
    missing = [c for c in must if c not in df.columns]
    if missing:
        raise ValueError(f"missing columns: {missing}")
    # 숫자만 남기기
    for c in ["계약년","계약월","계약일"]:
        df[c] = df[c].astype(str).str.replace(r"\D","",regex=True)
    return df

def agg_national(df: pd.DataFrame) -> Dict[str,int]:
    s = df.groupby("광역").size().astype(int)
    return dict(s)

def agg_seoul(df: pd.DataFrame) -> Dict[str,int]:
    se = df[df["광역"]=="서울특별시"]
    if se.empty: return {}
    s = se.groupby("구").size().astype(int)
    return dict(s)

# ---------------- Sheet access ----------------

def fuzzy_find_sheet(sh, want_title: str):
    want_n = norm(want_title)
    for ws in sh.worksheets():
        if norm(ws.title) == want_n:
            return ws
    # 공백/전각 차이 등 느슨 매칭
    for ws in sh.worksheets():
        if norm(ws.title) == norm(want_title.replace(" ", "")):
            return ws
    return None

def build_row_by_header(header: List[str], day: date, series: Dict[str,int], kind: str) -> List:
    row = []
    total = 0
    alias_map = {
        "강원도": "강원도",
        "강원특별자치도": "강원특별자치도",
        "전라북도": "전라북도",
        "전북특별자치도": "전북특별자치도",
        "세종특별자치시": "세종특별자치시",
        "제주특별자치도": "제주특별자치도",
        "총합계": "__SUM__",
        "전체 개수": "__SUM__",
        "합계": "__SUM__",
    }
    for i, h in enumerate(header):
        h = str(h).strip()
        if i == 0:
            row.append(kdate_str(day))
            continue
        if not h:
            row.append("")
            continue
        key = h
        val = series.get(key)
        if val is None and key in alias_map and alias_map[key] != "__SUM__":
            val = series.get(alias_map[key])
        if isinstance(val, int):
            row.append(val); total += val
        else:
            if alias_map.get(key) == "__SUM__":
                row.append(total)
            else:
                row.append("")
    return row

def upsert_row(ws, day: date, series: Dict[str,int], kind: str) -> Tuple[str,int]:
    header = ws.row_values(1)
    if not header:
        raise RuntimeError(f"empty header in sheet '{ws.title}'")
    date_col = find_date_col_idx(ws)
    row_idx = find_date_row(ws, day, date_col_idx=date_col, header_row=1)
    mode = "update" if row_idx else "append"
    if not row_idx:
        # 날짜열의 마지막 사용행 뒤에 추가
        col_vals = ws.col_values(date_col)
        used = 1
        for i in range(len(col_vals), 1, -1):
            if str(col_vals[i-1]).strip():
                used = i
                break
        row_idx = used + 1

    out = build_row_by_header(header, day, series, kind)
    last_a1 = gspread.utils.rowcol_to_a1(1, len(header))  # e.g., Z1
    last_col_letter = re.sub(r"\d+","", last_a1)          # Z
    rng = f"A{row_idx}:{last_col_letter}{row_idx}"
    ws.update([out], rng)
    return mode, row_idx

# ---------------- Main ----------------

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--artifacts-dir", required=True)
    ap.add_argument("--sheet-id", required=True)
    args = ap.parse_args()

    # latest.log 초기화
    try:
        if LATEST.exists():
            LATEST.unlink()
    except Exception:
        pass

    log("[MAIN]")
    log("[COLLECT]")
    art = Path(args.artifacts_dir)
    log(f"artifacts_dir={art}")
    xlsx_paths: List[Path] = sorted(art.rglob("*.xlsx"))
    log(f"zip files found: 0")
    log(f"total xlsx under work_dir: {len(xlsx_paths)}")

    nat_files = [p for p in xlsx_paths if p.name.startswith("전국 ")]
    log(f"national files count= {len(nat_files)}")

    # gspread 인증
    log("[gspread] auth with sa.json")
    sa_raw = os.environ.get("SA_JSON","").strip()
    if not sa_raw:
        raise RuntimeError("SA_JSON is empty")
    creds = Credentials.from_service_account_info(json.loads(sa_raw),
              scopes=["https://www.googleapis.com/auth/spreadsheets",
                      "https://www.googleapis.com/auth/drive"])
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(args.sheet_id)
    log("[gspread] spreadsheet opened")

    # ✅ 오늘 날짜(한국시간)로 고정 사용
    today_kst = datetime.now(ZoneInfo("Asia/Seoul")).date()
    log(f"[date] using today (KST) = {kdate_str(today_kst)}")

    for x in nat_files:
        try:
            y, m, file_day = parse_filename(x.name)
        except Exception as e:
            log_error(f"filename parse failed: {x.name}", e); continue

        nat_tab_title = f"전국 {y%100}년 {m}월"
        se_tab_title  = f"서울 {y%100}년 {m}월"
        log(f"[file] {x.name} -> nat='{nat_tab_title}' seoul='{se_tab_title}' (file_day={file_day})")

        # 엑셀 로드
        try:
            df = read_molit_xlsx(x)
        except Exception as e:
            log_error(f"read error: {x}", e); continue

        log(f"[read] rows={len(df)} cols={len(df.columns)}")

        # 집계
        nat_series = agg_national(df)
        se_series = agg_seoul(df)

        # 탭 찾기(느슨 매칭 허용)
        ws_nat = fuzzy_find_sheet(sh, nat_tab_title)
        ws_se  = fuzzy_find_sheet(sh, se_tab_title)

        if not ws_nat:
            log(f"[전국] sheet not found: '{nat_tab_title}' (skip)")
        if not ws_se:
            log(f"[서울] sheet not found: '{se_tab_title}' (skip)")

        # ✅ 기록 대상 날짜: 무조건 오늘
        target_date = today_kst

        # 전국 쓰기
        if ws_nat and nat_series:
            mode, row = upsert_row(ws_nat, target_date, nat_series, kind="national")
            log(f"[전국] {ws_nat.title} -> {kdate_str(target_date)} {mode} row={row}")
            note_written(f"{ws_nat.title}\t{kdate_str(target_date)}\t{mode}\t{row}")
        elif ws_nat:
            log(f"[전국] {ws_nat.title} -> no rows (empty agg)")

        # 서울 쓰기
        if ws_se and se_series:
            mode, row = upsert_row(ws_se, target_date, se_series, kind="seoul")
            log(f"[서울] {ws_se.title} -> {kdate_str(target_date)} {mode} row={row}")
            note_written(f"{ws_se.title}\t{kdate_str(target_date)}\t{mode}\t{row}")
        elif ws_se:
            log(f"[서울] {ws_se.title} -> no rows (empty agg)")

    log("[main] logs written to analyze_report/")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log_error(str(e), e)
        sys.exit(1)
