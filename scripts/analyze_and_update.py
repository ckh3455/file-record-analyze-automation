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
    if LOG_DIR.exists() and not LOG_DIR.is_dir():
        try: LOG_DIR.unlink()
        except Exception: pass
    if not LOG_DIR.exists():
        LOG_DIR.mkdir(parents=True, exist_ok=True)

_ensure_logdir()
RUN_LOG = LOG_DIR / f"run-{datetime.now(ZoneInfo('Asia/Seoul')).strftime('%Y%m%dT%H%M%S%z')}.log"
LATEST = LOG_DIR / "latest.log"
WRITTEN = LOG_DIR / "where_written.txt"

def _t(): return datetime.now(ZoneInfo("Asia/Seoul")).strftime("[%H:%M:%S]")
def _w(line: str):
    print(line)
    try:
        with RUN_LOG.open("a", encoding="utf-8") as f: f.write(line+"\n")
        with LATEST.open("a", encoding="utf-8") as f: f.write(line+"\n")
    except Exception: pass
def log(msg: str): _w(f"{_t()} {msg}")
def log_error(msg: str, exc: Optional[BaseException]=None):
    _w(f"{_t()} [ERROR] {msg}")
    if exc:
        import traceback
        tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        print(tb, file=sys.stderr)
        try:
            with RUN_LOG.open("a", encoding="utf-8") as f: f.write(tb+"\n")
            with LATEST.open("a", encoding="utf-8") as f: f.write(tb+"\n")
        except Exception: pass
def note_written(s: str):
    try: 
        with WRITTEN.open("a", encoding="utf-8") as f: f.write(s.rstrip()+"\n")
    except Exception: pass

# ---------------- Normalize helpers ----------------
def norm_raw(s: str) -> str:
    if s is None: return ""
    return str(s).replace("\u00A0","").replace(" ","").strip()

# 광역명 → 시트 열 라벨(사용자 요구 형식) 매핑
PROV_TO_SHEET = {
    "서울특별시": "서울",
    "세종특별자치시": "세종시",
    "강원특별자치도": "강원도",
    "경기도": "경기도",
    "인천광역시": "인천광역시",
    "부산광역시": "부산",
    "대구광역시": "대구",
    "광주광역시": "광주",
    "대전광역시": "대전",
    "울산광역시": "울산",
    "전라남도": "전남",
    "전북특별자치도": "전북",
    "경상남도": "경남",
    "경상북도": "경북",
    "충청남도": "충남",
    "충청북도": "충북",
    "제주특별자치도": "제주",
}

# 시트 헤더 정규화 시에도 동일한 라벨로 인식되도록 별칭 제작
SHEET_ALIASES = {
    "서울특별시":"서울", "서울":"서울",
    "세종특별자치시":"세종시","세종시":"세종시",
    "강원특별자치도":"강원도","강원도":"강원도",
    "전북특별자치도":"전북","전북":"전북",
    "전라남도":"전남","전남":"전남",
    "경상남도":"경남","경남":"경남",
    "경상북도":"경북","경북":"경북",
    "충청남도":"충남","충남":"충남",
    "충청북도":"충북","충북":"충북",
    "제주특별자치도":"제주","제주":"제주",
    "부산광역시":"부산","부산":"부산",
    "대구광역시":"대구","대구":"대구",
    "광주광역시":"광주","광주":"광주",
    "대전광역시":"대전","대전":"대전",
    "울산광역시":"울산","울산":"울산",
    "경기도":"경기도",
    "인천광역시":"인천광역시",
}

def sheet_label(s: str) -> str:
    return SHEET_ALIASES.get(s, s)

def canonize_gu_name(s: str) -> str:
    return str(s or "").strip()

# ---------------- Filename / Tab parsing ----------------
FN_RE = re.compile(r".*?(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})\.xlsx$")

def parse_filename(fname: str) -> Tuple[int,int,int]:
    m = FN_RE.match(fname)
    if not m:
        raise ValueError(f"unexpected filename: {fname}")
    y = 2000 + int(m.group(1))
    mth = int(m.group(2))
    day = int(m.group(5))
    return y, mth, day

def kdate_str(d: date) -> str:
    return f"{d.year}. {d.month}. {d.day}"

def parse_any_date(s: str) -> Optional[date]:
    if s is None: return None
    s = str(s).strip()
    if not s: return None
    for fmt in ("%Y-%m-%d", "%Y.%m.%d", "%Y.%m. %d", "%Y. %m. %d"):
        try: return datetime.strptime(s, fmt).date()
        except Exception: pass
    m = re.match(r"^\s*(\d{4})\.\s*(\d{1,2})\.\s*(\d{1,2})\s*$", s)
    if m:
        y,mn,dd = map(int, m.groups())
        return date(y,mn,dd)
    return None

# ---------------- Data read & agg ----------------
def read_molit_xlsx(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name="data", dtype=str, engine="openpyxl")
    df = df.fillna("")
    for c in ["광역","구","계약년","계약월","계약일"]:
        if c not in df.columns:
            raise ValueError(f"missing column '{c}' in {path.name}")
    # 숫자 정리
    for c in ["계약년","계약월","계약일"]:
        df[c] = df[c].astype(str).str.replace(r"\D","",regex=True)
    return df

def agg_national(df: pd.DataFrame) -> Dict[str,int]:
    # 광역 → 시트 라벨로 변환 후 개수
    prov_counts: Dict[str,int] = {}
    for v in df["광역"]:
        label = sheet_label(PROV_TO_SHEET.get(str(v), str(v)))
        prov_counts[label] = prov_counts.get(label, 0) + 1
    return prov_counts

def agg_seoul(df: pd.DataFrame) -> Dict[str,int]:
    se = df[df["광역"]=="서울특별시"]
    counts: Dict[str,int] = {}
    for v in se["구"]:
        g = canonize_gu_name(v)
        if not g: continue
        counts[g] = counts.get(g, 0) + 1
    # 시트에 '서울' 열이 있는 경우 대비(총합계는 따로 계산하지만, 일부 시트에 '서울'이 있을 수 있음)
    counts["서울"] = len(se)
    return counts

# ---------------- Sheet access ----------------
def find_ws(sh, title: str) -> Optional[gspread.Worksheet]:
    want = title.replace(" ","")
    for ws in sh.worksheets():
        if ws.title.replace(" ","") == want:
            return ws
    return None

def find_date_col_idx(ws: gspread.Worksheet) -> int:
    header = ws.row_values(1)
    for i, v in enumerate(header, start=1):
        if str(v).strip() == "날짜":
            return i
    return 1

def find_or_create_row_for_date(ws: gspread.Worksheet, target: date) -> int:
    date_col = find_date_col_idx(ws)
    vals = ws.col_values(date_col)
    # 기존 날짜 찾기
    for i in range(2, len(vals)+1):
        s = vals[i-1]
        if not s: continue
        d = parse_any_date(s)
        if d == target:
            return i
    # 없으면 마지막 사용행 다음에 추가
    used = 1
    for i in range(len(vals), 1, -1):
        if str(vals[i-1]).strip():
            used = i
            break
    return used + 1

def build_row(header: List[str], day: date, series: Dict[str,int]) -> Tuple[List, int]:
    """header 순서에 맞추어 값 채움. 총합계는 채운 값들의 합으로."""
    row: List = []
    total = 0
    for i, h in enumerate(header):
        h = str(h).strip()
        if i == 0:
            row.append(kdate_str(day))
            continue
        if not h:
            row.append("")
            continue
        # 헤더 라벨 정규화(광역 별칭 적용)
        key = sheet_label(h)
        if key == "총합계":
            # 총합계는 뒤에서 한 번에 채우므로 임시 0
            row.append(0)
            continue
        val = series.get(key)
        if val is None:
            # 헤더가 '서울특별시'처럼 길게 있는 케이스도 보정
            val = series.get(sheet_label(key))
        if val is None:
            row.append(0)
        else:
            row.append(int(val))
            total += int(val)
    # 총합계 자리 보정
    for i, h in enumerate(header):
        if str(h).strip() == "총합계":
            row[i] = total
            break
    return row, total

def upsert_row(ws: gspread.Worksheet, day: date, series: Dict[str,int]) -> Tuple[str,int]:
    header = ws.row_values(1)
    if not header:
        raise RuntimeError(f"empty header in sheet '{ws.title}'")
    row_idx = find_or_create_row_for_date(ws, day)
    row, _ = build_row(header, day, series)
    # 범위 계산
    last_col = len(header)
    a1_end = gspread.utils.rowcol_to_a1(1, last_col)  # like 'Z1'
    last_letter = re.sub(r"\d+","", a1_end)
    rng = f"A{row_idx}:{last_letter}{row_idx}"
    mode = "update" if row_idx <= ws.row_count else "append"
    ws.update([row], rng, value_input_option="USER_ENTERED")
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
    except Exception: pass

    log("[MAIN]")
    art = Path(args.artifacts_dir)
    log(f"artifacts_dir={art}")
    xlsx_paths: List[Path] = sorted(art.rglob("전국 *.xlsx"))
    log(f"national files count= {len(xlsx_paths)}")

    # gspread 인증
    log("[gspread] auth with SA_JSON (env)")
    sa_raw = os.environ.get("SA_JSON","").strip()
    if not sa_raw:
        raise RuntimeError("SA_JSON is empty")
    creds = Credentials.from_service_account_info(json.loads(sa_raw),
              scopes=["https://www.googleapis.com/auth/spreadsheets",
                      "https://www.googleapis.com/auth/drive"])
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(args.sheet_id)
    log("[gspread] spreadsheet opened")

    # 오늘(KST) 날짜
    today_kst = datetime.now(ZoneInfo("Asia/Seoul")).date()
    log(f"[date] {kdate_str(today_kst)}")

    for p in xlsx_paths:
        try:
            y, m, file_day = parse_filename(p.name)
        except Exception as e:
            log_error(f"filename parse failed: {p.name}", e); continue

        nat_tab = f"전국 {y%100}년 {m}월"
        se_tab  = f"서울 {y%100}년 {m}월"
        log(f"[file] {p.name} -> nat='{nat_tab}' seoul='{se_tab}' (file_day={file_day})")

        try:
            df = read_molit_xlsx(p)
        except Exception as e:
            log_error(f"read error: {p}", e); continue

        # 집계
        nat_series = agg_national(df)  # ex) {'서울': 123, '경기도': 456, ...}
        se_series  = agg_seoul(df)     # ex) {'강남구':10, ..., '서울':총합(보조)}

        # 탭 찾기
        ws_nat = find_ws(sh, nat_tab)
        ws_se  = find_ws(sh, se_tab)
        if not ws_nat: log(f"[전국] sheet not found: '{nat_tab}' (skip)")
        if not ws_se:  log(f"[서울] sheet not found: '{se_tab}' (skip)")

        if ws_nat and nat_series:
            mode, row = upsert_row(ws_nat, today_kst, nat_series)
            log(f"[전국] {ws_nat.title} -> {kdate_str(today_kst)} {mode} row={row}")
            note_written(f"{ws_nat.title}\t{kdate_str(today_kst)}\t{mode}\t{row}")

        if ws_se and se_series:
            # ‘서울’ 키는 총합계 계산에만 쓰이고, 개별 구 열에 채움
            # build_row에서 총합계 자동 계산하므로 그대로 전달
            mode, row = upsert_row(ws_se, today_kst, se_series)
            log(f"[서울] {ws_se.title} -> {kdate_str(today_kst)} {mode} row={row}")
            note_written(f"{ws_se.title}\t{kdate_str(today_kst)}\t{mode}\t{row}")

    log("[main] logs written to analyze_report/")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log_error(str(e), e)
        sys.exit(1)
