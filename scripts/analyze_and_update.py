# scripts/analyze_and_update.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os, re, sys, json, time, random
from pathlib import Path
from datetime import datetime, date
from typing import Dict, List, Tuple, Optional

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

# ===================== 기본 설정 =====================
LOG_DIR = Path("analyze_report")
WORK_DIR_DEFAULT = os.environ.get("ARTIFACTS_DIR", "artifacts")
SHEET_NAME_DATA = "data"

SEOUL_GU = [
    "강남구","강동구","강북구","강서구","관악구","광진구","구로구","금천구",
    "노원구","도봉구","동대문구","동작구","마포구","서대문구","서초구",
    "성동구","성북구","송파구","양천구","영등포구","용산구","은평구",
    "종로구","중구","중랑구"
]

# --------- 광역 정규화(모든 변형→공식 명칭) ----------
def _ns(s: str) -> str:
    return "" if s is None else str(s).replace("\u00A0","").replace(" ", "").strip()

# 공식 명칭 세트 (전국 탭 헤더가 이 형태)
PROV_OFFICIAL = {
    "서울특별시","부산광역시","대구광역시","인천광역시","광주광역시","대전광역시","울산광역시",
    "세종특별자치시","경기도","강원특별자치도","충청북도","충청남도","전라북도","전북특별자치도",
    "전라남도","경상북도","경상남도","제주특별자치도"
}

# 별칭→공식명칭 맵
PROV_CANON_MAP: Dict[str,str] = {}
def _add_alias(alias: str, official: str):
    PROV_CANON_MAP[_ns(alias)] = official

# 기본 매핑(줄임표기 포함)
_add_alias("서울", "서울특별시")
_add_alias("서울특별시", "서울특별시")

_add_alias("부산", "부산광역시"); _add_alias("부산광역시", "부산광역시")
_add_alias("대구", "대구광역시"); _add_alias("대구광역시", "대구광역시")
_add_alias("인천", "인천광역시"); _add_alias("인천광역시", "인천광역시")
_add_alias("광주", "광주광역시"); _add_alias("광주광역시", "광주광역시")
_add_alias("대전", "대전광역시"); _add_alias("대전광역시", "대전광역시")
_add_alias("울산", "울산광역시"); _add_alias("울산광역시", "울산광역시")

_add_alias("세종", "세종특별자치시"); _add_alias("세종특별자치시", "세종특별자치시")

_add_alias("경기도", "경기도")

_add_alias("강원도", "강원특별자치도")
_add_alias("강원특별자치도", "강원특별자치도")

_add_alias("충북", "충청북도"); _add_alias("충청북도", "충청북도")
_add_alias("충남", "충청남도"); _add_alias("충청남도", "충청남도")

_add_alias("전북", "전북특별자치도")
_add_alias("전라북도", "전북특별자치도")
_add_alias("전북특별자치도", "전북특별자치도")

_add_alias("전남", "전라남도"); _add_alias("전라남도", "전라남도")

_add_alias("경북", "경상북도"); _add_alias("경상북도", "경상북도")
_add_alias("경남", "경상남도"); _add_alias("경상남도", "경상남도")

_add_alias("제주", "제주특별자치도")
_add_alias("제주특별자치도", "제주특별자치도")

def canonicalize_province(x: str) -> str:
    n = _ns(x)
    # 이미 공식명칭이면 그대로
    for off in PROV_OFFICIAL:
        if _ns(off) == n:
            return off
    # 별칭 매핑
    if n in PROV_CANON_MAP:
        return PROV_CANON_MAP[n]
    # 모를 땐 원문(공백 제거 전 원문 유지가 나음)
    return x

# ---------------- 로깅 ----------------
def _ensure_logdir():
    if LOG_DIR.exists():
        if LOG_DIR.is_file():
            LOG_DIR.unlink()
            LOG_DIR.mkdir(parents=True, exist_ok=True)
    else:
        LOG_DIR.mkdir(parents=True, exist_ok=True)

_ensure_logdir()
RUNLOG = LOG_DIR / "latest.log"
WRITTEN = LOG_DIR / "where_written.txt"

def log(msg: str):
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    print(line)
    try:
        with RUNLOG.open("a", encoding="utf-8") as f: f.write(line+"\n")
    except Exception:
        pass

def note_written(sheet_title: str, sheet_id: int, dst_range: str):
    try:
        with WRITTEN.open("a", encoding="utf-8") as f:
            f.write(f"{sheet_title}\t(id={sheet_id})\t{dst_range}\n")
    except Exception:
        pass

# ---------------- gspread 도우미 ----------------
_LAST_TS = 0.0
def _throttle(sec=0.45):
    global _LAST_TS
    now = time.time()
    if now - _LAST_TS < sec:
        time.sleep(sec - (now - _LAST_TS))
    _LAST_TS = time.time()

def _retry(fn, *a, **kw):
    base = 0.8
    for i in range(6):
        try:
            _throttle()
            return fn(*a, **kw)
        except APIError as e:
            s = str(e)
            if any(x in s for x in ("429","500","502","503")):
                time.sleep(base*(2**i) + random.uniform(0,0.25))
                continue
            raise

def _find_ws_exact(st: gspread.Spreadsheet, title: str) -> Optional[gspread.Worksheet]:
    for ws in st.worksheets():
        if ws.title == title:
            log(f"[ws] matched (exact): '{ws.title}'")
            return ws
    return None

def _find_ws_nospace(st: gspread.Spreadsheet, title: str) -> Optional[gspread.Worksheet]:
    want = _ns(title)
    for ws in st.worksheets():
        if _ns(ws.title) == want:
            log(f"[ws] matched (nospace): '{ws.title}' (wanted='{title}')")
            return ws
    return None

def find_ws_multi(st: gspread.Spreadsheet, candidates: List[str]) -> Optional[gspread.Worksheet]:
    for t in candidates:
        ws = _find_ws_exact(st, t)
        if ws: return ws
    for t in candidates:
        ws = _find_ws_nospace(st, t)
        if ws: return ws
    return None

def a1_col(idx: int) -> str:
    s = ""
    n = idx
    while n>0:
        n, r = divmod(n-1, 26)
        s = chr(65+r) + s
    return s

# ---------------- 파일 읽기 & 집계 ----------------
def read_month_df(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=SHEET_NAME_DATA, dtype=str)
    df = df.fillna("")
    for c in ["계약년","계약월","계약일","거래금액(만원)"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def agg_national(df: pd.DataFrame) -> Dict[str,int]:
    if "광역" not in df.columns:
        return {}
    canon = df["광역"].map(canonicalize_province)
    s = canon.groupby(canon).size().astype(int)
    out = dict(s)
    log(f"[agg/national] sample={sorted(out.items(), key=lambda x:-x[1])[:10]}")
    return out

def agg_seoul(df: pd.DataFrame) -> Dict[str,int]:
    if "광역" not in df.columns or "구" not in df.columns:
        return {}
    se = df[df["광역"].map(canonicalize_province) == "서울특별시"]
    s = se["구"].groupby(se["구"]).size().astype(int)
    out = {g:int(s.get(g,0)) for g in SEOUL_GU}
    log(f"[agg/seoul] nowon_present={'노원구' in s.index} value={int(s.get('노원구',0))}")
    return out

# ---------------- 월별 탭 쓰기 (행 전체 한 번에 업데이트) ----------------
def find_or_append_date_row(ws: gspread.Worksheet, date_label: str) -> int:
    vals = _retry(ws.get_all_values)
    if not vals:
        return 2
    for i, row in enumerate(vals[1:], start=2):
        if (len(row)>0) and str(row[0]).strip()==date_label:
            return i
    return len(vals)+1

def build_full_row(header: List[str], date_label: str, values_by_colname: Dict[str,int]) -> List:
    row = []
    total = 0
    for i, h in enumerate(header):
        if i == 0:
            row.append(date_label); continue
        name = str(h).strip()
        if not name:
            row.append(""); continue
        if name in ("총합계","합계"):
            row.append(total); continue
        v = int(values_by_colname.get(name, 0))
        row.append(v)
        total += v
    return row

def write_month_sheet(ws: gspread.Worksheet, date_label: str, header: List[str], values_by_colname: Dict[str,int]):
    row_idx = find_or_append_date_row(ws, date_label)
    full_row = build_full_row(header, date_label, values_by_colname)
    end_col = a1_col(len(header))
    _retry(ws.update, [full_row], f"A{row_idx}:{end_col}{row_idx}")
    note_written(ws.title, ws.id, f"A{row_idx}:{end_col}{row_idx}")
    log(f"[ws] {ws.title} -> {date_label} row={row_idx}")

# ---------------- 메인 ----------------
def main():
    # 로그 초기화
    try:
        RUNLOG.write_text("", encoding="utf-8")
        WRITTEN.write_text("", encoding="utf-8")
    except Exception:
        pass

    log("[MAIN]")
    art_dir = WORK_DIR_DEFAULT
    log(f"artifacts_dir={art_dir}")

    # gspread 인증
    sa_raw = os.environ.get("SA_JSON","").strip()
    sa_path = os.environ.get("SA_PATH","sa.json")
    if sa_raw:
        creds = Credentials.from_service_account_info(
            json.loads(sa_raw),
            scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"],
        )
    else:
        if not Path(sa_path).exists():
            raise RuntimeError("service account not provided")
        creds = Credentials.from_service_account_file(
            sa_path,
            scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"],
        )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(os.environ.get("SHEET_ID","").strip())
    log("[gspread] spreadsheet opened")

    today = datetime.now().date()
    date_label = f"{today.year}. {today.month}. {today.day}"
    paths = sorted(Path(art_dir).rglob("전국 *.xlsx"))
    log(f"[collect] found {len(paths)} xlsx files")

    for p in paths:
        m = re.search(r"(\d{2})(\d{2})_", p.name)
        if not m:
            log(f"[file] skip: {p.name}")
            continue
        yy = int(m.group(1)); mm = int(m.group(2))
        nat_candidates = [f"전국 {yy}년 {mm}월", f"전국 {2000+yy}년 {mm}월"]
        se_candidates  = [f"서울 {yy}년 {mm}월", f"서울 {2000+yy}년 {mm}월"]
        log(f"[file] {p.name} -> nat candidates={nat_candidates} / seoul candidates={se_candidates}")

        df = read_month_df(p)
        log(f"[read] rows={len(df)} cols={len(df.columns)}")

        nat_counts_canon = agg_national(df)      # 키: 공식명칭
        se_counts = agg_seoul(df)                # 25개 구

        # 전국 탭
        ws_nat = find_ws_multi(sh, nat_candidates)
        if ws_nat:
            header_nat = _retry(ws_nat.row_values, 1)
            nat_row: Dict[str,int] = {}
            # 헤더를 공식명칭으로 정규화해서 그대로 집계값을 매칭
            for h in header_nat:
                if not h or h in ("날짜","총합계","합계"): continue
                canon_h = canonicalize_province(h)
                nat_row[h] = int(nat_counts_canon.get(canon_h, 0))
            write_month_sheet(ws_nat, date_label, header_nat, nat_row)
        else:
            log(f"[전국] sheet not found: tried {nat_candidates} (skip)")

        # 서울 탭
        ws_se = find_ws_multi(sh, se_candidates)
        if ws_se:
            header_se = _retry(ws_se.row_values, 1)
            se_row: Dict[str,int] = {}
            for h in header_se:
                if not h or h in ("날짜","총합계","합계"): continue
                se_row[h] = int(se_counts.get(h, 0))
            write_month_sheet(ws_se, date_label, header_se, se_row)
        else:
            log(f"[서울] sheet not found: tried {se_candidates} (skip)")

    log("[main] logs written to analyze_report/")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"[ERROR] {repr(e)}")
        raise
