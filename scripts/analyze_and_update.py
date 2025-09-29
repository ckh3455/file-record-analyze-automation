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

# 엑셀 '광역' → 전국 탭 열명(축약형) 매핑
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
        with RUNLOG.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass

def note_written(sheet_title: str, sheet_id: int, dst_range: str):
    try:
        with WRITTEN.open("a", encoding="utf-8") as f:
            f.write(f"{sheet_title}\t(id={sheet_id})\t{dst_range}\n")
    except Exception:
        pass

# ---------------- 공통 유틸 ----------------
def nospace(s: str) -> str:
    return "" if s is None else str(s).replace("\u00A0","").replace(" ", "").strip()

def kdate_str(d: date) -> str:
    return f"{d.year}. {d.month}. {d.day}"

def parse_filename(fname: str) -> Tuple[int,int,int]:
    m = re.search(r"(\d{2})(\d{2})_", fname)
    if not m:
        raise ValueError(f"unexpected filename: {fname}")
    yy = int(m.group(1)); mm = int(m.group(2))
    return 2000 + yy, mm, yy

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
    want = nospace(title)
    for ws in st.worksheets():
        if nospace(ws.title) == want:
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
    ser_mapped = df["광역"].map(lambda x: PROV_TO_SHEET.get(str(x), str(x)))
    s = ser_mapped.groupby(ser_mapped).size().astype(int)
    out = dict(s)
    log(f"[agg/national] sample={sorted(out.items(), key=lambda x:-x[1])[:10]}")
    return out

def agg_seoul(df: pd.DataFrame) -> Dict[str,int]:
    if "광역" not in df.columns or "구" not in df.columns:
        return {}
    se = df[df["광역"] == "서울특별시"]
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
    # header 순서대로 한 줄 완성 (날짜/총합계 포함)
    key_sum = nospace("총합계")
    vb_norm = {nospace(k): int(v) for k, v in values_by_colname.items()}
    row = []
    total = 0
    for i, h in enumerate(header):
        nh = nospace(h)
        if i == 0:
            row.append(date_label)
            continue
        if not nh:
            row.append("")
            continue
        if nh == key_sum:
            row.append(total)
            continue
        v = int(vb_norm.get(nh, 0))
        row.append(v)
        total += v
    return row

def write_month_sheet(ws: gspread.Worksheet, date_label: str, header: List[str], values_by_colname: Dict[str,int]):
    row_idx = find_or_append_date_row(ws, date_label)
    full_row = build_full_row(header, date_label, values_by_colname)
    end_col = a1_col(len(header))
    # 행 전체 통짜 업데이트
    _retry(ws.update, [full_row], f"A{row_idx}:{end_col}{row_idx}")
    note_written(ws.title, ws.id, f"A{row_idx}:{end_col}{row_idx}")
    log(f"[ws] {ws.title} -> {date_label} row={row_idx}")
    # 바로 읽어서 검증 로그
    chk = _retry(ws.row_values, row_idx)
    log(f"[ws/verify] {ws.title} row={row_idx} length={len(chk)} first3={chk[:3]} last3={chk[-3:] if len(chk)>=3 else chk}")

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
    log("[gspread] auth")
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
    date_label = kdate_str(today)

    # 파일 수집
    paths = sorted(Path(art_dir).rglob("전국 *.xlsx"))
    log(f"[collect] found {len(paths)} xlsx files")

    for p in paths:
        try:
            y_full, mm, yy = parse_filename(p.name)
        except Exception as e:
            log(f"[file] skip (name parse fail): {p.name} {e}")
            continue

        nat_candidates = [f"전국 {yy}년 {mm}월", f"전국 {y_full}년 {mm}월"]
        se_candidates  = [f"서울 {yy}년 {mm}월", f"서울 {y_full}년 {mm}월"]
        log(f"[file] {p.name} -> nat candidates={nat_candidates} / seoul candidates={se_candidates}")

        # 엑셀 로드 & 집계
        df = read_month_df(p)
        log(f"[read] rows={len(df)} cols={len(df.columns)}")

        nat_counts_raw = agg_national(df)  # 이미 축약형 열명 기준
        se_counts      = agg_seoul(df)     # 25개 구 모두 포함(누락 0)

        # 정규화 키 준비(공백 무시)
        nat_counts = {nospace(k): int(v) for k, v in nat_counts_raw.items()}
        se_counts_n = {nospace(k): int(v) for k, v in se_counts.items()}

        # 시트 찾기
        ws_nat = find_ws_multi(sh, nat_candidates)
        ws_se  = find_ws_multi(sh, se_candidates)

        # 전국 탭 기록
        if ws_nat:
            header_nat = _retry(ws_nat.row_values, 1)
            nat_row: Dict[str,int] = {}
            for h in header_nat:
                nh = nospace(h)
                if not nh or nh == nospace("날짜") or nh == nospace("총합계"):
                    continue
                nat_row[nh] = int(nat_counts.get(nh, 0))
            write_month_sheet(ws_nat, date_label, header_nat, nat_row)
        else:
            log(f"[전국] sheet not found: tried {nat_candidates} (skip)")

        # 서울 탭 기록
        if ws_se:
            header_se = _retry(ws_se.row_values, 1)
            se_row: Dict[str,int] = {}
            for h in header_se:
                nh = nospace(h)
                if not nh or nh == nospace("날짜") or nh == nospace("총합계"):
                    continue
                se_row[nh] = int(se_counts_n.get(nh, 0))
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
