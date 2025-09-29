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

PROV_MAP = {
    "서울특별시": "서울특별시",
    "경기도": "경기도",
    "인천광역시": "인천광역시",
    "세종특별자치시": "세종특별자치시",
    "부산광역시": "부산광역시",
    "대구광역시": "대구광역시",
    "광주광역시": "광주광역시",
    "대전광역시": "대전광역시",
    "울산광역시": "울산광역시",
    "강원특별자치도": "강원특별자치도",
    "충청남도": "충청남도",
    "충청북도": "충청북도",
    "전북특별자치도": "전북특별자치도",
    "전라남도": "전라남도",
    "경상북도": "경상북도",
    "경상남도": "경상남도",
    "제주특별자치도": "제주특별자치도",
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
def norm_soft(s: str) -> str:
    if s is None: return ""
    return str(s).replace("\u00A0","").strip()

def norm_nospace(s: str) -> str:
    return norm_soft(s).replace(" ", "")

def kdate_str(d: date) -> str:
    return f"{d.year}. {d.month}. {d.day}"

def parse_filename(fname: str) -> Tuple[int,int,str,str,str]:
    # '전국 2509_250928.xlsx' → (2025, 9, '전국 25년 9월', '서울 25년 9월', '25/9')
    m = re.search(r"(\d{2})(\d{2})_", fname)
    if not m:
        raise ValueError(f"unexpected filename: {fname}")
    yy = int(m.group(1)); mm = int(m.group(2))
    y_full = 2000 + yy
    nat_title = f"전국 {y_full}년 {mm}월"
    se_title  = f"서울 {y_full}년 {mm}월"
    ym = f"{yy}/{mm}"
    return y_full, mm, nat_title, se_title, ym

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

def find_ws(st: gspread.Spreadsheet, want: str) -> Optional[gspread.Worksheet]:
    # 1) 정확 일치
    for ws in st.worksheets():
        if ws.title == want:
            log(f"[ws] matched (exact): '{ws.title}'")
            return ws
    # 2) 공백 제거 정규화 매칭
    want_ns = norm_nospace(want)
    for ws in st.worksheets():
        if norm_nospace(ws.title) == want_ns:
            log(f"[ws] matched (nospace): '{ws.title}' (wanted='{want}')")
            return ws
    return None

def a1_col(idx: int) -> str:
    s = ""
    n = idx
    while n>0:
        n, r = divmod(n-1, 26)
        s = chr(65+r) + s
    return s

def values_batch_update(ws: gspread.Worksheet, data: List[Dict]):
    body = {"valueInputOption":"USER_ENTERED","data":data}
    return _retry(ws.spreadsheet.values_batch_update, body=body)

# ---------------- 파일 읽기 & 집계 ----------------
def read_month_df(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=SHEET_NAME_DATA, dtype=str)
    df = df.fillna("")
    # 필요한 숫자형
    for c in ["계약년","계약월","계약일","거래금액(만원)"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def agg_national(df: pd.DataFrame) -> Dict[str,int]:
    if "광역" not in df.columns:
        return {}
    ser = df["광역"].map(lambda x: PROV_MAP.get(str(x), str(x)))
    s = ser.groupby(ser).size().astype(int)
    out = dict(s)
    log(f"[agg/national] sample={sorted(out.items(), key=lambda x:-x[1])[:10]}")
    return out

def agg_seoul(df: pd.DataFrame) -> Dict[str,int]:
    if "광역" not in df.columns or "구" not in df.columns:
        return {}
    se = df[df["광역"] == "서울특별시"]
    # 구 이름을 그냥 있는 그대로 사용(시트 헤더와 동일해야 함)
    s = se["구"].groupby(se["구"]).size().astype(int)
    out = {g:int(s.get(g,0)) for g in SEOUL_GU}  # 누락 방지: 없으면 0
    # 디버그 (노원구 확인)
    log(f"[agg/seoul] nowon_present={'노원구' in s.index} value={int(s.get('노원구',0))}")
    return out

# ---------------- 월별 탭 쓰기 ----------------
def find_or_append_date_row(ws: gspread.Worksheet, date_label: str) -> int:
    vals = _retry(ws.get_all_values)
    if not vals:
        return 2
    for i, row in enumerate(vals[1:], start=2):
        if (len(row)>0) and str(row[0]).strip()==date_label:
            return i
    return len(vals)+1

def write_month_sheet(ws: gspread.Worksheet, date_label: str, header: List[str], values_by_colname: Dict[str,int]):
    hmap = {str(h).strip(): idx+1 for idx,h in enumerate(header) if str(h).strip()}
    row_idx = find_or_append_date_row(ws, date_label)
    payload = [{"range": f"A{row_idx}", "values": [[date_label]]}]
    total = 0
    for h in header[1:]:  # 첫 열은 '날짜'
        if h == "총합계":
            continue
        v = int(values_by_colname.get(h, 0))
        payload.append({"range": f"{a1_col(hmap[h])}{row_idx}", "values": [[v]]})
        total += v
    # 총합계 열 보정
    if "총합계" in hmap:
        payload.append({"range": f"{a1_col(hmap['총합계'])}{row_idx}", "values": [[total]]})
    values_batch_update(ws, payload)
    note_written(ws.title, ws.id, f"A{row_idx}:{a1_col(len(header))}{row_idx}")
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

    # 오늘(한국시간) 날짜로만 기록
    today_kst = datetime.now().date()
    date_label = kdate_str(today_kst)
    log(f"[date] using today (KST) = {date_label}")

    # 파일 수집
    paths = sorted(Path(art_dir).rglob("전국 *.xlsx"))
    log(f"[collect] found {len(paths)} xlsx files")

    for p in paths:
        try:
            y, m, nat_title, se_title, ym = parse_filename(p.name)
            log(f"[file] {p.name} -> nat='{nat_title}' / seoul='{se_title}' / ym={ym}")
        except Exception as e:
            log(f"[file] skip (name parse fail): {p.name} {e}")
            continue

        # 엑셀 로드
        df = read_month_df(p)
        log(f"[read] rows={len(df)} cols={len(df.columns)}")

        # 집계
        nat_counts = agg_national(df)
        se_counts  = agg_seoul(df)

        # 시트 찾기 (정확 일치 → nospace 보조)
        ws_nat = find_ws(sh, nat_title)
        ws_se  = find_ws(sh, se_title)

        # 전국 탭
        if ws_nat:
            header_nat = _retry(ws_nat.row_values, 1)
            # header 그대로의 컬럼만 채움(없으면 0)
            nat_row = {h:int(nat_counts.get(PROV_MAP.get(h, h), 0)) for h in header_nat if h and h!="날짜" and h!="총합계"}
            write_month_sheet(ws_nat, date_label, header_nat, nat_row)
        else:
            log(f"[전국] sheet not found: '{nat_title}' (skip)")

        # 서울 탭
        if ws_se:
            header_se = _retry(ws_se.row_values, 1)
            # 서울 구 헤더 기준 채우기(없으면 0)
            se_row = {h:int(se_counts.get(h, 0)) for h in header_se if h and h!="날짜" and h!="총합계"}
            write_month_sheet(ws_se, date_label, header_se, se_row)
        else:
            log(f"[서울] sheet not found: '{se_title}' (skip)")

    log("[main] logs written to analyze_report/")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"[ERROR] {repr(e)}")
        raise
