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

# 전국 탭의 "열 이름"을 기준으로 맞추는 최종 타깃 매핑
# (엑셀의 '광역' 값 → 시트 열명)
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
    # '전국 2509_250929.xlsx' → (y_full=2025, m=9, yy=25)
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

def values_batch_update(ws: gspread.Worksheet, data: List[Dict]):
    body = {"valueInputOption":"USER_ENTERED","data":data}
    return _retry(ws.spreadsheet.values_batch_update, body=body)

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
    # 1) 원본 → 최종 시트 열명으로 변환
    ser_mapped = df["광역"].map(lambda x: PROV_TO_SHEET.get(str(x), str(x)))
    # 2) 집계
    s = ser_mapped.groupby(ser_mapped).size().astype(int)
    out = dict(s)
    log(f"[agg/national] sample={sorted(out.items(), key=lambda x:-x[1])[:10]}")
    return out

def agg_seoul(df: pd.DataFrame) -> Dict[str,int]:
    if "광역" not in df.columns or "구" not in df.columns:
        return {}
    se = df[df["광역"] == "서울특별시"]
    s = se["구"].groupby(se["구"]).size().astype(int)
    # 헤더 기준으로 모두 채워서 누락 방지 (노원구 포함 25개)
    out = {g:int(s.get(g,0)) for g in SEOUL_GU}
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
    # 키 정규화(공백 무시)로 안전 매칭
    hmap = {}
    for idx, h in enumerate(header, start=1):
        key = nospace(h)
        if not key:
            continue
        hmap[key] = idx

    row_idx = find_or_append_date_row(ws, date_label)
    payload = [{"range": f"A{row_idx}", "values": [[date_label]]}]
    total = 0

    # A열(날짜)과 총합계 제외하고 채움
    for raw in header[1:]:
        key = nospace(raw)
        if not key or key == nospace("총합계"):
            continue
        # values_by_colname 키도 정규화하여 매칭
        v = None
        if key in values_by_colname:
            v = values_by_colname.get(key, 0)
        else:
            # values_by_colname가 원래 축약/원표기가 섞일 수 있으니 공백무시 딕셔너리로 한 번 더 보정
            vb_norm = {nospace(k): v for k, v in values_by_colname.items()}
            v = vb_norm.get(key, 0)

        payload.append({"range": f"{a1_col(hmap[key])}{row_idx}", "values": [[int(v)]]})
        total += int(v)

    # 총합계 보정(있을 때만)
    key_sum = nospace("총합계")
    if key_sum in hmap:
        payload.append({"range": f"{a1_col(hmap[key_sum])}{row_idx}", "values": [[total]]})

    if payload:
        values_batch_update(ws, payload)
        # 로그용 표시는 실제 마지막 열과 무관(참고용)
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

    # 오늘(한국시간) 날짜로 기록
    today = datetime.now().date()
    date_label = kdate_str(today)

    # 파일 수집
    paths = sorted(Path(art_dir).rglob("전국 *.xlsx"))
    log(f"[collect] found {len(paths)} xlsx files")

    for p in paths:
        try:
            y_full, mm, yy = parse_filename(p.name)  # 2025, 9, 25
        except Exception as e:
            log(f"[file] skip (name parse fail): {p.name} {e}")
            continue

        # 탭 후보(2자리 → 4자리 순서로 시도)
        nat_candidates = [
            f"전국 {yy}년 {mm}월",
            f"전국 {y_full}년 {mm}월",
        ]
        se_candidates = [
            f"서울 {yy}년 {mm}월",
            f"서울 {y_full}년 {mm}월",
        ]

        log(f"[file] {p.name} -> nat candidates={nat_candidates} / seoul candidates={se_candidates}")

        # 엑셀 로드 & 집계
        df = read_month_df(p)
        log(f"[read] rows={len(df)} cols={len(df.columns)}")
        nat_counts_raw = agg_national(df)  # 키: 전국 탭 열명(축약형)으로 이미 맞춘 dict
        se_counts      = agg_seoul(df)     # 키: 자치구 이름 그대로

        # --- 정규화 딕셔너리로 만들어 두기(공백무시) ---
        nat_counts = {nospace(k): int(v) for k, v in nat_counts_raw.items()}
        se_counts_n = {nospace(k): int(v) for k, v in se_counts.items()}

        # 시트 찾기
        ws_nat = find_ws_multi(sh, nat_candidates)
        ws_se  = find_ws_multi(sh, se_candidates)

        # 전국 탭 기록 (시트 헤더 기준으로 값을 채움)
        if ws_nat:
            header_nat = _retry(ws_nat.row_values, 1)
            nat_row: Dict[str,int] = {}
            for h in header_nat:
                key = nospace(h)
                if not key or key == nospace("날짜") or key == nospace("총합계"):
                    continue
                nat_row[key] = int(nat_counts.get(key, 0))
            write_month_sheet(ws_nat, date_label, header_nat, nat_row)
        else:
            log(f"[전국] sheet not found: tried {nat_candidates} (skip)")

        # 서울 탭 기록 (시트 헤더 기준으로 값을 채움)
        if ws_se:
            header_se = _retry(ws_se.row_values, 1)
            se_row: Dict[str,int] = {}
            for h in header_se:
                key = nospace(h)
                if not key or key == nospace("날짜") or key == nospace("총합계"):
                    continue
                se_row[key] = int(se_counts_n.get(key, 0))
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
