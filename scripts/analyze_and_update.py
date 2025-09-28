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

def log(msg: str):
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    print(line)
    try:
        with RUNLOG.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass

# ---------------- 공통 유틸 ----------------
def norm_cell(s: str) -> str:
    """시트 헤더/값/구이름을 동일 규칙으로 정규화 (공백/전각공백 제거)."""
    if s is None:
        return ""
    return str(s).replace("\u00A0", "").replace(" ", "").strip()

def kdate_str(d: date) -> str:
    # 구글시트에서 사용 중인 형식 그대로: 2025. 9. 28
    return f"{d.year}. {d.month}. {d.day}"

# 파일명에서 (연,월) 추출 + 탭 타이틀 생성
def parse_filename(fname: str) -> Tuple[int,int,str,str,str]:
    # 예) '전국 2509_250928.xlsx' → (2025, 9, '전국 2025년 9월', '서울 2025년 9월', '25/9')
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
            if any(x in s for x in ["429", "500", "502", "503"]):
                time.sleep(base * (2**i) + random.uniform(0, 0.3))
                continue
            raise

def a1_col(idx: int) -> str:
    s = ""
    n = idx
    while n > 0:
        n, r = divmod(n-1, 26)
        s = chr(65+r) + s
    return s

def values_batch_update(ws, payload):
    body = {"valueInputOption": "USER_ENTERED", "data": payload}
    return _retry(ws.spreadsheet.values_batch_update, body=body)

def open_sheet(sheet_id: str, sa_path: str|None):
    log("[gspread] auth")
    sa_raw = os.environ.get("SA_JSON", "").strip()
    if sa_raw:
        creds = Credentials.from_service_account_info(
            json.loads(sa_raw),
            scopes=["https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive"]
        )
    else:
        if not sa_path or not Path(sa_path).exists():
            raise RuntimeError("service account not provided")
        creds = Credentials.from_service_account_file(
            sa_path,
            scopes=["https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive"]
        )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)
    log("[gspread] spreadsheet opened")
    return sh

def _norm_title_for_match(title: str) -> str:
    """
    탭 매칭용 정규화:
    - 공백 제거
    - '전국 2024년 10월'과 '전국 24년 10월'을 동일 취급
    """
    t = title.replace(" ", "")
    t = re.sub(r"20(\d{2})년", r"\1년", t)  # 2024년 -> 24년
    return t

def fuzzy_find_sheet(sh, want_title: str):
    want_n = _norm_title_for_match(want_title)
    for ws in sh.worksheets():
        if _norm_title_for_match(ws.title) == want_n:
            log(f"[ws] matched: '{ws.title}'")
            return ws
    return None

# ---------------- 데이터 읽기 & 집계 ----------------
def read_month_df(path: Path) -> pd.DataFrame:
    # 원본 엑셀의 data 시트를 그대로 읽어온다
    df = pd.read_excel(path, sheet_name=SHEET_NAME_DATA, dtype=str)
    df = df.fillna("")
    # 숫자 필드 정리
    for c in ["계약년", "계약월", "계약일", "거래금액(만원)"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def agg_national(df: pd.DataFrame) -> Dict[str, int]:
    """
    광역 단위 집계. 키는 시트 헤더와 매칭될 '표시명' 그대로 사용.
    """
    if "광역" not in df.columns:
        return {}
    # 공백/전각공백 제거
    ser = df["광역"].map(norm_cell)
    # 표기 흔들림(강원특별자치도/강원도 등) 최소 보정
    alias = {
        "강원도": "강원특별자치도",
        "강원특별자치도": "강원특별자치도",
        "전라북도": "전북특별자치도",
        "전북특별자치도": "전북특별자치도",
        "서울특별시": "서울특별시",
        "울산광역시": "울산광역시",
    }
    ser = ser.map(lambda x: alias.get(x, x))
    s = ser.groupby(ser).size().astype(int)
    out = dict(s)
    # 디버깅: 상위 10개
    sample = sorted(out.items(), key=lambda x: -x[1])[:10]
    log(f"[agg/national] sample={sample}")
    return out

def agg_seoul(df: pd.DataFrame) -> Dict[str, int]:
    """
    서울만 필터 후 '구' 집계.
    """
    if "광역" not in df.columns or "구" not in df.columns:
        return {}
    mask = df["광역"].map(norm_cell) == norm_cell("서울특별시")
    seoul = df[mask]
    if seoul.empty:
        log("[agg/seoul] empty after filter")
        return {}
    ser = seoul["구"].map(norm_cell)
    s = ser.groupby(ser).size().astype(int)
    out = {k: int(v) for k, v in dict(s).items()}
    # 노원구 체크 로그
    log(f"[agg/seoul] nowon_present={norm_cell('노원구') in out} value={out.get(norm_cell('노원구'))}")
    return out

# ---------------- 월별 탭 기록 ----------------
def find_date_col_idx(ws) -> int:
    header = ws.row_values(1)
    for i, v in enumerate(header, start=1):
        if str(v).strip() == "날짜":
            return i
    return 1

def find_or_append_date_row(ws, target: date, date_col_idx: int = 1, header_row: int = 1) -> int:
    vals = _retry(ws.get_all_values)
    if not vals:
        return 2
    target_label = kdate_str(target)
    for i, row in enumerate(vals[1:], start=2):
        a = row[0] if row else ""
        if str(a).strip() == target_label:
            return i
    return len(vals) + 1

def build_row_by_header(header: List[str], day: date, series_norm: Dict[str, int]) -> List:
    """
    header: 시트 A1 행 그대로(표시 텍스트)
    series_norm: 정규화된 키(공백 제거) -> count
    """
    row = []
    filled_values: List[int] = []  # 총합계 계산용(실제 기록 값)
    for i, h in enumerate(header):
        h = str(h).strip()
        if i == 0:
            row.append(kdate_str(day))
            continue
        if not h:
            row.append("")
            continue
        nh = norm_cell(h)
        if nh in (norm_cell("총합계"), norm_cell("합계"), norm_cell("전체개수")):
            # 이미 채운 값들의 합을 사용 (열 밀림 방지)
            row.append(sum(filled_values) if filled_values else 0)
            continue
        # 일반 지역 컬럼
        val = int(series_norm.get(nh, 0))
        row.append(val)
        filled_values.append(val)
    return row

def upsert_row(ws, day: date, series: Dict[str, int]) -> Tuple[str, int]:
    header = _retry(ws.row_values, 1)
    if not header:
        raise RuntimeError(f"empty header in sheet '{ws.title}'")

    # 집계 series도 정규화 키로 변환
    series_norm: Dict[str, int] = {}
    for k, v in series.items():
        series_norm[norm_cell(k)] = int(v)

    date_col = find_date_col_idx(ws)
    row_idx = find_or_append_date_row(ws, day, date_col_idx=date_col, header_row=1)
    mode = "update" if row_idx and row_idx <= ws.row_count else "append"
    out = build_row_by_header(header, day, series_norm)
    last_col_letter = a1_col(len(header))
    rng = f"A{row_idx}:{last_col_letter}{row_idx}"
    values_batch_update(ws, [{"range": rng, "values": [out]}])
    return mode, row_idx

# ---------------- 메인 ----------------
def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--artifacts-dir", default=WORK_DIR_DEFAULT)
    ap.add_argument("--sheet-id", default=os.environ.get("SHEET_ID",""))
    ap.add_argument("--sa", default=os.environ.get("SA_PATH","sa.json"))
    args = ap.parse_args()

    # 로그 초기화
    try:
        RUNLOG.write_text("", encoding="utf-8")
    except Exception:
        pass

    log("[MAIN]")
    log(f"artifacts_dir={args.artifacts_dir}")

    sh = open_sheet(args.sheet_id, args.sa)

    # 오늘(한국시간) 날짜
    today = datetime.now().date()

    # 파일 수집
    files = sorted(Path(args.artifacts_dir).rglob("전국 *.xlsx"))
    log(f"[collect] found {len(files)} xlsx files")

    for p in files:
        try:
            y, m, nat_title, se_title, ym = parse_filename(p.name)
        except Exception as e:
            log(f"[file/skip] {p.name} : {e}")
            continue

        log(f"[file] {p.name} -> nat='{nat_title}' / seoul='{se_title}' / ym={ym}")

        # 엑셀 로드
        df = read_month_df(p)
        log(f"[read] rows={len(df)} cols={len(df.columns)}")

        # 집계
        nat_series = agg_national(df)
        se_series  = agg_seoul(df)

        # 탭 매칭(24년 vs 2024년 모두 인식)
        ws_nat = fuzzy_find_sheet(sh, nat_title)
        ws_se  = fuzzy_find_sheet(sh, se_title)

        if not ws_nat:
            log(f"[전국] sheet not found: '{nat_title}' (skip)")
        if not ws_se:
            log(f"[서울] sheet not found: '{se_title}' (skip)")

        if ws_nat and nat_series:
            mode, row = upsert_row(ws_nat, today, nat_series)
            log(f"[전국] {ws_nat.title} -> {kdate_str(today)} {mode} row={row}")
        elif ws_nat:
            log(f"[전국] {ws_nat.title} -> no data to write")

        if ws_se and se_series:
            mode, row = upsert_row(ws_se, today, se_series)
            log(f"[서울] {ws_se.title} -> {kdate_str(today)} {mode} row={row}")
        elif ws_se:
            log(f"[서울] {ws_se.title} -> no data to write")

    log("[main] done")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"[ERROR] {repr(e)}")
        raise
