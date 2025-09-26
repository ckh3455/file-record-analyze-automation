# -*- coding: utf-8 -*-
"""
analyze_and_update.py
- 아티팩트(.xlsx) → Google Sheets 기존 탭에 집계 기록
- 날짜가 시트에 '이미 있으면' 해당 행 업데이트, 없으면 '맨 아래'에 새로 추가
- 전국: '광역'별 건수, 서울: '서울특별시'만 필터 후 '구'별 건수
- 탭 매칭: 공백/0패딩 차이 허용(예: '서울25년 7월' == '서울 25년 07월')
"""

from __future__ import annotations
import os, re, sys
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Dict, Tuple, Optional, List

import pandas as pd
from google.oauth2.service_account import Credentials
import gspread
from gspread.utils import rowcol_to_a1

# =========================
# 로깅
# =========================
LOG_DIR = Path("analyze_report")
RUN_STAMP = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
RUN_LOG = LOG_DIR / f"run-{RUN_STAMP}.log"
LATEST = LOG_DIR / "latest.log"
WHERE = LOG_DIR / "where_written.txt"

def _safe_mkdir(p: Path):
    try:
        p.mkdir(parents=True, exist_ok=True)
    except FileExistsError:
        if p.is_file():
            p.unlink()
            p.mkdir(parents=True, exist_ok=True)

def log(msg: str):
    _safe_mkdir(LOG_DIR)
    now = datetime.utcnow().strftime("[%H:%M:%S]")
    line = f"{now} {msg}"
    print(line, flush=True)
    with open(RUN_LOG, "a", encoding="utf-8") as f:
        f.write(line + "\n")
    with open(LATEST, "w", encoding="utf-8") as f:
        f.write(line + "\n")

def where_write(line: str):
    _safe_mkdir(LOG_DIR)
    with open(WHERE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def log_block(title: str):
    log(f"[{title}]".upper())

# =========================
# 파일명 파싱
# =========================
FN_RE = re.compile(r"^(전국)\s+(\d{4})_(\d{6})\.xlsx$")  # '전국 2410_250926.xlsx'

def parse_national_filename(name: str) -> Optional[Tuple[str, str, str]]:
    m = FN_RE.match(name)
    if not m:
        return None
    return m.group(1), m.group(2), m.group(3)

def yymm_to_year_month(yymm: str) -> Tuple[int, int]:
    yy = int(yymm[:2]); mm = int(yymm[2:])
    year = 2000 + yy if yy < 70 else 1900 + yy
    return year, mm

def yymmdd_to_date(yymmdd: str) -> date:
    yy = int(yymmdd[:2]); mm = int(yymmdd[2:4]); dd = int(yymmdd[4:])
    year = 2000 + yy if yy < 70 else 1900 + yy
    return date(year, mm, dd)

# =========================
# 퍼지 탭 매칭
# =========================
def _month_normalize(s: str) -> str:
    t = re.sub(r"\s+", "", s.strip())
    t = re.sub(r"(\d{2,4})년0?([1-9]|1[0-2])월",
               lambda m: f"{m.group(1)}년{int(m.group(2))}월", t)
    return t

def fuzzy_find_sheet(sh: gspread.Spreadsheet, want_title: str) -> Optional[gspread.Worksheet]:
    tgt = _month_normalize(want_title)
    for ws in sh.worksheets():
        if _month_normalize(ws.title) == tgt:
            log(f"[ws] fuzzy matched: '{ws.title}'")
            return ws
    titles = [_month_normalize(ws.title) for ws in sh.worksheets()]
    log(f"[ws] no match for '{want_title}' (norm='{tgt}'). available(norm)={titles}")
    return None

# =========================
# 지역명 정규화
# =========================
REGION_ALIAS = {
    "강원도": "강원특별자치도",
    "전라북도": "전북특별자치도",
    "전라남도": "전라남도",
    "충청북도": "충청북도",
    "충청남도": "충청남도",
    "경상북도": "경상북도",
    "경상남도": "경상남도",
    "서울특별시": "서울특별자치시" if "서울특별자치시" in [] else "서울특별시",  # 안전하게 기본 유지
    "세종특별자치시": "세종특별자치시",
    "제주특별자치도": "제주특별자치도",
    "부산광역시": "부산광역시",
    "대구광역시": "대구광역시",
    "인천광역시": "인천광역시",
    "광주광역시": "광주광역시",
    "대전광역시": "대전광역시",
    "울산광역시": "울산광역시",
    "전북특별자치도": "전북특별자치도",
    "강원특별자치도": "강원특별자치도",
}
def norm_region(name: str) -> str:
    name = (name or "").strip()
    return REGION_ALIAS.get(name, name)

# =========================
# 날짜 정규화 & 행 찾기
# =========================
def to_ymd_str(x) -> Optional[str]:
    from datetime import datetime as D, date as Dd
    if x is None:
        return None
    if isinstance(x, (Dd, D)):
        d = x if isinstance(x, Dd) else x.date()
        return d.strftime("%Y-%m-%d")
    s = str(x).strip()
    if not s:
        return None
    # 구글시트 일련번호
    if s.isdigit():
        try:
            base = datetime(1899, 12, 30)
            return (base + timedelta(days=int(s))).strftime("%Y-%m-%d")
        except Exception:
            pass
    s2 = s.replace(".", "-").replace("/", "-")
    s2 = s2.split()[0]  # 시간 꼬리 제거(예: "2025-09-26 00:00:00")
    # 2025-9-6 형태도 허용
    m = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})$", s2)
    if m:
        y, mo, d = map(int, m.groups())
        try:
            return f"{y:04d}-{mo:02d}-{d:02d}"
        except Exception:
            return None
    return None

def find_date_col_index(ws, header_row: int = 1) -> int:
    header = ws.row_values(header_row)
    if not header:
        return 1
    header = [str(h).strip() for h in header]
    for idx, name in enumerate(header, start=1):
        if name == "날짜":
            return idx
    # 못 찾으면 1열 가정
    return 1

def find_date_row(ws, target_ymd: str, header_row: int = 1) -> Optional[int]:
    """시트에서 '날짜' 열을 자동 탐지 후 target_ymd(YYYY-MM-DD) 행 인덱스 반환."""
    col_idx = find_date_col_index(ws, header_row=header_row)
    colA1 = rowcol_to_a1(header_row + 1, col_idx).split(":")[0]
    # 큰 범위 한번에 받아와서 비교(속도+쿼터 절약)
    values = ws.col_values(col_idx)
    # 헤더 아래부터만 검색
    search_values = values[header_row:]
    # 진단 로그(상위 15개 샘플)
    samples = [to_ymd_str(v) for v in search_values[:15]]
    log(f"[date] target={target_ymd} in col={col_idx} header_row={header_row} samples={samples}")
    for i, cell in enumerate(search_values, start=header_row + 1):
        got = to_ymd_str(cell)
        if got == target_ymd:
            return i
    return None

# =========================
# 시트 기록(업서트)
# =========================
def ensure_header(ws, target_header: List[str], header_row: int = 1) -> List[str]:
    sheet_header = ws.row_values(header_row) or []
    sheet_header = [h.strip() for h in sheet_header]
    if len(sheet_header) < len(target_header):
        sheet_header += [""] * (len(target_header) - len(sheet_header))
    changed = False
    for i, want in enumerate(target_header):
        cur = sheet_header[i] if i < len(sheet_header) else ""
        if cur != want:
            sheet_header[i] = want; changed = True
    if changed:
        end_a1 = rowcol_to_a1(header_row, len(sheet_header)).split(":")[0]
        ws.update([sheet_header], f"A{header_row}:{end_a1}")
        log(f"[ws] header updated -> {sheet_header}")
    return sheet_header

def write_row_mapped(ws, when_date: date, header: List[str], series: pd.Series,
                     header_row: int = 1, kind: str = "national"):
    ymd = when_date.strftime("%Y-%m-%d")
    sheet_header = ensure_header(ws, header, header_row=header_row)

    # 날짜열 탐지 & 행 찾기
    row_idx = find_date_row(ws, ymd, header_row=header_row)

    # 값 맵 준비
    values_by_header: Dict[str, int] = {}
    for k in series.index:
        try:
            values_by_header[k] = int(series[k])
        except Exception:
            try:
                values_by_header[k] = int(float(series[k]))
            except Exception:
                values_by_header[k] = 0

    total_sum = sum(v for v in values_by_header.values() if isinstance(v, (int, float)))

    row_vals: List = []
    for i, col in enumerate(sheet_header):
        if i == 0:
            row_vals.append(ymd); continue
        if not col:
            row_vals.append(""); continue
        if col in ("전체 개수", "총합계", "합계"):
            row_vals.append(total_sum)
        else:
            row_vals.append(values_by_header.get(col, 0))

    if row_idx is not None:
        a1 = f"A{row_idx}"
        ws.update([row_vals], a1)
        log(f"[ws] update row {row_idx} ({kind})")
        where_write(f"{ws.title} | {ymd} | UPDATE | sum={total_sum}")
        return {"op": "update", "row": row_idx}
    else:
        ws.append_row(row_vals, value_input_option="RAW")
        log(f"[ws] append new row ({kind})")
        where_write(f"{ws.title} | {ymd} | APPEND | sum={total_sum}")
        return {"op": "append", "row": None}

# =========================
# 집계 빌더
# =========================
def build_national_series(df: pd.DataFrame, want_columns: List[str]) -> pd.Series:
    if "광역" not in df.columns:
        return pd.Series(dtype=int)
    df2 = df.copy()
    df2["광역2"] = df2["광역"].map(norm_region)
    cnt = df2.groupby("광역2").size()
    out = {}
    for col in want_columns:
        if col in ("날짜", "", "전체 개수", "총합계", "합계"):
            continue
        out[col] = int(cnt.get(col, 0))
    return pd.Series(out, dtype=int)

def build_seoul_series(df: pd.DataFrame, want_columns: List[str]) -> pd.Series:
    needed = "서울특별시"
    if "광역" not in df.columns or "구" not in df.columns:
        return pd.Series(dtype=int)
    se = df[df["광역"].map(norm_region) == needed]
    if se.empty:
        return pd.Series(dtype=int)
    cnt = se.groupby("구").size()
    out = {}
    for col in want_columns:
        if col in ("날짜", "", "전체 개수", "총합계", "합계"):
            continue
        out[col] = int(cnt.get(col, 0))
    return pd.Series(out, dtype=int)

# =========================
# 메인
# =========================
def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifacts-dir", default="artifacts")
    parser.add_argument("--sa", default="sa.json")
    parser.add_argument("--sheet-id", required=True)
    args = parser.parse_args()

    log_block("MAIN")

    artifacts_dir = Path(args.artifacts_dir)
    if not artifacts_dir.exists():
        raise SystemExit(f"artifacts dir not found: {artifacts_dir}")

    # 파일 수집
    log_block("COLLECT")
    xlsx_files: List[Path] = sorted(artifacts_dir.rglob("*.xlsx"))
    log(f"artifacts_dir={artifacts_dir}")
    log(f"zip files found: 0")
    log(f"total xlsx under work_dir: {len(xlsx_files)}")

    # 전국 파일만
    nat_files = []
    for p in xlsx_files:
        if parse_national_filename(p.name):
            nat_files.append(p)
    log(f"national files count={len(nat_files)}")

    # gspread 인증
    log("[gspread] auth with sa.json")
    sa_path = Path(args.sa)
    if not sa_path.exists():
        raise SystemExit("sa.json not found")
    creds = Credentials.from_service_account_file(sa_path, scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ])
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(args.sheet_id)
    log("[gspread] spreadsheet opened")

    # 처리
    for p in nat_files:
        head, yymm, yymmdd = parse_national_filename(p.name)
        y, m = yymm_to_year_month(yymm)
        when = yymmdd_to_date(yymmdd)

        nat_title = f"전국 {y%100:02d}년 {m:02d}월"
        seoul_title = f"서울 {y%100:02d}년 {m:02d}월"

        log(f"[file] {p.name} -> nat='{nat_title}' seoul='{seoul_title}' date={when.isoformat()}")

        # 엑셀(전처리 결과: sheet_name='data')
        log(f"[read] loading xlsx: {p.as_posix()}")
        df = pd.read_excel(p, sheet_name="data", dtype=str, engine="openpyxl")
        log(f"[read] sheet='data' rows={df.shape[0]} cols={df.shape[1]}")

        # 전국
        ws_nat = fuzzy_find_sheet(sh, nat_title)
        if ws_nat is None:
            log(f"[전국] {p.name} -> sheet not found: '{nat_title}' (skip)")
        else:
            nat_header = [h.strip() for h in (ws_nat.row_values(1) or [])]
            if not nat_header or nat_header[0] != "날짜":
                # 기존 시트 헤더를 믿되, 비어 있으면 최소 헤더 구성
                uniq_regions = sorted(set(df["광역"].map(norm_region))) if "광역" in df.columns else []
                nat_header = ["날짜"] + (nat_header[1:] if nat_header else uniq_regions)  # 기존 유지
                if "전체 개수" not in nat_header and "총합계" not in nat_header and "합계" not in nat_header:
                    nat_header += ["전체 개수"]
            nat_series = build_national_series(df, nat_header)
            write_row_mapped(ws_nat, when, nat_header, nat_series, header_row=1, kind="national")

        # 서울
        ws_se = fuzzy_find_sheet(sh, seoul_title)
        if ws_se is None:
            log(f"[서울] {p.name} -> sheet not found: '{seoul_title}' (skip)")
        else:
            se_header = [h.strip() for h in (ws_se.row_values(1) or [])]
            if not se_header or se_header[0] != "날짜":
                uniq_gus = sorted(set(
                    df[df["광역"].map(norm_region) == "서울특별시"]["구"]
                )) if "광역" in df.columns and "구" in df.columns else []
                se_header = ["날짜"] + (se_header[1:] if se_header else uniq_gus)
                if "총합계" not in se_header and "전체 개수" not in se_header and "합계" not in se_header:
                    se_header += ["총합계"]
            se_series = build_seoul_series(df, se_header)
            write_row_mapped(ws_se, when, se_header, se_series, header_row=1, kind="seoul")

    log("[main] logs written to analyze_report/")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"[ERROR] {e!r}")
        raise
