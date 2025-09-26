# -*- coding: utf-8 -*-
"""
analyze_and_update.py

- 아티팩트(.xlsx) → Google Sheets 기존 '해당 탭'에 집계 기록
- 날짜가 시트에 있으면 그 '해당 날짜 행' 업데이트, 없으면 맨 아래 행에 날짜 추가 후 기록
- 전국: '광역'별 건수, 서울: '서울특별시'만 필터 후 '구'별 건수
- 탭 매칭: 공백/0패딩 차이 허용(예: '서울25년7월' == '서울 25년 07월')
- 날짜 매칭: 시트가 'YYYY. M. D'여도 내부 비교는 YYYY-MM-DD 정규화
- 날짜 표시는 시트에 'YYYY. M. D'로 기록
- ⚠ 핵심수정: 헤더 행 탐지 + 날짜 열 탐지 자동화. 엉뚱한 열에 쓰는 문제 방지.
"""

from __future__ import annotations
import os, re, sys
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Dict, Tuple, Optional, List

import pandas as pd
from google.oauth2.service_account import Credentials
import gspread
from gspread.utils import rowcol_to_a1, a1_to_rowcol

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
        # 이미 디렉토리면 OK, 파일이면 삭제 후 생성
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
    # latest.log는 항상 마지막 메시지로 갱신
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
    # 0패딩 월 → 비패딩으로 통일
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
    "강원특별자치도": "강원특별자치도",
    "전라북도": "전북특별자치도",
    "전북특별자치도": "전북특별자치도",
    "전라남도": "전라남도",
    "충청북도": "충청북도",
    "충청남도": "충청남도",
    "경상북도": "경상북도",
    "경상남도": "경상남도",
    "서울특별시": "서울특별자치시" if False else "서울특별시",  # (표기는 그대로 둠)
    "세종특별자치시": "세종특별자치시",
    "제주특별자치도": "제주특별자치도",
    "부산광역시": "부산광역시",
    "대구광역시": "대구광역시",
    "인천광역시": "인천광역시",
    "광주광역시": "광주광역시",
    "대전광역시": "대전광역시",
    "울산광역시": "울산광역시",
}

def norm_region(name: str) -> str:
    name = (name or "").strip()
    return REGION_ALIAS.get(name, name)

# =========================
# 날짜 정규화 & 표시
# =========================
def normalize_to_ymd(x) -> Optional[str]:
    """
    여러 형태의 날짜(일련번호/문자열/Datetime)를 'YYYY-MM-DD'로 정규화.
    '2025. 9. 26' 같은 점/공백 혼합도 잡아냄.
    """
    from datetime import datetime as D, date as Dd
    if x is None:
        return None
    if isinstance(x, (Dd, D)):
        d = x if isinstance(x, Dd) else x.date()
        return d.strftime("%Y-%m-%d")
    s = str(x).strip()
    if not s:
        return None
    # 구글시트 일련번호(정수)
    if s.isdigit():
        try:
            base = datetime(1899, 12, 30)  # Google Sheets epoch
            return (base + timedelta(days=int(s))).strftime("%Y-%m-%d")
        except Exception:
            pass
    # 불필요문자 제거 후 구분자 통일
    s = re.sub(r"[^\d./\- ]", "", s)
    s2 = re.sub(r"\s+", "", s).strip(".-/")
    s2 = s2.replace(".", "-").replace("/", "-")
    m = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})$", s2)
    if m:
        y, mo, d = map(int, m.groups())
        try:
            return f"{y:04d}-{mo:02d}-{d:02d}"
        except Exception:
            return None
    return None

def display_dot_date(d: date) -> str:
    """시트에 쓸 표시용 날짜: 'YYYY. M. D' (0패딩 없음, 점 뒤 공백 포함)"""
    return f"{d.year}. {d.month}. {d.day}"

# =========================
# 헤더/날짜열 자동 탐지
# =========================
def detect_header_row(ws: gspread.Worksheet, max_scan_rows: int = 10) -> int:
    """
    상단 N줄 스캔해서 '날짜'가 포함된 행을 헤더로 우선 채택.
    없으면, '강원/경기/서울/부산' 등 지역명이 다수 포함된 행을 헤더로 추정.
    둘 다 없으면 1행.
    """
    region_keys = {"강원", "경기", "서울", "부산", "인천", "대전", "대구", "광주", "울산", "세종",
                   "전북", "전남", "충북", "충남", "경북", "경남", "제주", "총합", "전체"}
    cand_by_regions = []
    for r in range(1, max_scan_rows + 1):
        row = [c.strip() for c in ws.row_values(r)]
        if not row:
            continue
        if any(c == "날짜" for c in row):
            log(f"[header] found '날짜' in row {r}: {row[:10]}")
            return r
        hit = sum(any(k in c for k in region_keys) for c in row if c)
        cand_by_regions.append((hit, r, row[:10]))
    if cand_by_regions:
        cand_by_regions.sort(reverse=True)
        top_hit, r, sample = cand_by_regions[0]
        if top_hit >= 2:
            log(f"[header] choose by region-hit row {r} hit={top_hit}: {sample}")
            return r
    log("[header] fallback to row 1")
    return 1

def detect_date_col(ws: gspread.Worksheet, header_row: int, scan_rows: int = 40, max_cols: int = 40) -> int:
    """
    '날짜' 제목 열이 없을 수도 있으므로, 헤더 아래 scan_rows 만큼 각 열에서
    '날짜로 해석 가능한 값' 비율을 계산해 최다 컬럼을 날짜열로 판단.
    """
    header = ws.row_values(header_row)
    if header:
        for i, c in enumerate(header, start=1):
            if c.strip() == "날짜":
                log(f"[date-col] header says '날짜' at col {i}")
                return i

    # 헤더에 '날짜' 없음 → 통계로 판단
    best_col, best_score = 1, -1
    for col in range(1, min(ws.col_count, max_cols) + 1):
        vals = ws.col_values(col)[header_row:header_row + scan_rows]
        normed = [normalize_to_ymd(v) for v in vals if str(v).strip()]
        score = sum(1 for v in normed if v is not None)
        if score > best_score:
            best_col, best_score = col, score
    log(f"[date-col] guessed col={best_col} by score={best_score}")
    return best_col

def find_date_row(ws: gspread.Worksheet, target_ymd: str, header_row: int, date_col: int) -> Optional[int]:
    """탐지된 날짜열에서 target_ymd(YYYY-MM-DD)와 같은 행을 찾는다."""
    values = ws.col_values(date_col)
    search_values = values[header_row:]
    # 진단 샘플
    samples = [normalize_to_ymd(v) for v in search_values[:15]]
    log(f"[date-find] target={target_ymd} in col={date_col} header_row={header_row} samples={samples}")
    for i, cell in enumerate(search_values, start=header_row + 1):
        got = normalize_to_ymd(cell)
        if got == target_ymd:
            return i
    return None

# =========================
# 시트 기록(업서트)
# =========================
def ensure_header(ws: gspread.Worksheet, target_header: List[str], header_row: int) -> List[str]:
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

def write_row_mapped(ws: gspread.Worksheet, when_date: date, header: List[str], series: pd.Series,
                     header_row: int, kind: str):
    """
    - header_row/날짜열(date_col) 자동 탐지 사용
    - 기존 날짜행 있으면 그 행에 전체 폭으로 업데이트
    - 없으면 append_row
    """
    # 헤더 보정
    sheet_header = ensure_header(ws, header, header_row=header_row)

    ymd_norm = when_date.strftime("%Y-%m-%d")
    show_date = display_dot_date(when_date)

    # 날짜열 탐지 + 대상행 탐색
    date_col = detect_date_col(ws, header_row=header_row)
    row_idx = find_date_row(ws, ymd_norm, header_row=header_row, date_col=date_col)

    # 값 맵
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

    # 한 행 값 구성
    row_vals: List = []
    for i, col in enumerate(sheet_header):
        if i == 0:
            row_vals.append(show_date); continue
        if not col:
            row_vals.append(""); continue
        if col in ("전체 개수", "총합계", "합계"):
            row_vals.append(total_sum)
        else:
            row_vals.append(values_by_header.get(col, 0))

    if row_idx is not None:
        # 정렬 보장: A{row} ~ {last_col}{row} 범위를 한 번에 업데이트
        last_a1 = rowcol_to_a1(header_row, len(sheet_header))
        _, last_col = a1_to_rowcol(last_a1)
        end_a1 = rowcol_to_a1(row_idx, last_col)
        ws.update([row_vals], f"A{row_idx}:{end_a1}")
        log(f"[ws] update row {row_idx} ({kind}) -> {show_date}")
        where_write(f"{ws.title} | {show_date} | UPDATE | sum={total_sum}")
        return {"op": "update", "row": row_idx}
    else:
        ws.append_row(row_vals, value_input_option="RAW")
        log(f"[ws] append new row ({kind}) -> {show_date}")
        where_write(f"{ws.title} | {show_date} | APPEND | sum={total_sum}")
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
    if "광역" not in df.columns or "구" not in df.columns:
        return pd.Series(dtype=int)
    se = df[df["광역"].map(norm_region) == "서울특별시"]
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
    nat_files: List[Path] = []
    xlsx_files: List[Path] = sorted(artifacts_dir.rglob("*.xlsx"))
    log_block("COLLECT")
    log(f"artifacts_dir={artifacts_dir}")
    log(f"zip files found: 0")
    log(f"total xlsx under work_dir: {len(xlsx_files)}")
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

        # 전국 탭
        ws_nat = fuzzy_find_sheet(sh, nat_title)
        if ws_nat is None:
            log(f"[전국] {p.name} -> sheet not found: '{nat_title}' (skip)")
        else:
            # 헤더 행/날짜 열 자동 탐지
            nat_header_row = detect_header_row(ws_nat)
            # 시트 첫 행(헤더행) 기준 컬럼 목록
            nat_header = [h.strip() for h in (ws_nat.row_values(nat_header_row) or [])]
            if not nat_header or nat_header[0] != "날짜":
                # 헤더 보정(최소 '날짜' + 합계)
                uniq_regions = sorted(set(df["광역"].map(norm_region))) if "광역" in df.columns else []
                nat_header = ["날짜"] + [c for c in nat_header[1:] if c]  # 기존 보존
                if not any(c in ("전체 개수","총합계","합계") for c in nat_header):
                    nat_header += ["전체 개수"]
                # 지역 컬럼이 없다면 유니크 지역을 뒤에 보강
                for r in uniq_regions:
                    if r not in nat_header:
                        nat_header.append(r)

            nat_series = build_national_series(df, nat_header)
            write_row_mapped(ws_nat, when, nat_header, nat_series,
                             header_row=nat_header_row, kind="national")

        # 서울 탭
        ws_se = fuzzy_find_sheet(sh, seoul_title)
        if ws_se is None:
            log(f"[서울] {p.name} -> sheet not found: '{seoul_title}' (skip)")
        else:
            se_header_row = detect_header_row(ws_se)
            se_header = [h.strip() for h in (ws_se.row_values(se_header_row) or [])]
            if not se_header or se_header[0] != "날짜":
                uniq_gus = sorted(set(
                    df[df["광역"].map(norm_region) == "서울특별시"]["구"]
                )) if "광역" in df.columns and "구" in df.columns else []
                se_header = ["날짜"] + [c for c in se_header[1:] if c]
                if not any(c in ("총합계","전체 개수","합계") for c in se_header):
                    se_header += ["총합계"]
                for g in uniq_gus:
                    if g not in se_header:
                        se_header.append(g)

            se_series = build_seoul_series(df, se_header)
            write_row_mapped(ws_se, when, se_header, se_series,
                             header_row=se_header_row, kind="seoul")

    log("[main] logs written to analyze_report/")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"[ERROR] {e!r}")
        raise
