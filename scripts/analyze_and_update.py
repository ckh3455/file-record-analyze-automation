# -*- coding: utf-8 -*-
"""
analyze_and_update.py
- GitHub Actions에서 MOLIT 아티팩트(.xlsx)들을 읽어 Google Sheets의 '기존' 탭에 집계 기록
- 규칙:
  1) 파일명에서 년/월/날짜를 추출해 대상 탭명을 퍼지 매칭(기존 탭만 사용, 새 탭 생성 금지)
  2) 전국 탭: A열 '날짜'에 해당일이 있으면 그 행을 업데이트, 없으면 마지막 행 '아래'에 새 날짜로 append
  3) 서울 탭: '광역' == '서울특별시'만 필터하여 '구' 단위로 동일 규칙 적용
  4) 날짜 매칭은 형식이 섞여도 동작(문자열/구글시트 일련번호/다양한 구분자). 내부적으로 YYYY-MM-DD로 정규화 후 비교
  5) 헤더는 시트의 기존 헤더를 우선(정렬/빈칸 허용). 누락된 컬럼은 뒤에 보강(헤더 1행만 갱신)
"""

from __future__ import annotations
import os, re, sys, json, zipfile, io
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Dict, Tuple, Optional, List

import pandas as pd
from google.oauth2.service_account import Credentials
import gspread
from gspread.utils import rowcol_to_a1

# =========================
# 로깅 유틸
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
        # 디렉토리면 OK, 파일이면 에러
        if p.is_file():
            # 기존 파일이면 백업
            bak = p.with_suffix(p.suffix + ".bak")
            p.rename(bak)
            p.mkdir(parents=True, exist_ok=True)

def log(msg: str):
    _safe_mkdir(LOG_DIR)
    now = datetime.utcnow().strftime("[%H:%M:%S]")
    line = f"{now} {msg}"
    sys.stdout.write(line + "\n")
    sys.stdout.flush()
    # 파일에도 기록
    with open(RUN_LOG, "a", encoding="utf-8") as f:
        f.write(line + "\n")
    with open(LATEST, "w", encoding="utf-8") as f:
        f.write(line + "\n")  # 마지막 메시지 덮어씀이 아니라, 최신 한 줄만 남기는 용도
    # where_written은 별도로 누적 기록할 때만 갱신

def where_write(line: str):
    _safe_mkdir(LOG_DIR)
    with open(WHERE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def log_block(title: str):
    log(f"[{title}]".upper())

# =========================
# 파일/이름 파싱
# =========================
FN_RE = re.compile(r"^(전국)\s+(\d{4})_(\d{6})\.xlsx$")  # '전국 2410_250926.xlsx' 등
SEOUL_FN_RE = re.compile(r"^(서울시)\s+(\d{6})\.xlsx$")  # '서울시 250926.xlsx' (참고: 서울 전용 파일은 현재 사용 X)

def parse_national_filename(name: str) -> Optional[Tuple[str, str, str]]:
    """
    '전국 2410_250926.xlsx' -> ('전국', '2410', '250926')
    """
    m = FN_RE.match(name)
    if not m:
        return None
    return m.group(1), m.group(2), m.group(3)

def yymm_to_year_month(yymm: str) -> Tuple[int, int]:
    yy = int(yymm[:2])
    mm = int(yymm[2:])
    year = 2000 + yy if yy < 70 else 1900 + yy
    return year, mm

def yymmdd_to_date(yymmdd: str) -> date:
    yy = int(yymmdd[:2]); mm = int(yymmdd[2:4]); dd = int(yymmdd[4:])
    year = 2000 + yy if yy < 70 else 1900 + yy
    return date(year, mm, dd)

# =========================
# 구글시트 날짜 정규화
# =========================
def to_ymd_str(x) -> Optional[str]:
    """셀 값 x를 'YYYY-MM-DD' 문자열로 변환(문자/숫자 일련번호/날짜 모두 수용)."""
    from datetime import datetime as D, date as Dd
    if x is None:
        return None
    if isinstance(x, D):
        return x.date().strftime("%Y-%m-%d")
    if isinstance(x, Dd):
        return x.strftime("%Y-%m-%d")
    s = str(x).strip()
    if not s:
        return None
    # 구글시트 일련번호
    if s.isdigit():
        try:
            base = datetime(1899, 12, 30)
            d = base + timedelta(days=int(s))
            return d.strftime("%Y-%m-%d")
        except Exception:
            pass
    # 구분자 통일 후 파싱
    s2 = s.replace(".", "-").replace("/", "-")
    for fmt in ("%Y-%m-%d",):
        try:
            d = datetime.strptime(s2, fmt).date()
            return d.strftime("%Y-%m-%d")
        except Exception:
            pass
    # '2025-9-6' 같은 한 자리 월/일
    try:
        parts = [p for p in s2.split("-") if p]
        if len(parts) == 3 and all(p.isdigit() for p in parts):
            y, m, d = int(parts[0]), int(parts[1]), int(parts[2])
            return f"{y:04d}-{m:02d}-{d:02d}"
    except Exception:
        pass
    return None

def find_date_row(ws, target_ymd: str, header_row: int = 1) -> Optional[int]:
    """ws의 A열에서 target_ymd('YYYY-MM-DD')와 같은 날짜가 있는 행을 찾아 인덱스 반환(1-based)."""
    colA = ws.col_values(1)  # 1-based
    start_idx = header_row + 1
    for idx in range(start_idx, len(colA) + 1):
        got = to_ymd_str(colA[idx - 1])
        if got == target_ymd:
            return idx
    return None

# =========================
# 퍼지 탭 매칭(공백/띄어쓰기/0패딩 유연)
# =========================
def normalize_title(s: str) -> str:
    return re.sub(r"\s+", "", s.strip())

def fuzzy_find_sheet(sh: gspread.Spreadsheet, want_title: str) -> Optional[gspread.Worksheet]:
    """
    탭 '정확 생성 금지'. 기존 탭 중에서 퍼지 매칭:
    - 공백 제거 후 완전일치 우선
    - 대안: 한글 숫자 빈칸 제거 차이를 허용(예: '전국 24년10월' == '전국 24년 10월')
    """
    tgt = normalize_title(want_title)
    for ws in sh.worksheets():
        if normalize_title(ws.title) == tgt:
            log(f"[ws] fuzzy matched (exact norm): '{ws.title}'")
            return ws
    # 더 느슨한 규칙이 필요하면 여기에 추가 가능
    return None

# =========================
# 지역명 정규화(헤더 방향으로 매핑)
# =========================
# 강원도/전라북도 등 구 명칭 변화 대응
REGION_ALIAS = {
    "강원도": "강원특별자치도",
    "전라북도": "전북특별자치도",
    "전라남도": "전라남도",
    "충청북도": "충청북도",
    "충청남도": "충청남도",
    "경상북도": "경상북도",
    "경상남도": "경상남도",
    "서울특별시": "서울특별시",
    "세종특별자치시": "세종특별자치시",
    "제주특별자치도": "제주특별자치도",
    "부산광역시": "부산광역시",
    "대구광역시": "대구광역시",
    "인천광역시": "인천광역시",
    "광주광역시": "광주광역시",
    "대전광역시": "대전광역시",
    "울산광역시": "울산광역시",
    # 과거 데이터 대비 별칭 추가
    "전북특별자치도": "전북특별자치도",
    "강원특별자치도": "강원특별자치도",
}

def norm_region(name: str) -> str:
    name = (name or "").strip()
    if not name:
        return name
    return REGION_ALIAS.get(name, name)

# =========================
# 시트 기록(업서트)
# =========================
def ensure_header(ws, target_header: List[str], header_row: int = 1) -> List[str]:
    """
    시트의 1행 헤더를 읽어 target_header에 최대한 맞춤.
    - 기존 헤더 최대 존중 (길이 부족하면 뒤에 보강)
    - 값이 다르면 해당 위치만 치환
    반환: 최종 헤더(시트에 반영된 상태 기준)
    """
    sheet_header = ws.row_values(header_row) or []
    sheet_header = [h.strip() for h in sheet_header]
    if len(sheet_header) < len(target_header):
        sheet_header += [""] * (len(target_header) - len(sheet_header))
    changed = False
    for i, want in enumerate(target_header):
        cur = sheet_header[i] if i < len(sheet_header) else ""
        if cur != want:
            sheet_header[i] = want
            changed = True
    if changed:
        end_a1 = rowcol_to_a1(header_row, len(sheet_header)).split(":")[0]
        ws.update([sheet_header], f"A{header_row}:{end_a1}")
        log(f"[ws] header updated -> {sheet_header}")
    return sheet_header

def write_row_mapped(ws, when_date: date, header: List[str], series: pd.Series, header_row: int = 1, kind: str = "national"):
    """
    - header 순서대로 한 줄을 구성(첫 컬럼은 '날짜')
    - 해당 날짜가 있으면 그 행을 UPDATE
    - 없으면 '맨 아래'에 APPEND
    """
    ymd = when_date.strftime("%Y-%m-%d")
    # 1) 헤더 정착
    sheet_header = ensure_header(ws, header, header_row=header_row)

    # 2) 날짜 행 위치 탐색
    row_idx = find_date_row(ws, ymd, header_row=header_row)

    # 3) 값 구성(헤더 기준)
    # series: index가 컬럼명(=헤더키), 값은 집계수
    # 합계 컬럼 관례 처리
    values_by_header: Dict[str, int] = {}
    for k in series.index:
        try:
            values_by_header[k] = int(series[k])
        except Exception:
            try:
                values_by_header[k] = int(float(series[k]))
            except Exception:
                values_by_header[k] = 0

    # 미리 합계 계산
    total_sum = sum(v for v in values_by_header.values() if isinstance(v, (int, float)))

    row_vals: List = []
    for i, col in enumerate(sheet_header):
        if i == 0:
            row_vals.append(ymd)
            continue
        if not col:
            row_vals.append("")
            continue
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
# 엑셀 로드 & 집계
# =========================
def read_processed_xlsx(path: Path) -> pd.DataFrame:
    """
    file-automation에서 전처리해 저장한 엑셀은 sheet='data'에 아래 컬럼들이 있음:
      - 전국: ['광역','구','법정동','리',..., '계약년','계약월','계약일', ...]
    """
    df = pd.read_excel(path, sheet_name="data", dtype=str, engine="openpyxl")
    # 숫자/결측 정리(여기선 카운트만 필요)
    for c in ("광역","구","계약년","계약월","계약일"):
        if c in df.columns:
            df[c] = df[c].fillna("").astype(str).str.strip()
    return df

def build_national_series(df: pd.DataFrame, want_columns: List[str]) -> pd.Series:
    """
    전국 탭용: '광역' 기준으로 건수 count -> 시트의 헤더(광역 컬럼들) 순서대로 값 생성
    """
    if "광역" not in df.columns:
        return pd.Series(dtype=int)

    # 지역명 정규화 후 집계
    df2 = df.copy()
    df2["광역2"] = df2["광역"].map(norm_region)
    cnt = df2.groupby("광역2").size()

    # 헤더 기준으로 매핑
    out = {}
    for col in want_columns:
        if col in ("날짜", "", "전체 개수", "총합계", "합계"):
            continue
        out[col] = int(cnt.get(col, 0))
    return pd.Series(out, dtype=int)

def build_seoul_series(df: pd.DataFrame, want_columns: List[str]) -> pd.Series:
    """
    서울 탭용: '광역' == '서울특별시'만 필터 → '구' 기준 집계
    """
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
    xlsx_files: List[Path] = []
    for p in artifacts_dir.rglob("*.xlsx"):
        xlsx_files.append(p)
    log(f"artifacts_dir={artifacts_dir}")
    log(f"zip files found: 0")
    log(f"total xlsx under work_dir: {len(xlsx_files)}")

    # 전국 파일만(파일명 패턴 일치) 정렬
    nat_files = []
    for p in sorted(xlsx_files):
        nm = p.name
        if parse_national_filename(nm):
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

    # 각 파일 처리
    for p in nat_files:
        head, yymm, yymmdd = parse_national_filename(p.name)  # ('전국', '2410', '250926')
        y, m = yymm_to_year_month(yymm)
        when = yymmdd_to_date(yymmdd)

        # 탭 타이틀 후보 (시트 실제는 공백 유무/0패딩 다를 수 있으므로 퍼지 매칭)
        nat_title = f"전국 {y%100:02d}년 {m:02d}월".replace(" 0", " 0").replace(" 00", " 00")
        seoul_title = f"서울 {y%100:02d}년 {m:02d}월"

        log(f"[file] {p.name} -> nat='{nat_title}' seoul='{seoul_title}' date={when.isoformat()}")

        # 엑셀 로드
        log(f"[read] loading xlsx: {p.as_posix()}")
        df = read_processed_xlsx(p)
        log(f"[read] sheet='data' rows={df.shape[0]} cols={df.shape[1]}")

        # === 전국 탭 ===
        ws_nat = fuzzy_find_sheet(sh, nat_title)
        if ws_nat is None:
            log(f"[전국] {p.name} -> sheet not found: '{nat_title}' (skip)")
        else:
            # 시트 헤더 읽어 전국용 컬럼 추출
            nat_header = ws_nat.row_values(1) or []
            nat_header = [h.strip() for h in nat_header]
            if not nat_header or nat_header[0] != "날짜":
                # 첫 컬럼명이 다르면 강제로 맞춰두되, 기존 헤더 우선
                if nat_header:
                    nat_header[0:1] = ["날짜"]
                else:
                    # 비정상 상황 최소화: 날짜 + df의 고유 광역들 정렬(시트 구조를 크게 바꾸지 않기 위해 전체 보강은 피함)
                    uniq_regions = sorted(set(df["광역"].map(norm_region))) if "광역" in df.columns else []
                    nat_header = ["날짜"] + uniq_regions + ["전체 개수"]
            # 전국 시리즈 생성
            nat_series = build_national_series(df, nat_header)
            # 기록
            write_row_mapped(ws_nat, when, nat_header, nat_series, header_row=1, kind="national")

        # === 서울 탭 ===
        ws_se = fuzzy_find_sheet(sh, seoul_title)
        if ws_se is None:
            log(f"[서울] {p.name} -> sheet not found: '{seoul_title}' (skip)")
        else:
            se_header = ws_se.row_values(1) or []
            se_header = [h.strip() for h in se_header]
            if not se_header or se_header[0] != "날짜":
                if se_header:
                    se_header[0:1] = ["날짜"]
                else:
                    uniq_gus = sorted(set(df[df["광역"].map(norm_region) == "서울특별시"]["구"])) \
                               if "광역" in df.columns and "구" in df.columns else []
                    se_header = ["날짜"] + uniq_gus + ["총합계"]
            se_series = build_seoul_series(df, se_header)
            write_row_mapped(ws_se, when, se_header, se_series, header_row=1, kind="seoul")

    log("[main] logs written to analyze_report/")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"[ERROR] {e!r}")
        raise
