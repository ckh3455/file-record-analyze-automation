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

# ---------------- Normalize helpers ----------------

def norm_raw(s: str) -> str:
    """공백/전각공백 제거 + 소문자화(한글엔 영향無)"""
    if s is None: return ""
    return str(s).replace("\u00A0","").replace(" ", "").strip()

# 행정구역 표기 통합(정규화된 키 -> 정규화된 대표키)
# ex) '강원도'와 '강원특별자치도'는 한 그룹으로 묶기
CANON_MAP = {
    # 강원
    "강원도": "강원특별자치도",
    "강원특별자치도": "강원특별자치도",
    # 전북
    "전라북도": "전북특별자치도",
    "전북특별자치도": "전북특별자치도",
    # 세종/제주는 그대로
    "세종특별자치시": "세종특별자치시",
    "제주특별자치도": "제주특별자치도",
    # 나머지 광역은 자체 유지
    "경기도": "경기도",
    "경상남도": "경상남도",
    "경상북도": "경상북도",
    "광주광역시": "광주광역시",
    "대구광역시": "대구광역시",
    "대전광역시": "대전광역시",
    "부산광역시": "부산광역시",
    "서울특별시": "서울특별자치시" if False else "서울특별시",  # 시트가 '서울특별시'면 그대로
    "울산광역시": "울산광역시",
    "인천광역시": "인천광역시",
    "전라남도": "전라남도",
    "충청남도": "충청남도",
    "충청북도": "충청북도",
}

# 정규화 키로 매칭할 때 사용할: (정규화문자열) -> (정규화 대표문자열)
CANON_MAP_NORM: Dict[str,str] = {}
for k,v in CANON_MAP.items():
    CANON_MAP_NORM[norm_raw(k)] = norm_raw(v)

def canonize_region_name(s: str) -> str:
    n = norm_raw(s)
    return CANON_MAP_NORM.get(n, n)

def canonize_gu_name(s: str) -> str:
    # 구 이름도 공백 제거만으로 대부분 해결
    return norm_raw(s)

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

def parse_tab_ym(title: str) -> Optional[Tuple[int,int]]:
    m = re.match(r"(?:전국|서울)\s*(\d{2})년\s*(\d{1,2})월", title.replace(" ",""))
    if not m: return None
    return 2000 + int(m.group(1)), int(m.group(2))

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
    must = ["광역","구","계약년","계약월","계약일"]
    missing = [c for c in must if c not in df.columns]
    if missing:
        raise ValueError(f"missing columns: {missing}")
    # 숫자만 남기기
    for c in ["계약년","계약월","계약일"]:
        df[c] = df[c].astype(str).str.replace(r"\D","",regex=True)
    return df

def agg_national(df: pd.DataFrame) -> Dict[str,int]:
    # 광역명 정규화 -> 대표키 기준으로 합산
    ser = df["광역"].map(canonize_region_name)
    s = ser.groupby(ser).size().astype(int)
    out: Dict[str,int] = dict(s)
    # 디버그 샘플
    items = sorted(out.items(), key=lambda x: -x[1])[:10]
    log(f"[agg/national] sample={items}")
    return out

def agg_seoul(df: pd.DataFrame) -> Dict[str,int]:
    # 서울만 필터
    mask = df["광역"].map(lambda x: canonize_region_name(x) in (canonize_region_name("서울특별시"),))
    se = df[mask]
    if se.empty:
        log("[agg/seoul] empty after filter")
        return {}
    ser = se["구"].map(canonize_gu_name)
    s = ser.groupby(ser).size().astype(int)
    out: Dict[str,int] = dict(s)
    items = sorted(out.items(), key=lambda x: -x[1])[:10]
    log(f"[agg/seoul] sample={items}")
    return out

# ---------------- Sheet access ----------------

def fuzzy_find_sheet(sh, want_title: str):
    want_n = norm_raw(want_title)
    hit = None
    for ws in sh.worksheets():
        if norm_raw(ws.title) == want_n:
            hit = ws; break
    if hit:
        log(f"[ws] fuzzy matched: '{ws.title}'")
    return hit

def find_date_col_idx(ws) -> int:
    header = ws.row_values(1)
    for i, v in enumerate(header, start=1):
        if str(v).strip() == "날짜":
            return i
    return 1

def find_date_row(ws, target: date, date_col_idx: int = 1, header_row: int = 1) -> Optional[int]:
    rng = ws.range(header_row+1, date_col_idx, ws.row_count, date_col_idx)
    for off, cell in enumerate(rng, start=header_row+1):
        v = str(cell.value).strip()
        if not v: continue
        d = parse_any_date(v)
        if d and d == target:
            return off
    return None

def build_row_by_header(header: List[str], day: date, series_norm: Dict[str,int]) -> List:
    """
    header: A1 행의 표시 텍스트
    series_norm: 정규화된 키(공백제거/표준화) -> count
    """
    row = []
    total = 0
    # 헤더 별칭(정규화 형태로 보관)
    alias_norm = {
        norm_raw("강원도"): norm_raw("강원특별자치도"),
        norm_raw("강원특별자치도"): norm_raw("강원특별자치도"),
        norm_raw("전라북도"): norm_raw("전북특별자치도"),
        norm_raw("전북특별자치도"): norm_raw("전북특별자치도"),
    }
    for i, h in enumerate(header):
        h = str(h).strip()
        if i == 0:
            row.append(kdate_str(day)); continue
        if not h:
            row.append(""); continue
        nh = norm_raw(h)
        val = series_norm.get(nh)
        if val is None and nh in alias_norm:
            val = series_norm.get(alias_norm[nh])
        if val is None:
            # 서울 시트의 '총합계' 등 합계 컬럼 처리
            if nh in (norm_raw("총합계"), norm_raw("전체 개수"), norm_raw("합계")):
                row.append(total)
            else:
                row.append(0)   # ← 이전엔 빈칸, 이제 0
        else:
            row.append(int(val))
            total += int(val)
    return row

def upsert_row(ws, day: date, series: Dict[str,int]) -> Tuple[str,int]:
    header = ws.row_values(1)
    if not header:
        raise RuntimeError(f"empty header in sheet '{ws.title}'")
    # series를 정규화 키로 재작성
    series_norm: Dict[str,int] = {}
    for k,v in series.items():
        series_norm[norm_raw(k)] = int(v)

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

    out = build_row_by_header(header, day, series_norm)
    # A1:마지막열
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

    # 오늘 날짜(한국시간)으로 기록
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

        ws_nat = None
        ws_se = None
        for ws in sh.worksheets():
            t = ws.title.replace(" ", "")
            if t == nat_tab_title.replace(" ", ""):
                ws_nat = ws
            if t == se_tab_title.replace(" ", ""):
                ws_se = ws

        if not ws_nat:
            log(f"[전국] sheet not found: '{nat_tab_title}' (skip)")
        if not ws_se:
            log(f"[서울] sheet not found: '{se_tab_title}' (skip)")

        target_date = today_kst

        if ws_nat and nat_series:
            mode, row = upsert_row(ws_nat, target_date, nat_series)
            log(f"[전국] {ws_nat.title} -> {kdate_str(target_date)} {mode} row={row}")
            note_written(f"{ws_nat.title}\t{kdate_str(target_date)}\t{mode}\t{row}")
        elif ws_nat:
            log(f"[전국] {ws_nat.title} -> no rows (empty agg)")

        if ws_se and se_series:
            mode, row = upsert_row(ws_se, target_date, se_series)
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
