#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
아티팩트로 받은 '전국 YYYYMM_YYMMDD.xlsx' 파일들을 읽어
- 각 월 탭(전국/서울)에 날짜별로 건수를 기록
- '거래요약' 탭에 월별 요약(건수/중앙값/평균가/전월대비)을 기록
- '압구정동' 탭에 원본 행을 붙이고, 증감(신규/삭제)을 맨 아래 빨간 글씨 로그로 기록
  (고유키는 (계약년,계약월,계약일,단지명,전용면적(㎡),동,층,거래금액(만원),기록일))
"""

from __future__ import annotations
import os, re, sys, json, glob, math, time
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict, Counter

import pandas as pd
import numpy as np

import gspread
from google.oauth2.service_account import Credentials
from gspread.utils import rowcol_to_a1, a1_range_to_grid_range

# -------------------- 설정/유틸 --------------------

LOG_DIR = Path("analyze_report")
def log(msg: str) -> None:
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
    except FileExistsError:
        pass
    ts = datetime.now().strftime("[%H:%M:%S]")
    print(f"{ts} {msg}")
    # 최신 로그 파일에 append
    (LOG_DIR / "latest.log").write_text(
        ((LOG_DIR / "latest.log").read_text(encoding="utf-8") if (LOG_DIR / "latest.log").exists() else "")
        + f"{ts} {msg}\n", encoding="utf-8"
    )

def log_block(title: str):
    log(f"[{title.upper()}]")

def today_ymd_dot(d: datetime) -> str:
    # "2025. 9. 27" 형식
    return f"{d.year}. {d.month}. {d.day}"

def to_int(x):
    try:
        return int(str(x).strip())
    except Exception:
        return None

# 탭명 퍼지 매칭(공백 제거)
def normalize_title(s: str) -> str:
    return re.sub(r"\s+", "", s or "").strip()

def find_ws_by_title(spread: gspread.Spreadsheet, target: str) -> gspread.Worksheet | None:
    want = normalize_title(target)
    for ws in spread.worksheets():
        if normalize_title(ws.title) == want:
            return ws
    return None

# 첫 행에서 '날짜' 헤더 찾기
def find_date_col(ws: gspread.Worksheet) -> tuple[int,int]:
    vals = ws.get_values("1:1")
    if vals and vals[0]:
        header = vals[0]
        for i, name in enumerate(header, start=1):
            if str(name).strip() == "날짜":
                return (1, i)  # (header_row, date_col)
    return (1, 1)

# 날짜 찾기: '날짜' 열만 검색, 없으면 추가
def find_or_append_date_row(ws: gspread.Worksheet, date_str: str) -> int:
    header_row, date_col = find_date_col(ws)
    # 날짜 열 전체 읽기
    col_letter = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"[date_col-1]
    col_vals = ws.get_values(f"{col_letter}{header_row+1}:{col_letter}")
    # 일치하는 날짜 찾기
    target = date_str.strip()
    for idx, r in enumerate(col_vals, start=header_row+1):
        v = (r[0] if r else "").strip()
        if v == target:
            return idx
    # 없으면 맨 아래 추가
    ws.append_row([""]*(ws.col_count or 26), value_input_option="USER_ENTERED")
    last = ws.row_count
    ws.update(f"{col_letter}{last}", [[target]], value_input_option="USER_ENTERED")
    return last

# 열 이름 → 인덱스 매핑
def header_map_from_ws(ws: gspread.Worksheet) -> dict:
    header = ws.get_values("1:1")
    header = header[0] if header else []
    return {name.strip(): idx+1 for idx, name in enumerate(header)}

# -------------------- 데이터 읽기 --------------------

def read_xlsx_rows(xlsx_path: Path) -> pd.DataFrame:
    # data 시트 기준
    df = pd.read_excel(xlsx_path, sheet_name="data", dtype=str)
    # 표준 컬럼명으로 트림
    df.columns = [str(c).strip() for c in df.columns]
    # 결측치 공백 변환
    df = df.fillna("")
    return df

# 광역/구 집계
def summarize_nat_seoul(df: pd.DataFrame) -> tuple[pd.Series,pd.Series]:
    # 표준화: 광역, 구, 법정동, 계약년/월/일, 거래금액(만원) 등
    # 일부 파일은 광역 컬럼명이 다를 수 있어 보호적 접근
    if "광역" not in df.columns and "시도" in df.columns:
        df = df.rename(columns={"시도": "광역"})
    if "구" not in df.columns and "시군구" in df.columns:
        df = df.rename(columns={"시군구": "구"})

    # 집계용
    nat = df.groupby("광역", dropna=False).size() if "광역" in df.columns else pd.Series(dtype=int)
    seoul = df.loc[df.get("광역","")=="서울특별시"]
    seoul = seoul.groupby("구", dropna=False).size() if "구" in seoul.columns else pd.Series(dtype=int)
    return nat, seoul

# -------------------- 시트 기록 (전국/서울) --------------------

def write_counts(ws: gspread.Worksheet, date_str: str, header_keys: list[str], values_map: dict[str,int], sum_col_name: str | None = None):
    """header_keys 순서대로 날짜 행에 숫자를 채운다. sum_col_name 지정 시 마지막에 총합계도 기록."""
    row_idx = find_or_append_date_row(ws, date_str)
    # 날짜 col은 1
    # header는 1행 기준, 각 키의 열 위치 찾기
    header_map = header_map_from_ws(ws)
    updates = []
    total = 0
    for k in header_keys:
        col = header_map.get(k)
        if not col:  # 탭 헤더에 없는 열은 건너뜀
            continue
        val = int(values_map.get(k, 0))
        total += val
        a1 = f"{rowcol_to_a1(row_idx, col)}"
        updates.append((a1, [[val]]))

    if sum_col_name:
        sum_col = header_map.get(sum_col_name)
        if sum_col:
            updates.append((f"{rowcol_to_a1(row_idx, sum_col)}", [[total]]))

    # 배치 업데이트로 레이트리밋 완화
    for a1, v in updates:
        ws.update(a1, v, value_input_option="USER_ENTERED")

# -------------------- 거래요약 탭 --------------------

# 시트의 열 이름 매핑(요약 탭 헤더)
SUMMARY_COLS = [
    "년월","구분","전국","서울","강남구","압구정동","경기도","인천광역시","세종시","서초구","송파구","용산구",
    "강동구","성동구","마포구","양천구","동작구","영등포구","종로구","광진구","강서구","강북구","관악구","구로구",
    "금천구","도봉구","동대문구","서대문구","성북구","은평구","중구","중랑구","부산","대구","광주","대전",
    "강원도","경남","경북","전남","전북","충남","충북","제주"
]

# 광역명 → 요약열 이름(축약) 매핑
PROV_TO_SUMMARY = {
    "서울특별시": "서울", "세종특별자치시": "세종시", "강원특별자치도": "강원도",
    "경기도": "경기도", "인천광역시": "인천광역시",
    "경상남도": "경남", "경상북도": "경북", "광주광역시": "광주",
    "대구광역시": "대구", "대전광역시": "대전", "부산광역시": "부산",
    "울산광역시": "울산", "전라남도": "전남", "전북특별자치도": "전북",
    "제주특별자치도": "제주", "충청남도": "충남", "충청북도": "충북"
}

SEOUL_DISTRICTS = [
    "강남구","강동구","강북구","강서구","관악구","광진구","구로구","금천구","노원구","도봉구","동대문구",
    "동작구","마포구","서대문구","서초구","성동구","성북구","송파구","양천구","영등포구","용산구",
    "은평구","종로구","중구","중랑구"
]

def ym_str(y: int, m: int) -> str:
    return f"{str(y)[-2:]}/{m:02d}"

def write_month_summary(ws: gspread.Worksheet, y: int, m: int,
                        counts: dict[str,int], prices: dict[str,tuple[float,float]], prev_counts: dict[str,int] | None):
    # 대상 행 찾기: A열(년월)에서 "YY/MM"
    ym = ym_str(y, m)
    vals = ws.get_all_values()
    header = vals[0] if vals else SUMMARY_COLS
    col_map = {c:i+1 for i,c in enumerate(header)}
    # 년월 열은 항상 A(1), 구분은 B(2)
    year_col = 1; type_col = 2

    # 행 탐색
    row_by_type = {}
    for r_idx in range(2, len(vals)+1):
        if vals[r_idx-1][0].strip() == ym:
            row_by_type[vals[r_idx-1][1].strip()] = r_idx

    def ensure_row(row_type: str) -> int:
        r = row_by_type.get(row_type)
        if r: return r
        ws.append_row([ym, row_type], value_input_option="USER_ENTERED")
        r = ws.row_count
        row_by_type[row_type] = r
        return r

    # 기록 함수
    def put(row_type: str, col_name: str, value, bold=False, color=None, number_fmt=None):
        r = ensure_row(row_type)
        c = col_map.get(col_name)
        if not c: 
            return
        a1 = rowcol_to_a1(r, c)
        ws.update(a1, [[value]], value_input_option="USER_ENTERED")
        # 서식
        fmt = {}
        if bold:
            fmt.setdefault("textFormat", {})["bold"] = True
        if color: # {"red":0, "green":0, "blue":1}
            fmt.setdefault("textFormat", {})["foregroundColor"] = color
        if number_fmt:
            fmt["numberFormat"] = {"type":"NUMBER","pattern": number_fmt}
        if fmt:
            ws.format(a1, fmt)

    # 전국/광역/서울/구 집계 채우기 (건수, 중앙값, 평균가)
    # 건수는 볼드
    for col in ["전국"] + list(PROV_TO_SUMMARY.values()) + SEOUL_DISTRICTS + ["압구정동"]:
        cnt = counts.get(col, 0)
        med, avg = prices.get(col, (None, None))
        put("거래건수", col, cnt, bold=True)
        if med is not None:
            put("중앙값(단위:억)", col, round(med, 2), number_fmt="0.00")
        if avg is not None:
            put("평균가(단위:억)", col, round(avg, 2), number_fmt="0.00")

    # 전월대비(건수) 색상/기호
    if prev_counts:
        for col in ["전국"] + list(PROV_TO_SUMMARY.values()) + SEOUL_DISTRICTS + ["압구정동"]:
            cur = counts.get(col, 0)
            prev = prev_counts.get(col, 0)
            diff = cur - prev
            if diff > 0:
                txt = f"+{diff}"
                color = {"red":0, "green":0, "blue":1}  # 파란색
            elif diff < 0:
                txt = f"{diff}"  # 음수 그대로
                color = {"red":1, "green":0, "blue":0}  # 빨간색
            else:
                txt = "0"
                color = None
            # 전월대비 건수증감 열 제목은 '전월대비 건수증감'
            put("전월대비 건수증감", col, txt, color=color)

    # 예상건수는 현재 스킵 (기존 구조 유지)


# -------------------- 압구정동 (원본 행 + 증감 로그 + 기록일 컬럼/키) --------------------

APGU_SHEET_TITLE = "압구정동"
# 고유키 필드 (맨 끝에 '기록일' 추가)
APGU_KEY_FIELDS = ["계약년","계약월","계약일","단지명","전용면적(㎡)","동","층","거래금액(만원)","기록일"]

# 압구정동 탭의 표준 컬럼(맨 끝에 '기록일' 추가)
APGU_SHEET_COLUMNS = [
    "광역","구","법정동","법정동코드","지번","본번","부번","단지명","전용면적(㎡)","계약년",
    "계약월","계약일","거래금액(만원)","동","층","건축년도","도로명","거래유형","주택유형","기록일"
]

def _row_to_key(row: list[str], header_map: dict[str,int]) -> tuple:
    def get(col):
        idx = header_map.get(col)
        if not idx: return ""
        i0 = idx - 1
        return row[i0] if 0 <= i0 < len(row) else ""
    return tuple(str(get(c)).strip() for c in APGU_KEY_FIELDS)

def read_apgu_existing(ws: gspread.Worksheet) -> tuple[list[list[str]], set[tuple], dict[str,int]]:
    vals = ws.get_all_values()
    if not vals:
        return [], set(), {}
    header = vals[0]
    hmap = {name.strip(): i+1 for i, name in enumerate(header)}
    rows = vals[1:]
    keys = set()
    for r in rows:
        keys.add(_row_to_key(r, hmap))
    return rows, keys, hmap

def build_apgu_new(df_all: pd.DataFrame, record_date: str) -> tuple[list[list[str]], set[tuple], dict[str,int]]:
    # 표준화
    if "광역" not in df_all.columns and "시도" in df_all.columns:
        df_all = df_all.rename(columns={"시도":"광역"})
    if "구" not in df_all.columns and "시군구" in df_all.columns:
        df_all = df_all.rename(columns={"시군구":"구"})
    # 필터: 서울특별시 & 압구정동
    df = df_all[(df_all.get("광역","")=="서울특별시") & (df_all.get("법정동","")=="압구정동")].copy()
    if df.empty:
        return [], set(), {c:i+1 for i,c in enumerate(APGU_SHEET_COLUMNS)}

    # 결측 보정
    for c in APGU_SHEET_COLUMNS:
        if c not in df.columns:
            df[c] = ""

    # 정렬(오래된 -> 최신)
    # 숫자 변환
    for c in ["계약년","계약월","계약일"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
    for c in ["전용면적(㎡)","거래금액(만원)","층"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df.sort_values(["계약년","계약월","계약일","단지명","전용면적(㎡)","동","층","거래금액(만원)"], inplace=True, kind="mergesort")

    # 기록일 컬럼 채우기
    df["기록일"] = record_date

    # 행 생성: 시트 컬럼 순서로
    rows = df[APGU_SHEET_COLUMNS].astype(str).applymap(lambda x: x.strip()).values.tolist()

    # 키 집합 생성
    hmap = {c:i+1 for i,c in enumerate(APGU_SHEET_COLUMNS)}
    keys = set()
    for r in rows:
        keys.add(_row_to_key(r, hmap))
    return rows, keys, hmap

def ensure_apgu_header(ws: gspread.Worksheet):
    vals = ws.get_all_values()
    if vals and vals[0]:
        header = [h.strip() for h in vals[0]]
        if header == APGU_SHEET_COLUMNS:
            return
        # 헤더 다르면 교체
        ws.update("1:1", [APGU_SHEET_COLUMNS], value_input_option="USER_ENTERED")
    else:
        ws.update("1:1", [APGU_SHEET_COLUMNS], value_input_option="USER_ENTERED")

def append_change_log(ws: gspread.Worksheet, added: set[tuple], removed: set[tuple]):
    if not added and not removed:
        return
    now = datetime.now().strftime("%Y.%m.%d %H:%M")
    ws.append_row([f"{now} 변동"], value_input_option="USER_ENTERED")
    lines = []

    def format_line(key: tuple, tag: str) -> str:
        d = dict(zip(APGU_KEY_FIELDS, key))
        return f"({tag}) {d['계약년']}/{d['계약월']}/{d['계약일']} {d['단지명']} {d['전용면적(㎡)']}㎡ {d['동']}동 {d['층']}층 {d['거래금액(만원)']}만원 (기록일:{d['기록일']})"

    for k in sorted(added):
        lines.append([format_line(k, "신규")])
    for k in sorted(removed):
        lines.append([format_line(k, "삭제")])

    if lines:
        ws.append_rows(lines, value_input_option="USER_ENTERED")
        # 제목+항목 빨간색
        end = ws.row_count
        start = end - (len(lines)+1) + 1
        rng = f"A{start}:A{end}"
        ws.format(rng, {"textFormat":{"foregroundColor":{"red":1.0,"green":0.0,"blue":0.0}}})

def update_apgu_sheet(spread: gspread.Spreadsheet, df_all: pd.DataFrame, record_date: str):
    ws = find_ws_by_title(spread, APGU_SHEET_TITLE)
    if ws is None:
        # 기존 탭만 사용해야 하면 여기서 return; 새로 만들지 않음
        log(f"[압구정동] sheet not found: '{APGU_SHEET_TITLE}' (skip)")
        return

    ensure_apgu_header(ws)

    old_rows, old_keys, old_hmap = read_apgu_existing(ws)
    new_rows, new_keys, new_hmap = build_apgu_new(df_all, record_date)

    # 증감
    added = new_keys - old_keys
    removed = old_keys - new_keys

    # 본문 교체(헤더 제외)
    if old_rows:
        last_row = ws.row_count
        if last_row >= 2:
            ws.batch_clear([f"2:{last_row}"])

    if new_rows:
        ws.update("A2", new_rows, value_input_option="USER_ENTERED")

    append_change_log(ws, added, removed)
    log(f"[압구정동] appended {len(new_rows)} rows, +{len(added)} / -{len(removed)} (키에 기록일 포함)")

# -------------------- 가격 통계(억 단위) --------------------

def compute_price_stats(df: pd.DataFrame) -> tuple[dict[str,tuple[float,float]], dict[str,tuple[float,float]], float, float]:
    # 전국, 광역별, 서울구별, 압구정동별 가격(만원→억)
    def to_uk(v):
        try:
            return float(v)/10000.0
        except Exception:
            return np.nan

    if "거래금액(만원)" not in df.columns:
        return {}, {}, np.nan, np.nan

    df = df.copy()
    df["price"] = pd.to_numeric(df["거래금액(만원)"], errors="coerce").apply(to_uk)

    # 전국
    nat_med = float(np.nanmedian(df["price"])) if df["price"].notna().any() else np.nan
    nat_avg = float(np.nanmean(df["price"])) if df["price"].notna().any() else np.nan

    # 광역
    prov_stats = {}
    if "광역" in df.columns:
        for prov, g in df.groupby("광역"):
            s = g["price"]
            if s.notna().any():
                prov_stats[PROV_TO_SUMMARY.get(prov, prov)] = (float(np.nanmedian(s)), float(np.nanmean(s)))

    # 서울 구
    seoul_stats = {}
    seoul = df[df.get("광역","")=="서울특별시"]
    if not seoul.empty and "구" in seoul.columns:
        for gu, g in seoul.groupby("구"):
            s = g["price"]
            if s.notna().any():
                seoul_stats[gu] = (float(np.nanmedian(s)), float(np.nanmean(s)))

    # 압구정동
    apgu = seoul[seoul.get("법정동","")=="압구정동"]
    apgu_med = float(np.nanmedian(apgu["price"])) if not apgu.empty and apgu["price"].notna().any() else np.nan
    apgu_avg = float(np.nanmean(apgu["price"])) if not apgu.empty and apgu["price"].notna().any() else np.nan

    return prov_stats, seoul_stats, apgu_med, apgu_avg

# -------------------- 메인 --------------------

def main():
    log_block("main")
    artifacts_dir = Path(os.environ.get("ARTIFACTS_DIR","artifacts"))
    sheet_id = os.environ.get("SHEET_ID","").strip()
    sa_json = os.environ.get("SA_JSON","").strip()

    if not sheet_id:
        log("Error: SHEET_ID empty"); sys.exit(1)

    # 구글 인증
    if sa_json:
        creds = Credentials.from_service_account_info(json.loads(sa_json),
            scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"])
    else:
        # 로컬 테스트: sa.json 파일 찾기
        with open("sa.json","r",encoding="utf-8") as f:
            creds = Credentials.from_service_account_info(json.load(f),
                scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"])
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)
    log("[gspread] spreadsheet opened")

    # 파일 찾기
    xlsx_files = sorted(Path(artifacts_dir).rglob("*.xlsx"))
    log(f"[collect] found {len(xlsx_files)} xlsx files")

    # 월별 prev_counts 저장(전월 대비용)
    month_counts_store: dict[tuple[int,int], dict[str,int]] = {}

    # 거래요약 탭 캐시
    ws_summary = find_ws_by_title(sh, "거래요약")

    for xp in xlsx_files:
        fname = xp.name
        m = re.match(r"전국\s+(\d{2})(\d{2})_", fname)
        if not m:
            continue
        yy = int(m.group(1))  # 24, 25 ..
        mm = int(m.group(2))  # 01..12
        year = 2000 + yy if yy < 70 else 1900 + yy  # 24 -> 2024
        # 기록할 '오늘 날짜' (시트 포맷)
        write_date = today_ymd_dot(datetime.now())

        # 탭명 후보
        nat_title = f"전국 {yy}년 {mm}월"
        se_title  = f"서울 {yy}년 {mm}월"

        # 워크시트 찾기(기존 탭만 사용)
        ws_nat = find_ws_by_title(sh, nat_title)
        ws_se  = find_ws_by_title(sh, se_title)
        if ws_nat is None or ws_se is None:
            log(f"[skip] sheet not found: {nat_title if ws_nat is None else ''} {se_title if ws_se is None else ''}")
            continue

        # 파일 읽기
        df = read_xlsx_rows(xp)
        log(f"[read] {fname} rows={len(df)} cols={len(df.columns)}")

        # 건수 집계
        nat_series, se_series = summarize_nat_seoul(df)

        # 전국/서울 탭 헤더 키(열) 불러오기
        hm_nat = header_map_from_ws(ws_nat); hm_se = header_map_from_ws(ws_se)
        # 전국 탭에서 집계할 키들(헤더 교집합)
        nat_keys = [k for k in [
            "강원특별자치도","경기도","경상남도","경상북도","광주광역시","대구광역시","대전광역시","부산광역시",
            "서울특별시","세종특별자치시","울산광역시","인천광역시","전라남도","전북특별자치도","제주특별자치도",
            "충청남도","충청북도"
        ] if k in hm_nat]
        se_keys = [k for k in SEOUL_DISTRICTS if k in hm_se]

        # dict로 변환
        nat_map = {k:int(nat_series.get(k,0)) for k in nat_keys}
        se_map = {k:int(se_series.get(k,0)) for k in se_keys}

        # 기록 (총합계 열이 있으면 채움)
        write_counts(ws_nat, write_date, nat_keys, nat_map, sum_col_name="전체 개수" if "전체 개수" in hm_nat else ("총합계" if "총합계" in hm_nat else None))
        write_counts(ws_se, write_date, se_keys, se_map, sum_col_name="총합계" if "총합계" in hm_se else None)

        # 거래요약 집계 준비: 가격 통계
        prov_stats, seoul_stats, apgu_med, apgu_avg = compute_price_stats(df)

        # counts/price 맵 구성
        counts_for_summary = {}
        prices_for_summary = {}

        # 전국
        counts_for_summary["전국"] = int(len(df))
        prices_for_summary["전국"] = (np.nanmedian(pd.to_numeric(df.get("거래금액(만원)",""), errors="coerce")/10000.0), 
                                       np.nanmean(pd.to_numeric(df.get("거래금액(만원)",""), errors="coerce")/10000.0))

        # 광역
        for prov_k, (med_v, avg_v) in prov_stats.items():
            counts_for_summary[prov_k] = int((df.get("광역","")==("서울특별시" if prov_k=="서울" else next((k for k,v in PROV_TO_SUMMARY.items() if v==prov_k), prov_k))).sum()) if False else int((df.get("광역","")== next((k for k,v in PROV_TO_SUMMARY.items() if v==prov_k), prov_k)).sum())
            prices_for_summary[prov_k] = (round(med_v,2) if not math.isnan(med_v) else None,
                                          round(avg_v,2) if not math.isnan(avg_v) else None)

        # 서울 구별
        seoul_only = df[df.get("광역","")=="서울특별시"]
        for gu in SEOUL_DISTRICTS:
            cnt = int((seoul_only.get("구","")==gu).sum())
            counts_for_summary[gu] = cnt
            if gu in seoul_stats:
                med_v, avg_v = seoul_stats[gu]
                prices_for_summary[gu] = (round(med_v,2) if not math.isnan(med_v) else None,
                                          round(avg_v,2) if not math.isnan(avg_v) else None)

        # 압구정동(건수/가격)
        apgu = seoul_only[seoul_only.get("법정동","")=="압구정동"]
        counts_for_summary["압구정동"] = int(len(apgu))
        if not math.isnan(apgu_med) and not math.isnan(apgu_avg):
            prices_for_summary["압구정동"] = (round(apgu_med,2), round(apgu_avg,2))

        # 전월 대비를 위해 보관
        this_key = (year, mm)
        prev_key = (year, mm-1) if mm>1 else (year-1, 12)
        prev = month_counts_store.get(prev_key)

        # 거래요약 탭 쓰기
        if ws_summary:
            write_month_summary(ws_summary, year, mm, counts_for_summary, prices_for_summary, prev)

        month_counts_store[this_key] = counts_for_summary

        # 압구정동 탭(원본+증감, 기록일 포함)
        record_date = write_date  # 오늘 날짜 포맷
        update_apgu_sheet(sh, df, record_date)

    # where_written 로그 간단 생성(옵션)
    (LOG_DIR / "where_written.txt").write_text("completed\n", encoding="utf-8")
    log("[main] done.")


if __name__ == "__main__":
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
    except FileExistsError:
        pass
    main()
