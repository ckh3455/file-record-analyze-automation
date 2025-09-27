#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import os, re, sys, json, math
from pathlib import Path
from datetime import datetime
from collections import defaultdict

import pandas as pd
import numpy as np

import gspread
from google.oauth2.service_account import Credentials
from gspread.utils import rowcol_to_a1

# ======================= 공용 유틸/로깅 =======================

LOG_DIR = Path("analyze_report")

def _ensure_log_dir():
    if LOG_DIR.exists() and LOG_DIR.is_file():
        # 파일이면 지우고 폴더로 만든다
        try:
            LOG_DIR.unlink()
        except Exception:
            pass
    if not LOG_DIR.exists():
        LOG_DIR.mkdir(parents=True, exist_ok=True)

def log(msg: str) -> None:
    _ensure_log_dir()
    ts = datetime.now().strftime("[%H:%M:%S]")
    line = f"{ts} {msg}\n"
    # append 모드로 간단히 기록
    with open(LOG_DIR / "latest.log", "a", encoding="utf-8") as f:
        f.write(line)
    print(line, end="")

def log_block(title: str) -> None:
    log(f"[{title.upper()}]")

def today_dot(d: datetime) -> str:
    # 2025. 9. 27
    return f"{d.year}. {d.month}. {d.day}"

def normalize_title(s: str) -> str:
    return re.sub(r"\s+", "", s or "").strip()

def find_ws_by_title(spread: gspread.Spreadsheet, target: str) -> gspread.Worksheet | None:
    want = normalize_title(target)
    for ws in spread.worksheets():
        if normalize_title(ws.title) == want:
            return ws
    return None

def header_map_from_ws(ws: gspread.Worksheet) -> dict[str,int]:
    vals = ws.get_values("1:1")
    header = vals[0] if vals else []
    return {str(c).strip(): i+1 for i, c in enumerate(header)}

def find_date_col(ws: gspread.Worksheet) -> tuple[int,int]:
    vals = ws.get_values("1:1")
    header = vals[0] if vals else []
    for i, name in enumerate(header, start=1):
        if str(name).strip() == "날짜":
            return (1, i)
    return (1, 1)

def find_or_append_date_row(ws: gspread.Worksheet, date_str: str) -> int:
    header_row, date_col = find_date_col(ws)
    col_letter = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"[date_col-1]
    col_vals = ws.get_values(f"{col_letter}{header_row+1}:{col_letter}")
    target = date_str.strip()
    for idx, r in enumerate(col_vals, start=header_row+1):
        v = (r[0] if r else "").strip()
        if v == target:
            return idx
    # append
    ws.append_row([""], value_input_option="USER_ENTERED")
    row_idx = ws.row_count
    ws.update(f"{col_letter}{row_idx}", [[target]], value_input_option="USER_ENTERED")
    return row_idx

# ======================= 데이터 처리 =======================

def read_xlsx_rows(xlsx_path: Path) -> pd.DataFrame:
    df = pd.read_excel(xlsx_path, sheet_name="data", dtype=str)
    df.columns = [str(c).strip() for c in df.columns]
    df = df.fillna("")
    return df

def summarize_nat_seoul(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    if "광역" not in df.columns and "시도" in df.columns:
        df = df.rename(columns={"시도": "광역"})
    if "구" not in df.columns and "시군구" in df.columns:
        df = df.rename(columns={"시군구": "구"})

    nat = df.groupby("광역", dropna=False).size() if "광역" in df.columns else pd.Series(dtype=int)
    seoul = df.loc[df.get("광역","") == "서울특별시"]
    seoul = seoul.groupby("구", dropna=False).size() if "구" in seoul.columns else pd.Series(dtype=int)
    return nat, seoul

def write_counts(ws: gspread.Worksheet, date_str: str, header_keys: list[str], values_map: dict[str,int], sum_col_name: str | None = None):
    row_idx = find_or_append_date_row(ws, date_str)
    hmap = header_map_from_ws(ws)
    total = 0
    updates = []
    for k in header_keys:
        col = hmap.get(k)
        if not col:
            continue
        val = int(values_map.get(k, 0))
        total += val
        a1 = rowcol_to_a1(row_idx, col)
        updates.append((a1, [[val]]))
    if sum_col_name:
        c = hmap.get(sum_col_name)
        if c:
            updates.append((rowcol_to_a1(row_idx, c), [[total]]))
    for a1, v in updates:
        ws.update(a1, v, value_input_option="USER_ENTERED")

# ======================= 거래요약 =======================

SUMMARY_COLS = [
    "년월","구분","전국","서울","강남구","압구정동","경기도","인천광역시","세종시","서초구","송파구","용산구",
    "강동구","성동구","마포구","양천구","동작구","영등포구","종로구","광진구","강서구","강북구","관악구","구로구",
    "금천구","도봉구","동대문구","서대문구","성북구","은평구","중구","중랑구","부산","대구","광주","대전",
    "강원도","경남","경북","전남","전북","충남","충북","제주"
]
PROV_TO_SUMMARY = {
    "서울특별시":"서울","세종특별자치시":"세종시","강원특별자치도":"강원도",
    "경기도":"경기도","인천광역시":"인천광역시","경상남도":"경남","경상북도":"경북",
    "광주광역시":"광주","대구광역시":"대구","대전광역시":"대전","부산광역시":"부산","울산광역시":"울산",
    "전라남도":"전남","전북특별자치도":"전북","제주특별자치도":"제주","충청남도":"충남","충청북도":"충북"
}
SEOUL_DISTRICTS = [
    "강남구","강동구","강북구","강서구","관악구","광진구","구로구","금천구","노원구","도봉구","동대문구",
    "동작구","마포구","서대문구","서초구","성동구","성북구","송파구","양천구","영등포구","용산구",
    "은평구","종로구","중구","중랑구"
]
def ym_str(y: int, m: int) -> str:
    return f"{str(y)[-2:]}/{m:02d}"

def compute_price_stats(df: pd.DataFrame):
    def to_uk(v):
        try:
            return float(v)/10000.0
        except Exception:
            return np.nan
    if "거래금액(만원)" not in df.columns:
        return {}, {}, np.nan, np.nan
    d = df.copy()
    d["price"] = pd.to_numeric(d["거래금액(만원)"], errors="coerce").apply(to_uk)
    nat_med = float(np.nanmedian(d["price"])) if d["price"].notna().any() else np.nan
    nat_avg = float(np.nanmean(d["price"])) if d["price"].notna().any() else np.nan

    prov_stats = {}
    if "광역" in d.columns:
        for prov, g in d.groupby("광역"):
            s = g["price"]
            if s.notna().any():
                prov_stats[PROV_TO_SUMMARY.get(prov, prov)] = (float(np.nanmedian(s)), float(np.nanmean(s)))

    seoul_stats = {}
    seoul = d[d.get("광역","")=="서울특별시"]
    if not seoul.empty and "구" in seoul.columns:
        for gu, g in seoul.groupby("구"):
            s = g["price"]
            if s.notna().any():
                seoul_stats[gu] = (float(np.nanmedian(s)), float(np.nanmean(s)))

    apgu = seoul[seoul.get("법정동","")=="압구정동"]
    apgu_med = float(np.nanmedian(apgu["price"])) if not apgu.empty and apgu["price"].notna().any() else np.nan
    apgu_avg = float(np.nanmean(apgu["price"])) if not apgu.empty and apgu["price"].notna().any() else np.nan
    return prov_stats, seoul_stats, apgu_med, apgu_avg

def write_month_summary(ws: gspread.Worksheet, y: int, m: int,
                        counts: dict[str,int], prices: dict[str,tuple[float,float]],
                        prev_counts: dict[str,int] | None):
    vals = ws.get_all_values()
    header = vals[0] if vals else SUMMARY_COLS
    col_map = {c:i+1 for i,c in enumerate(header)}
    ym = ym_str(y, m)

    row_by_type = {}
    for r in range(2, len(vals)+1):
        if vals[r-1][0].strip() == ym:
            row_by_type[vals[r-1][1].strip()] = r

    def ensure_row(row_type: str) -> int:
        r = row_by_type.get(row_type)
        if r: return r
        ws.append_row([ym, row_type], value_input_option="USER_ENTERED")
        r = ws.row_count
        row_by_type[row_type] = r
        return r

    def put(row_type: str, col_name: str, value, bold=False, color=None, number_fmt=None):
        r = ensure_row(row_type)
        c = col_map.get(col_name)
        if not c: return
        a1 = rowcol_to_a1(r, c)
        ws.update(a1, [[value]], value_input_option="USER_ENTERED")
        fmt = {}
        if bold:
            fmt.setdefault("textFormat", {})["bold"] = True
        if color:
            fmt.setdefault("textFormat", {})["foregroundColor"] = color
        if number_fmt:
            fmt["numberFormat"] = {"type":"NUMBER","pattern": number_fmt}
        if fmt:
            ws.format(a1, fmt)

    target_cols = ["전국"] + list(PROV_TO_SUMMARY.values()) + SEOUL_DISTRICTS + ["압구정동"]
    # 거래건수(볼드), 중앙값/평균가(소수점2자리)
    for col in target_cols:
        cnt = counts.get(col, 0)
        med, avg = prices.get(col, (None, None))
        put("거래건수", col, cnt, bold=True)
        if med is not None:
            put("중앙값(단위:억)", col, round(med,2), number_fmt="0.00")
        if avg is not None:
            put("평균가(단위:억)", col, round(avg,2), number_fmt="0.00")

    # 전월대비 색/기호
    if prev_counts:
        for col in target_cols:
            cur = counts.get(col, 0)
            prev = prev_counts.get(col, 0)
            diff = cur - prev
            if diff > 0:
                txt = f"+{diff}"
                color = {"red":0, "green":0, "blue":1}
            elif diff < 0:
                txt = f"{diff}"  # 음수 그대로
                color = {"red":1, "green":0, "blue":0}
            else:
                txt = "0"
                color = None
            put("전월대비 건수증감", col, txt, color=color)

# ======================= 압구정동 =======================

APGU_SHEET_TITLE = "압구정동"
APGU_KEY_FIELDS = ["계약년","계약월","계약일","단지명","전용면적(㎡)","동","층","거래금액(만원)","기록일"]
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

def read_apgu_existing(ws: gspread.Worksheet):
    vals = ws.get_all_values()
    if not vals:
        return [], set(), {}
    header = [h.strip() for h in vals[0]]
    hmap = {h:i+1 for i,h in enumerate(header)}
    rows = vals[1:]
    keys = set(_row_to_key(r, hmap) for r in rows)
    return rows, keys, hmap

def build_apgu_new(df_all: pd.DataFrame, record_date: str):
    d = df_all.copy()
    if "광역" not in d.columns and "시도" in d.columns:
        d = d.rename(columns={"시도":"광역"})
    if "구" not in d.columns and "시군구" in d.columns:
        d = d.rename(columns={"시군구":"구"})
    d = d[(d.get("광역","")=="서울특별시") & (d.get("법정동","")=="압구정동")].copy()
    if d.empty:
        return [], set(), {c:i+1 for i,c in enumerate(APGU_SHEET_COLUMNS)}

    for c in APGU_SHEET_COLUMNS:
        if c not in d.columns:
            d[c] = ""

    # 숫자화 및 정렬(오래된→최근)
    for c in ["계약년","계약월","계약일"]:
        d[c] = pd.to_numeric(d[c], errors="coerce").fillna(0).astype(int)
    for c in ["전용면적(㎡)","거래금액(만원)","층"]:
        d[c] = pd.to_numeric(d[c], errors="coerce")

    d.sort_values(["계약년","계약월","계약일","단지명","전용면적(㎡)","동","층","거래금액(만원)"], inplace=True, kind="mergesort")
    d["기록일"] = record_date

    rows = d[APGU_SHEET_COLUMNS].astype(str).applymap(lambda x: x.strip()).values.tolist()
    hmap = {c:i+1 for i,c in enumerate(APGU_SHEET_COLUMNS)}
    keys = set(_row_to_key(r, hmap) for r in rows)
    return rows, keys, hmap

def ensure_apgu_header(ws: gspread.Worksheet):
    vals = ws.get_all_values()
    if vals and vals[0]:
        header = [h.strip() for h in vals[0]]
        if header == APGU_SHEET_COLUMNS:
            return
    ws.update("1:1", [APGU_SHEET_COLUMNS], value_input_option="USER_ENTERED")

def append_change_log(ws: gspread.Worksheet, added: set[tuple], removed: set[tuple]):
    if not added and not removed:
        return
    now = datetime.now().strftime("%Y.%m.%d %H:%M")
    ws.append_row([f"{now} 변동"], value_input_option="USER_ENTERED")
    lines = []
    def fmt(key: tuple, tag: str) -> str:
        d = dict(zip(APGU_KEY_FIELDS, key))
        return (f"({tag}) {d['계약년']}/{d['계약월']}/{d['계약일']} {d['단지명']} "
                f"{d['전용면적(㎡)']}㎡ {d['동']}동 {d['층']}층 {d['거래금액(만원)']}만원 "
                f"(기록일:{d['기록일']})")
    for k in sorted(added):
        lines.append([fmt(k, "신규")])
    for k in sorted(removed):
        lines.append([fmt(k, "삭제")])
    if lines:
        ws.append_rows(lines, value_input_option="USER_ENTERED")
        end = ws.row_count
        start = end - (len(lines)+1) + 1
        ws.format(f"A{start}:A{end}", {"textFormat":{"foregroundColor":{"red":1.0,"green":0.0,"blue":0.0}}})

def update_apgu_sheet(spread: gspread.Spreadsheet, df_all: pd.DataFrame, record_date: str):
    ws = find_ws_by_title(spread, APGU_SHEET_TITLE)
    if ws is None:
        log(f"[압구정동] sheet not found: '{APGU_SHEET_TITLE}' (skip)")
        return
    ensure_apgu_header(ws)
    old_rows, old_keys, _ = read_apgu_existing(ws)
    new_rows, new_keys, _ = build_apgu_new(df_all, record_date)

    added = new_keys - old_keys
    removed = old_keys - new_keys

    # 본문 교체(헤더 제외)
    last = ws.row_count
    if last >= 2:
        ws.batch_clear([f"2:{last}"])
    if new_rows:
        ws.update("A2", new_rows, value_input_option="USER_ENTERED")

    append_change_log(ws, added, removed)
    log(f"[압구정동] appended {len(new_rows)} rows, +{len(added)} / -{len(removed)}")

# ======================= 메인 =======================

def main():
    log_block("main")

    artifacts_dir = Path(os.environ.get("ARTIFACTS_DIR","artifacts"))
    sheet_id = os.environ.get("SHEET_ID","").strip()
    sa_json = os.environ.get("SA_JSON","").strip()

    if not sheet_id:
        log("Error: SHEET_ID empty")
        sys.exit(1)

    # 인증
    if sa_json:
        creds = Credentials.from_service_account_info(
            json.loads(sa_json),
            scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
        )
    else:
        with open("sa.json","r",encoding="utf-8") as f:
            creds = Credentials.from_service_account_info(
                json.load(f),
                scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
            )
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)
    log("[gspread] spreadsheet opened")

    # 파일 나열
    xlsx_files = sorted(artifacts_dir.rglob("*.xlsx"))
    log(f"[collect] found {len(xlsx_files)} xlsx files")

    ws_summary = find_ws_by_title(sh, "거래요약")
    month_counts_store: dict[tuple[int,int], dict[str,int]] = {}

    for xp in xlsx_files:
        m = re.match(r"전국\s+(\d{2})(\d{2})_", xp.name)
        if not m:
            continue
        yy = int(m.group(1)); mm = int(m.group(2))
        year = 2000 + yy if yy < 70 else 1900 + yy
        write_date = today_dot(datetime.now())

        nat_title = f"전국 {yy}년 {mm}월"
        se_title  = f"서울 {yy}년 {mm}월"
        ws_nat = find_ws_by_title(sh, nat_title)
        ws_se  = find_ws_by_title(sh, se_title)
        if ws_nat is None or ws_se is None:
            log(f"[skip] sheet not found: {nat_title if ws_nat is None else ''} {se_title if ws_se is None else ''}")
            continue

        df = read_xlsx_rows(xp)
        log(f"[read] {xp.name} rows={len(df)} cols={len(df.columns)}")

        nat_series, se_series = summarize_nat_seoul(df)

        hm_nat = header_map_from_ws(ws_nat); hm_se = header_map_from_ws(ws_se)
        nat_keys = [k for k in [
            "강원특별자치도","경기도","경상남도","경상북도","광주광역시","대구광역시","대전광역시","부산광역시",
            "서울특별시","세종특별자치시","울산광역시","인천광역시","전라남도","전북특별자치도",
            "제주특별자치도","충청남도","충청북도"
        ] if k in hm_nat]
        se_keys = [k for k in SEOUL_DISTRICTS if k in hm_se]

        nat_map = {k:int(nat_series.get(k,0)) for k in nat_keys}
        se_map  = {k:int(se_series.get(k,0)) for k in se_keys}

        sum_name_nat = "전체 개수" if "전체 개수" in hm_nat else ("총합계" if "총합계" in hm_nat else None)
        sum_name_se  = "총합계" if "총합계" in hm_se else None
        write_counts(ws_nat, write_date, nat_keys, nat_map, sum_col_name=sum_name_nat)
        write_counts(ws_se,  write_date, se_keys,  se_map,  sum_col_name=sum_name_se)

        # 거래요약
        prov_stats, seoul_stats, apgu_med, apgu_avg = compute_price_stats(df)
        counts_for_summary = {}
        prices_for_summary = {}

        counts_for_summary["전국"] = int(len(df))
        if "거래금액(만원)" in df.columns:
            price_series = pd.to_numeric(df["거래금액(만원)"], errors="coerce")/10000.0
            prices_for_summary["전국"] = (float(np.nanmedian(price_series)) if price_series.notna().any() else None,
                                          float(np.nanmean(price_series)) if price_series.notna().any() else None)

        for prov_k, (med_v, avg_v) in prov_stats.items():
            origin_name = next((k for k,v in PROV_TO_SUMMARY.items() if v==prov_k), prov_k)
            counts_for_summary[prov_k] = int((df.get("광역","")==origin_name).sum())
            prices_for_summary[prov_k] = (round(med_v,2) if not math.isnan(med_v) else None,
                                          round(avg_v,2) if not math.isnan(avg_v) else None)

        seoul_only = df[df.get("광역","")=="서울특별시"]
        for gu in SEOUL_DISTRICTS:
            cnt = int((seoul_only.get("구","")==gu).sum())
            counts_for_summary[gu] = cnt
            if gu in seoul_stats:
                med_v, avg_v = seoul_stats[gu]
                prices_for_summary[gu] = (round(med_v,2) if not math.isnan(med_v) else None,
                                          round(avg_v,2) if not math.isnan(avg_v) else None)

        counts_for_summary["압구정동"] = int(len(seoul_only[seoul_only.get("법정동","")=="압구정동"]))
        if not math.isnan(apgu_med) and not math.isnan(apgu_avg):
            prices_for_summary["압구정동"] = (round(apgu_med,2), round(apgu_avg,2))

        this_key = (year, mm)
        prev_key = (year, mm-1) if mm>1 else (year-1, 12)
        prev = month_counts_store.get(prev_key)

        if ws_summary:
            write_month_summary(ws_summary, year, mm, counts_for_summary, prices_for_summary, prev)
        month_counts_store[this_key] = counts_for_summary

        # 압구정동 원본+증감 (기록일 포함)
        update_apgu_sheet(sh, df, write_date)

    with open(LOG_DIR / "where_written.txt", "w", encoding="utf-8") as f:
        f.write("completed\n")
    log("[main] done.")

if __name__ == "__main__":
    _ensure_log_dir()
    main()
