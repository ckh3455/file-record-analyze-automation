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

LOG_DIR = Path("analyze_report")
ARTIFACTS_DIR = os.environ.get("ARTIFACTS_DIR", "artifacts")
SUMMARY_SHEET_NAME = "거래요약"

SUMMARY_COLS = [
    "전국","서울",
    "강남구","압구정동",
    "경기도","인천광역시","세종시","울산",
    "서초구","송파구","용산구","강동구","성동구","마포구","양천구","동작구","영등포구","종로구","광진구",
    "강서구","강북구","관악구","구로구","금천구","도봉구","노원구",
    "동대문구","서대문구","성북구","은평구","중구","중랑구",
    "부산","대구","광주","대전","강원도","경남","경북","전남","전북","충남","충북","제주"
]

PROV_MAP = {
    "서울특별시":"서울",
    "세종특별자치시":"세종시",
    "강원특별자치도":"강원도",
    "경기도":"경기도",
    "인천광역시":"인천광역시",
    "부산광역시":"부산",
    "대구광역시":"대구",
    "광주광역시":"광주",
    "대전광역시":"대전",
    "울산광역시":"울산",
    "전라남도":"전남",
    "전북특별자치도":"전북",
    "경상남도":"경남",
    "경상북도":"경북",
    "충청남도":"충남",
    "충청북도":"충북",
    "제주특별자치도":"제주",
}

APGU_BASE_COLS = [
    "광역","구","법정동","리","번지","본번","부번","단지명","전용면적(㎡)",
    "계약년","계약월","계약일","거래금액(만원)","동","층",
    "매수자","매도자","건축년도","도로명","해제사유발생일","거래유형",
    "중개사소재지","등기일자","주택유형"
]

def _ensure_logdir():
    try:
        if LOG_DIR.exists() and not LOG_DIR.is_dir():
            LOG_DIR.unlink()
        LOG_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

_ensure_logdir()
RUN_LOG = LOG_DIR / "latest.log"

def log(msg: str):
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    print(line)
    try:
        with RUN_LOG.open("a", encoding="utf-8") as f:
            f.write(line+"\n")
    except Exception:
        pass

_LAST = 0.0
def _throttle(sec=0.35):
    global _LAST
    now = time.time()
    if now - _LAST < sec:
        time.sleep(sec - (now - _LAST))
    _LAST = time.time()

def _retry(fn, *a, **kw):
    base = 0.8
    for i in range(7):
        try:
            _throttle()
            return fn(*a, **kw)
        except APIError as e:
            s = str(e)
            if any(x in s for x in ("429","500","502","503")):
                time.sleep(base*(2**i) + random.uniform(0,0.25))
                continue
            raise

def a1_col(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n-1, 26)
        s = chr(65+r) + s
    return s

def values_batch_update(ws: gspread.Worksheet, data: List[Dict]):
    body = {"valueInputOption": "USER_ENTERED", "data": data}
    return _retry(ws.spreadsheet.values_batch_update, body=body)

def batch_format(ws: gspread.Worksheet, requests: List[dict]):
    if not requests: return
    _retry(ws.spreadsheet.batch_update, {"requests": requests})

def fuzzy_ws(sh: gspread.Spreadsheet, wanted: str) -> Optional[gspread.Worksheet]:
    tgt = re.sub(r"\s+","", wanted)
    for ws in sh.worksheets():
        if re.sub(r"\s+","", ws.title) == tgt:
            log(f"[ws] matched (exact): '{ws.title}'")
            return ws
    for ws in sh.worksheets():
        if re.sub(r"\s+","", ws.title) == re.sub(r"\s+","", wanted):
            log(f"[ws] matched (nospace): '{ws.title}' (wanted='{wanted}')")
            return ws
    return None

def read_month_df(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name="data", dtype=str)
    df = df.fillna("")
    for c in ["계약년","계약월","계약일","거래금액(만원)"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def eok_series(ser) -> pd.Series:
    s = pd.to_numeric(ser, errors="coerce").dropna()
    if s.empty: return pd.Series([], dtype=float)
    return s / 10000.0

def round2(v) -> str:
    try:
        return f"{float(v):.2f}"
    except Exception:
        return ""

# --- FIX: 스칼라/Series 모두 지원 ---
def normalize_key(s):
    """공백 제거 + 좌우공백 제거. 스칼라/Series 모두 처리."""
    if isinstance(s, pd.Series):
        return s.astype(str).str.replace(r"\s+","", regex=True).str.strip()
    return re.sub(r"\s+","", str(s) if s is not None else "").strip()

def agg_national_for_headers(df: pd.DataFrame, header: List[str]) -> Dict[str,int]:
    counts: Dict[str,int] = {}
    if "광역" not in df.columns:
        return {h:0 for h in header}
    prov_std = df["광역"].map(lambda x: PROV_MAP.get(str(x), str(x)))
    grp = prov_std.groupby(prov_std).size()
    for h in header:
        if not h or h == "날짜" or h == "총합계":
            continue
        v = int(grp[h]) if h in grp.index else 0
        counts[h] = v
    return counts

def agg_seoul_for_headers(df: pd.DataFrame, header: List[str]) -> Dict[str,int]:
    out: Dict[str,int] = {}
    if "광역" not in df.columns or "구" not in df.columns:
        return {h:0 for h in header}
    se_mask = normalize_key(df["광역"]) == normalize_key("서울특별시")
    se = df[se_mask]
    if se.empty:
        return {h:0 for h in header}
    grp = se["구"].astype(str).str.strip().groupby(se["구"].astype(str).str.strip()).size()
    for h in header:
        if not h or h == "날짜" or h == "총합계":
            continue
        out[h] = int(grp[h]) if h in grp.index else 0
    return out

def agg_all_stats(df: pd.DataFrame):
    counts = {col:0 for col in SUMMARY_COLS}
    med = {col:"" for col in SUMMARY_COLS}
    mean = {col:"" for col in SUMMARY_COLS}

    all_eok = eok_series(df.get("거래금액(만원)", []))
    counts["전국"] = int(len(df))
    if not all_eok.empty:
        med["전국"] = round2(all_eok.median())
        mean["전국"] = round2(all_eok.mean())

    if "광역" in df.columns:
        for prov, sub in df.groupby("광역"):
            prov_std = PROV_MAP.get(str(prov), str(prov))
            if prov_std in counts:
                counts[prov_std] += int(len(sub))
                s = eok_series(sub.get("거래금액(만원)", []))
                if not s.empty:
                    med[prov_std] = round2(s.median())
                    mean[prov_std] = round2(s.mean())

    se_mask = "광역" in df.columns and (normalize_key(df["광역"]) == normalize_key("서울특별시"))
    seoul = df[se_mask].copy() if isinstance(se_mask, pd.Series) else pd.DataFrame(columns=df.columns)
    counts["서울"] = int(len(seoul))
    if len(seoul)>0:
        s = eok_series(seoul["거래금액(만원)"])
        if not s.empty:
            med["서울"] = round2(s.median())
            mean["서울"] = round2(s.mean())

    if "구" in seoul.columns and not seoul.empty:
        for gu, sub in seoul.groupby("구"):
            gu = str(gu).strip()
            if gu in counts:
                counts[gu] += int(len(sub))
                s = eok_series(sub["거래금액(만원)"])
                if not s.empty:
                    med[gu] = round2(s.median())
                    mean[gu] = round2(s.mean())

    ap = seoul[seoul.get("법정동","")=="압구정동"] if not seoul.empty else pd.DataFrame()
    counts["압구정동"] = int(len(ap))
    if len(ap)>0:
        s = eok_series(ap["거래금액(만원)"])
        med["압구정동"] = round2(s.median())
        mean["압구정동"] = round2(s.mean())

    return counts, med, mean

def kdate(d: datetime) -> str:
    return f"{d.year}. {d.month}. {d.day}"

def find_or_append_date_row(ws: gspread.Worksheet, date_label: str) -> int:
    vals = _retry(ws.get_all_values)
    if not vals:
        return 2
    for i, row in enumerate(vals[1:], start=2):
        if (len(row)>0) and str(row[0]).strip()==date_label:
            return i
    return len(vals)+1

def write_month_sheet(ws, date_label: str, header: List[str], values_by_colname: Dict[str,int]):
    hmap = {str(h).strip(): idx+1 for idx,h in enumerate(header) if str(h).strip()}
    row_idx = find_or_append_date_row(ws, date_label)
    payload = [{"range": f"A{row_idx}", "values": [[date_label]]}]
    for h in header:
        if not h or h=="날짜":
            continue
        v = values_by_colname.get(h, 0)
        c = hmap.get(h)
        if c:
            payload.append({"range": f"{a1_col(c)}{row_idx}", "values": [[int(v)]]})
    if "총합계" in hmap:
        v = values_by_colname.get("총합계")
        if v is None:
            v = values_by_colname.get("전국")
            if v is None:
                v = 0
                for h in header:
                    if h and h not in ("날짜","총합계"):
                        v += int(values_by_colname.get(h, 0) or 0)
        payload.append({"range": f"{a1_col(hmap['총합계'])}{row_idx}", "values": [[int(v)]]})
    if payload:
        values_batch_update(ws, payload)
        log(f"[ws] {ws.title} -> {date_label} row={row_idx}")

def ym_from_filename(fname: str):
    m = re.search(r"(\d{2})(\d{2})_", fname)
    if not m: return None, None, None
    yy, mm = m.group(1), int(m.group(2))
    return f"전국 20{yy}년 {mm}월", f"서울 20{yy}년 {mm}월", f"{yy}/{mm}"

def prev_ym(ym: str) -> str:
    yy, mm = ym.split("/")
    y = int(yy); m = int(mm)
    if m == 1: return f"{y-1}/12"
    return f"{yy}/{m-1}"

def find_summary_row(ws, ym: str, label: str) -> int:
    vals = _retry(ws.get_all_values)
    if not vals:
        return 2
    for i, row in enumerate(vals[1:], start=2):
        a = str(row[0]).strip() if len(row)>0 else ""
        b = str(row[1]).strip() if len(row)>1 else ""
        if a==ym and b==label:
            return i
    return len(vals)+1

def put_summary_line(ws, row_idx: int, ym: str, label: str, line_map: dict):
    if "." in str(ym):
        log(f"[summary/guard] skip writing because ym looks like a date: {ym}")
        return
    header = _retry(ws.row_values, 1)
    if not header:
        _retry(ws.update, [["년월","구분"] + SUMMARY_COLS], "A1")
        header = ["년월","구분"] + SUMMARY_COLS
    hmap = {h:i+1 for i,h in enumerate(header)}
    payload = [
        {"range": f"A{row_idx}", "values": [[ym]]},
        {"range": f"B{row_idx}", "values": [[label]]},
    ]
    for c in SUMMARY_COLS:
        if c in hmap:
            payload.append({"range": f"{a1_col(hmap[c])}{row_idx}","values": [[line_map.get(c,"")]]})
    values_batch_update(ws, payload)

def color_diff_line(ws, row_idx: int, diff_line: dict, header: List[str]):
    hmap = {h:i+1 for i,h in enumerate(header)}
    reqs = []
    for k, v in diff_line.items():
        if k not in hmap: 
            continue
        if v == "" or v == "0":
            continue
        r,g,b = (0.0,0.35,1.0) if str(v).startswith("+") else (1.0,0.0,0.0)
        reqs.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws.id,
                    "startRowIndex": row_idx-1, "endRowIndex": row_idx,
                    "startColumnIndex": hmap[k]-1, "endColumnIndex": hmap[k]
                },
                "cell": {
                    "userEnteredFormat": {
                        "textFormat": {"foregroundColor": {"red": r, "green": g, "blue": b}}
                    }
                },
                # ✅ fields는 repeatCell 내부에!
                "fields": "userEnteredFormat.textFormat.foregroundColor"
            }
        })
    batch_format(ws, reqs)


def write_month_summary(ws, y: int, m: int, counts: dict, med: dict, mean: dict, prev_counts: Optional[dict]):
    ym = f"{str(y%100).zfill(2)}/{m}"
    r1 = find_summary_row(ws, ym, "거래건수"); put_summary_line(ws, r1, ym, "거래건수", counts); log(f"[summary] {ym} 거래건수 -> row={r1}")
    r2 = find_summary_row(ws, ym, "중앙값(단위:억)"); put_summary_line(ws, r2, ym, "중앙값(단위:억)", med); log(f"[summary] {ym} 중앙값 -> row={r2}")
    r3 = find_summary_row(ws, ym, "평균가(단위:억)"); put_summary_line(ws, r3, ym, "평균가(단위:억)", mean); log(f"[summary] {ym} 평균가 -> row={r3}")
    diffs = {}
    if prev_counts:
        for c in SUMMARY_COLS:
            cur = int(counts.get(c,0) or 0); prv = int(prev_counts.get(c,0) or 0)
            d = cur - prv; diffs[c] = f"+{d}" if d>0 else (str(d) if d<0 else "0")
    else:
        diffs = {c:"" for c in SUMMARY_COLS}
    r4 = find_summary_row(ws, ym, "전월대비 건수증감")
    put_summary_line(ws, r4, ym, "전월대비 건수증감", diffs)
    header = _retry(ws.row_values, 1); color_diff_line(ws, r4, diffs, header)
    log(f"[summary] {ym} 전월대비 -> row={r4}")
    # 예상건수는 기록하지 않음

def _apgu_norm(v) -> str:
    return "" if v is None else str(v).strip()

def _apgu_key_from_row_values(values: List[str], header: List[str]) -> str:
    idx = {h:i for i,h in enumerate(header)}
    parts = []
    for h in APGU_BASE_COLS:
        i = idx.get(h, None)
        parts.append(_apgu_norm(values[i] if (i is not None and i < len(values)) else "")) 
    return "|".join(parts)

def _ensure_rows(ws: gspread.Worksheet, need_end_row: int):
    if need_end_row > ws.row_count:
        _retry(ws.add_rows, need_end_row - ws.row_count)

def fmt_kdate(d: date) -> str:
    return f"{d.year}. {d.month}. {d.day}"

def upsert_apgu_verbatim(ws: gspread.Worksheet, df_all: pd.DataFrame, run_day: date):
    df = df_all[df_all.get("법정동","")=="압구정동"].copy()
    if df.empty:
        log("[압구정동] no rows"); return
    for c in APGU_BASE_COLS:
        if c not in df.columns: df[c] = ""
    for c in ["계약년","계약월","계약일"]:
        if c not in df.columns: df[c] = ""
    df = df.sort_values(["계약년","계약월","계약일"], ascending=[True,True,True], kind="mergesort")

    vals = _retry(ws.get_all_values) or []
    if not vals:
        _retry(ws.update, [APGU_BASE_COLS], "A1"); vals = [APGU_BASE_COLS]
    header = vals[0]
    if header != APGU_BASE_COLS:
        _retry(ws.update, [APGU_BASE_COLS], "A1"); header = APGU_BASE_COLS

    all_now = _retry(ws.get_all_values) or [header]
    body = all_now[1:]
    base_rows_old: List[List[str]] = []
    for r in body:
        if r and r[0] in ("변경구분","(신규)","(삭제)"): break
        base_rows_old.append((r + [""]*len(header))[:len(header)])

    base_rows_new: List[List[str]] = []
    for _, row in df.iterrows():
        base_rows_new.append([_apgu_norm(row.get(c, "")) for c in APGU_BASE_COLS])

    start = 2
    end = start + len(base_rows_new) - 1
    if end < start: end = start
    _ensure_rows(ws, end)
    _retry(ws.update, base_rows_new, f"A{start}:{a1_col(len(header))}{end}")
    log(f"[압구정동] base rows written: {len(base_rows_new)}")

    old_keys = {_apgu_key_from_row_values(r, header) for r in base_rows_old}
    new_keys = {_apgu_key_from_row_values(r, header) for r in base_rows_new}
    added_keys = sorted(list(new_keys - old_keys))
    removed_keys = sorted(list(old_keys - new_keys))
    if not added_keys and not removed_keys:
        log("[압구정동] changes: none"); return

    def _rowmap(rows: List[List[str]]):
        m={}; 
        for r in rows: m[_apgu_key_from_row_values(r, header)] = r
        return m
    new_map = _rowmap(base_rows_new); old_map = _rowmap(base_rows_old)

    change_header = ["변경구분","변경일"] + APGU_BASE_COLS
    change_rows: List[List[str]] = [change_header]
    today_str = fmt_kdate(run_day)
    for k in added_keys:   change_rows.append(["(신규)", today_str] + new_map[k])
    for k in removed_keys: change_rows.append(["(삭제)", today_str] + old_map[k])

    start_chg = end + 1
    end_chg = start_chg + len(change_rows) - 1
    _ensure_rows(ws, end_chg)
    _retry(ws.update, change_rows, f"A{start_chg}:{a1_col(len(change_header))}{end_chg}")

        req = {
        "repeatCell": {
            "range": {
                "sheetId": ws.id,
                "startRowIndex": start_chg-1,
                "endRowIndex": end_chg,
                "startColumnIndex": 0,
                "endColumnIndex": len(change_header)
            },
            "cell": {
                "userEnteredFormat": {
                    "textFormat": {"foregroundColor": {"red": 1.0, "green": 0.0, "blue": 0.0}}
                }
            },
            # ✅ 여기도 repeatCell 내부로 이동
            "fields": "userEnteredFormat.textFormat.foregroundColor"
        }
    }
    batch_format(ws, [req])

    log(f"[압구정동] changes: 신규={len(added_keys)} 삭제={len(removed_keys)}")

def main():
    try: RUN_LOG.write_text("", encoding="utf-8")
    except Exception: pass

    log("[MAIN]")
    log(f"artifacts_dir={ARTIFACTS_DIR}")

    sa_json = os.environ.get("SA_JSON","").strip()
    sa_path = os.environ.get("SA_PATH","sa.json")
    if sa_json:
        creds = Credentials.from_service_account_info(
            json.loads(sa_json),
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
    sh = _retry(gc.open_by_key, os.environ.get("SHEET_ID","").strip())
    log("[gspread] spreadsheet opened")

    files = sorted(Path(ARTIFACTS_DIR).rglob("전국 *.xlsx"))
    log(f"[collect] found {len(files)} xlsx files")

    month_cache = {}
    today_label = kdate(datetime.now())
    run_day = datetime.now().date()
    apgu_all: List[pd.DataFrame] = []

    for p in files:
        nat_title, se_title, ym = ym_from_filename(p.name)
        if not ym: continue
        log(f"[file] {p.name} -> nat='{nat_title}' / seoul='{se_title}' / ym={ym}")

        df = read_month_df(p)
        log(f"[read] rows={len(df)} cols={len(df.columns)}")

        counts, med, mean = agg_all_stats(df)
        month_cache[ym] = {"counts": counts, "med": med, "mean": mean}

        ws_nat = fuzzy_ws(sh, nat_title) or fuzzy_ws(sh, nat_title.replace("20","",1))
        if ws_nat:
            header_nat = _retry(ws_nat.row_values, 1)
            nat_counts = agg_national_for_headers(df, header_nat)
            write_month_sheet(ws_nat, today_label, header_nat, nat_counts)

        ws_se = fuzzy_ws(sh, se_title) or fuzzy_ws(sh, se_title.replace("20","",1))
        if ws_se:
            header_se = _retry(ws_se.row_values, 1)
            se_counts = agg_seoul_for_headers(df, header_se)
            write_month_sheet(ws_se, today_label, header_se, se_counts)

        ap = df[(normalize_key(df.get("광역","")) == normalize_key("서울특별시")) &
                (df.get("법정동","")=="압구정동")].copy()
        if not ap.empty:
            apgu_all.append(ap)

    ws_sum = fuzzy_ws(sh, SUMMARY_SHEET_NAME)
    if ws_sum and month_cache:
        def ym_key(ym): 
            yy, mm = ym.split("/"); return (int(yy), int(mm))
        for ym in sorted(month_cache.keys(), key=ym_key):
            cur = month_cache[ym]; prv = month_cache.get(prev_ym(ym))
            write_month_summary(
                ws_sum,
                2000 + int(ym.split("/")[0]),
                int(ym.split("/")[1]),
                cur["counts"], cur["med"], cur["mean"],
                prv["counts"] if prv else None
            )
            time.sleep(0.2)

    if apgu_all:
        ws_ap = fuzzy_ws(sh, "압구정동")
        if ws_ap:
            all_df = pd.concat(apgu_all, ignore_index=True)
            upsert_apgu_verbatim(ws_ap, all_df, run_day)
        else:
            log("[압구정동] sheet not found (skip)")

    log("[main] done")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"[ERROR] {repr(e)}")
        raise
