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

# ===================== 설정 =====================
LOG_DIR = Path("analyze_report")
ARTIFACTS_DIR = os.environ.get("ARTIFACTS_DIR", "artifacts")

SUMMARY_SHEET_NAME = "거래요약"

# 거래요약에 기록할 열 (시트 열명과 동일, 정식표기)
SUMMARY_COLS = [
    "전국","서울",
    "강남구","압구정동",
    "경기도","인천광역시","세종특별자치시","울산광역시",
    "서초구","송파구","용산구","강동구","성동구","마포구","양천구","동작구","영등포구","종로구","광진구",
    "강서구","강북구","관악구","구로구","금천구","도봉구","노원구",
    "동대문구","서대문구","성북구","은평구","중구","중랑구",
    "부산광역시","대구광역시","광주광역시","대전광역시",
    "강원특별자치도","경상남도","경상북도","전라남도","전북특별자치도","충청남도","충청북도","제주특별자치도"
]

# ===================== 로깅/리트라이 =====================
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
    import time as _t
    global _LAST
    now = _t.time()
    if now - _LAST < sec:
        _t.sleep(sec - (now - _LAST))
    _LAST = _t.time()

def _retry(fn, *a, **kw):
    base = 0.8
    for i in range(7):
        try:
            _throttle()
            return fn(*a, **kw)
        except APIError as e:
            s = str(e)
            # 재시도 가능한 에러만 백오프
            if any(x in s for x in ("429","500","502","503")):
                time.sleep(base*(2**i) + random.uniform(0,0.25))
                continue
            raise

# ===================== gspread 헬퍼 =====================
def a1_col(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n-1, 26)
        s = chr(65+r) + s
    return s

def values_batch_update(ws: gspread.Worksheet, data: List[Dict]):
    # 반드시 시트명 포함된 range만 보냄("'시트'!A1")
    body = {"valueInputOption": "USER_ENTERED", "data": data}
    return _retry(ws.spreadsheet.values_batch_update, body=body)

def batch_format(ws: gspread.Worksheet, requests: List[dict]):
    if not requests: return
    _retry(ws.spreadsheet.batch_update, {"requests": requests})

def fuzzy_ws(sh: gspread.Spreadsheet, wanted: str) -> Optional[gspread.Worksheet]:
    tgt = re.sub(r"\s+","", wanted)
    for ws in sh.worksheets():
        if re.sub(r"\s+","", ws.title) == tgt:
            log(f"[ws] matched: '{ws.title}' (wanted='{wanted}')")
            return ws
    return None

# ===================== 파일/읽기/집계 =====================
def read_month_df(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name="data", dtype=str)
    df = df.fillna("")
    # 숫자형
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

def agg_all_stats(df: pd.DataFrame):
    """
    집계 규칙
    - 전국: 광역 전체 행 개수 + 광역별 분포(정식명 그대로)
    - 서울: 광역=서울특별시 필터 후 자치구별, 서울 합계
    - 압구정동: 광역=서울특별시 & 법정동=압구정동
    """
    counts = {col:0 for col in SUMMARY_COLS}
    med = {col:"" for col in SUMMARY_COLS}
    mean = {col:"" for col in SUMMARY_COLS}

    # 전국
    counts["전국"] = int(len(df))
    all_eok = eok_series(df.get("거래금액(만원)", []))
    if not all_eok.empty:
        med["전국"] = round2(all_eok.median())
        mean["전국"] = round2(all_eok.mean())

    if "광역" in df.columns:
        for prov, sub in df.groupby("광역"):
            prov = str(prov)
            if prov in counts:
                counts[prov] += int(len(sub))
                se = eok_series(sub.get("거래금액(만원)", []))
                if not se.empty:
                    med[prov] = round2(se.median())
                    mean[prov] = round2(se.mean())

    # 서울
    seoul = df[df.get("광역","") == "서울특별시"].copy()
    counts["서울"] = int(len(seoul))
    if len(seoul) > 0:
        se = eok_series(seoul["거래금액(만원)"])
        if not se.empty:
            med["서울"] = round2(se.median())
            mean["서울"] = round2(se.mean())

        if "구" in seoul.columns:
            for gu, sub in seoul.groupby("구"):
                gu = str(gu)
                if gu in counts:
                    counts[gu] += int(len(sub))
                    se2 = eok_series(sub["거래금액(만원)"])
                    if not se2.empty:
                        med[gu] = round2(se2.median())
                        mean[gu] = round2(se2.mean())

    # 압구정동
    ap = seoul[seoul.get("법정동","") == "압구정동"]
    counts["압구정동"] = int(len(ap))
    if len(ap) > 0:
        s = eok_series(ap["거래금액(만원)"])
        if not s.empty:
            med["압구정동"] = round2(s.median())
            mean["압구정동"] = round2(s.mean())

    return counts, med, mean

# ===================== 공통 유틸 =====================
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

# ===================== 월별 시트 기록 =====================
def write_month_sheet(ws, date_label: str, header: List[str], values_by_colname: Dict[str,int]):
    """
    - 헤더가 존재하는 열에만 씀(모르는 열은 건드리지 않음)
    - range는 반드시 "'시트'!A1" 형식으로 지정
    """
    hmap = {str(h).strip(): idx+1 for idx,h in enumerate(header) if str(h).strip()}
    row_idx = find_or_append_date_row(ws, date_label)
    sheet_prefix = f"'{ws.title}'!"

    payload = [{"range": f"{sheet_prefix}A{row_idx}", "values": [[date_label]]}]

    # 알려진 헤더만 기록
    for col_name, val in values_by_colname.items():
        if col_name in hmap:
            c = hmap[col_name]
            payload.append({"range": f"{sheet_prefix}{a1_col(c)}{row_idx}", "values": [[val]]})

    # 총합계 보정(있으면)
    for possible in ("총합계","합계"):
        if possible in hmap:
            v = values_by_colname.get("총합계", None)
            if v is None:
                # 전국 탭은 counts["전국"], 서울 탭은 counts["서울"]
                v = values_by_colname.get("전국", values_by_colname.get("서울", None))
            if v is not None:
                payload.append({"range": f"{sheet_prefix}{a1_col(hmap[possible])}{row_idx}", "values": [[int(v)]]})
            break

    if payload:
        values_batch_update(ws, payload)
        log(f"[ws] {ws.title} -> {date_label} row={row_idx} (wrote {len(payload)} cells incl. date)")

# ===================== 거래요약 =====================
def ym_from_filename(fname: str):
    # '전국 2509_250930.xlsx' → ('전국 2025년 9월','서울 2025년 9월','25/9')
    m = re.search(r"(\d{2})(\d{2})_", fname)
    if not m: return None, None, None
    yy, mm = m.group(1), int(m.group(2))
    return f"전국 20{yy}년 {mm}월", f"서울 20{yy}년 {mm}월", f"{yy}/{mm}"

def prev_ym(ym: str) -> str:
    yy, mm = ym.split("/")
    y = int(yy); m = int(mm)
    if m == 1:
        return f"{y-1}/12"
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
    header = _retry(ws.row_values, 1)
    if not header:
        _retry(ws.update, [["년월","구분"] + SUMMARY_COLS], "A1")
        header = ["년월","구분"] + SUMMARY_COLS
    hmap = {h:i+1 for i,h in enumerate(header)}

    sheet_prefix = f"'{ws.title}'!"
    payload = [
        {"range": f"{sheet_prefix}A{row_idx}", "values": [[ym]]},
        {"range": f"{sheet_prefix}B{row_idx}", "values": [[label]]},
    ]
    for c in SUMMARY_COLS:
        if c in hmap:
            payload.append({
                "range": f"{sheet_prefix}{a1_col(hmap[c])}{row_idx}",
                "values": [[line_map.get(c,"")]]
            })
    values_batch_update(ws, payload)

def color_diff_line(ws, row_idx: int, diff_line: dict, header: List[str]):
    hmap = {h:i+1 for i,h in enumerate(header)}
    reqs = []
    for k, v in diff_line.items():
        if k not in hmap: continue
        if v == "" or v == "0": continue
        r,g,b = (0.0,0.35,1.0) if str(v).startswith("+") else (1.0,0.0,0.0)
        reqs.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws.id,
                    "startRowIndex": row_idx-1, "endRowIndex": row_idx,
                    "startColumnIndex": hmap[k]-1, "endColumnIndex": hmap[k]
                },
                "cell": {"userEnteredFormat":{"textFormat":{"foregroundColor":{"red":r,"green":g,"blue":b}}}},
                "fields": "userEnteredFormat.textFormat.foregroundColor"
            }
        })
    batch_format(ws, reqs)

def write_month_summary(ws, y: int, m: int, counts: dict, med: dict, mean: dict, prev_counts: Optional[dict]):
    ym = f"{str(y%100).zfill(2)}/{m}"

    r1 = find_summary_row(ws, ym, "거래건수")
    put_summary_line(ws, r1, ym, "거래건수", counts)
    log(f"[summary] {ym} 거래건수 -> row={r1}")

    r2 = find_summary_row(ws, ym, "중앙값(단위:억)")
    put_summary_line(ws, r2, ym, "중앙값(단위:억)", med)
    log(f"[summary] {ym} 중앙값 -> row={r2}")

    r3 = find_summary_row(ws, ym, "평균가(단위:억)")
    put_summary_line(ws, r3, ym, "평균가(단위:억)", mean)
    log(f"[summary] {ym} 평균가 -> row={r3}")

    # 전월대비 건수증감
    if prev_counts:
        diffs = {}
        for c in SUMMARY_COLS:
            cur = int(counts.get(c,0) or 0)
            prv = int(prev_counts.get(c,0) or 0)
            d = cur - prv
            diffs[c] = f"+{d}" if d>0 else (str(d) if d<0 else "0")
    else:
        diffs = {c:"" for c in SUMMARY_COLS}
    r4 = find_summary_row(ws, ym, "전월대비 건수증감")
    put_summary_line(ws, r4, ym, "전월대비 건수증감", diffs)
    header = _retry(ws.row_values, 1)
    color_diff_line(ws, r4, diffs, header)
    log(f"[summary] {ym} 전월대비 -> row={r4}")

# ===================== 압구정동 (원본 그대로 + 변동요약) =====================
APGU_BASE_COLS = [
    "광역","구","법정동","리","번지","본번","부번","단지명","전용면적(㎡)",
    "계약년","계약월","계약일","거래금액(만원)","동","층",
    "매수자","매도자","건축년도","도로명","해제사유발생일","거래유형",
    "중개사소재지","등기일자","주택유형"
]

def _apgu_norm(v) -> str:
    return "" if v is None else str(v).strip()

def _apgu_key_from_row_values(values: List[str], header: List[str]) -> str:
    idx = {h:i for i,h in enumerate(header)}
    parts = []
    for h in APGU_BASE_COLS:
        i = idx.get(h, None)
        parts.append(_apgu_norm(values[i] if (i is not None and i < len(values)) else ""))  # noqa
    return "|".join(parts)

def _ensure_rows(ws: gspread.Worksheet, need_end_row: int):
    if need_end_row > ws.row_count:
        _retry(ws.add_rows, need_end_row - ws.row_count)

def fmt_kdate(d: date) -> str:
    return f"{d.year}. {d.month}. {d.day}"

def upsert_apgu_verbatim(ws: gspread.Worksheet, df_all: pd.DataFrame, run_day: date):
    df = df_all[(df_all.get("광역","")=="서울특별시") & (df_all.get("법정동","")=="압구정동")].copy()
    if df.empty:
        log("[압구정동] no rows")
        return

    # 필수 컬럼 채우기
    for c in APGU_BASE_COLS:
        if c not in df.columns:
            df[c] = ""

    # 오래된 → 최신
    for c in ["계약년","계약월","계약일"]:
        if c not in df.columns:
            df[c] = ""
    df = df.sort_values(["계약년","계약월","계약일"], ascending=[True,True,True], kind="mergesort")

    # 헤더 고정
    vals = _retry(ws.get_all_values) or []
    if not vals:
        _retry(ws.update, [APGU_BASE_COLS], "A1")
        vals = [APGU_BASE_COLS]
    header = vals[0]
    if header != APGU_BASE_COLS:
        _retry(ws.update, [APGU_BASE_COLS], "A1")
        header = APGU_BASE_COLS

    # 기존 본표 행(변동 블록 제외)
    all_now = _retry(ws.get_all_values) or [header]
    body = all_now[1:]
    base_rows_old: List[List[str]] = []
    for r in body:
        if r and r[0] in ("변경구분","(신규)","(삭제)"):
            break
        base_rows_old.append((r + [""]*len(header))[:len(header)])

    # 오늘 본표 (원본 그대로)
    base_rows_new: List[List[str]] = []
    for _, row in df.iterrows():
        base_rows_new.append([_apgu_norm(row.get(c, "")) for c in APGU_BASE_COLS])

    # 본표 덮어쓰기 (워크시트 단일 update는 시트컨텍스트 고정이라 A1만으로 안전)
    start = 2
    end = start + len(base_rows_new) - 1
    if end < start: end = start
    _ensure_rows(ws, end)
    _retry(ws.update, base_rows_new, f"A{start}:{a1_col(len(header))}{end}")
    log(f"[압구정동] base rows written: {len(base_rows_new)}")

    # 신규/삭제 비교
    old_keys = {_apgu_key_from_row_values(r, header) for r in base_rows_old}
    new_keys = {_apgu_key_from_row_values(r, header) for r in base_rows_new}
    added_keys = sorted(list(new_keys - old_keys))
    removed_keys = sorted(list(old_keys - new_keys))

    if not added_keys and not removed_keys:
        log("[압구정동] changes: none")
        return

    def _rowmap(rows: List[List[str]]):
        m={}
        for r in rows:
            m[_apgu_key_from_row_values(r, header)] = r
        return m

    new_map = _rowmap(base_rows_new)
    old_map = _rowmap(base_rows_old)

    change_header = ["변경구분","변경일"] + APGU_BASE_COLS
    change_rows: List[List[str]] = [change_header]
    today_str = fmt_kdate(run_day)
    for k in added_keys:
        change_rows.append(["(신규)", today_str] + new_map[k])
    for k in removed_keys:
        change_rows.append(["(삭제)", today_str] + old_map[k])

    start_chg = end + 1
    end_chg = start_chg + len(change_rows) - 1
    _ensure_rows(ws, end_chg)
    _retry(ws.update, change_rows, f"A{start_chg}:{a1_col(len(change_header))}{end_chg}")

    # 빨간 글씨
    req = {
        "repeatCell": {
            "range": {
                "sheetId": ws.id,
                "startRowIndex": start_chg-1,
                "endRowIndex": end_chg,
                "startColumnIndex": 0,
                "endColumnIndex": len(change_header)
            },
            "cell": {"userEnteredFormat":{"textFormat":{"foregroundColor":{"red":1.0,"green":0.0,"blue":0.0}}}},
            "fields": "userEnteredFormat.textFormat.foregroundColor"
        }
    }
    batch_format(ws, [req])
    log(f"[압구정동] changes: 신규={len(added_keys)} 삭제={len(removed_keys)}")

# ===================== 메인 =====================
def main():
    try: RUN_LOG.write_text("", encoding="utf-8")
    except Exception: pass

    log("[MAIN]")
    log(f"artifacts_dir={ARTIFACTS_DIR}")

    # 인증
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

    # 파일 수집
    files = sorted(Path(ARTIFACTS_DIR).rglob("전국 *.xlsx"))
    log(f"[collect] found {len(files)} xlsx files")

    month_cache = {}  # ym -> {counts, med, mean}
    today_label = kdate(datetime.now())
    run_day = datetime.now().date()
    apgu_all: List[pd.DataFrame] = []

    for p in files:
        nat_title, se_title, ym = ym_from_filename(p.name)
        if not ym: 
            continue
        log(f"[file] {p.name} -> nat='{nat_title}' / seoul='{se_title}' / ym={ym}")

        df = read_month_df(p)
        log(f"[read] rows={len(df)} cols={len(df.columns)}")

        counts, med, mean = agg_all_stats(df)
        month_cache[ym] = {"counts": counts, "med": med, "mean": mean}

        # ===== 전국 탭 쓰기 =====
        ws_nat = fuzzy_ws(sh, nat_title)
        if ws_nat:

        # ===== 서울 탭 쓰기 =====
        ws_se = fuzzy_ws(sh, se_title)
        if ws_se:
            header_se = _retry(ws_se.row_values, 1)
            values_se: Dict[str,int] = {}
            for h in header_se:
                if not h or h=="날짜":
                    continue
                if h == "총합계":
                    values_se["총합계"] = int(counts.get("서울",0))
                else:
                    if h in counts:
                        values_se[h] = int(counts[h])
            write_month_sheet(ws_se, today_label, header_se, values_se)

        # 압구정동 원본 누적
        ap = df[(df.get("광역","")=="서울특별시") & (df.get("법정동","")=="압구정동")].copy()
        if not ap.empty:
            apgu_all.append(ap)

    # ===== 거래요약 =====
    ws_sum = fuzzy_ws(sh, SUMMARY_SHEET_NAME)
    if ws_sum and month_cache:
        def ym_key(ym): 
            yy, mm = ym.split("/")
            return (int(yy), int(mm))
        for ym in sorted(month_cache.keys(), key=ym_key):
            cur = month_cache[ym]
            prv = month_cache.get(prev_ym(ym))
            write_month_summary(
                ws_sum,
                2000 + int(ym.split("/")[0]),
                int(ym.split("/")[1]),
                cur["counts"], cur["med"], cur["mean"],
                prv["counts"] if prv else None
            )
            time.sleep(0.15)

    # ===== 압구정동 =====
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
