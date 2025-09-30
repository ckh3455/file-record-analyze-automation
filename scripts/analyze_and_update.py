# scripts/analyze_and_update.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os, re, sys, json, time, random
from pathlib import Path
from datetime import datetime, date, timezone, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

# ===================== 설정 =====================
LOG_DIR = Path("analyze_report")
ARTIFACTS_DIR = os.environ.get("ARTIFACTS_DIR", "artifacts")
WRITE_SUMMARY = False  # ← 거래요약 탭은 이번 버전에서는 건드리지 않음

SUMMARY_SHEET_NAME = "거래요약"

# 압구정동 본표 고정 열(원본 그대로)
APGU_BASE_COLS = [
    "광역","구","법정동","리","번지","본번","부번","단지명","전용면적(㎡)",
    "계약년","계약월","계약일","거래금액(만원)","동","층",
    "매수자","매도자","건축년도","도로명","해제사유발생일","거래유형",
    "중개사소재지","등기일자","주택유형"
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
WHERE_TXT = LOG_DIR / "where_written.txt"

def log(msg: str):
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    print(line)
    try:
        with RUN_LOG.open("a", encoding="utf-8") as f:
            f.write(line+"\n")
    except Exception:
        pass

def note_where(s: str):
    try:
        with WHERE_TXT.open("a", encoding="utf-8") as f:
            f.write(s.rstrip()+"\n")
    except Exception:
        pass

_LAST = 0.0
def _throttle(sec=0.35):
    import time as _t
    global _LAST
    now = _t.time()
    if now - _LAST < sec:
        _t.sleep(max(0, sec - (now - _LAST)))
    _LAST = _t.time()

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

# ===================== 유틸 =====================
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
            log(f"[ws] matched: '{ws.title}' (wanted='{wanted}')")
            return ws
    return None

# ===================== 날짜 라벨 =====================
def now_kst() -> datetime:
    KST = timezone(timedelta(hours=9))
    return datetime.now(KST)

def kdate(d: datetime) -> str:
    # 시트의 날짜 포맷과 동일: YYYY. M. D
    return f"{d.year}. {d.month}. {d.day}"

# ===================== 파일/읽기 =====================
def read_month_df(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name="data", dtype=str)
    df = df.fillna("")
    # 숫자형
    for c in ["계약년","계약월","계약일","거래금액(만원)"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

# ===================== 집계 =====================
def counts_by_province(df: pd.DataFrame) -> Dict[str, int]:
    """
    전국 탭용: 원본의 '광역' 컬럼을 기준으로 지역별 거래건수 집계.
    """
    out: Dict[str, int] = {}
    if "광역" not in df.columns:
        return out
    g = df.groupby("광역", dropna=False).size()
    for k, v in g.items():
        out[str(k)] = int(v)
    # 전국 총합
    out["전국"] = int(len(df))
    return out

def counts_by_seoul_gu(df: pd.DataFrame) -> Dict[str, int]:
    """
    서울 탭용: 먼저 '광역 == 서울특별시'로 필터링 → '구'로 집계.
    """
    out: Dict[str, int] = {}
    if "광역" not in df.columns:
        return out
    seoul = df[df["광역"] == "서울특별시"].copy()
    out["서울"] = int(len(seoul))
    if "구" in seoul.columns and len(seoul) > 0:
        g = seoul.groupby("구", dropna=False).size()
        for k, v in g.items():
            out[str(k)] = int(v)
    return out

# ===================== 시트 기록 =====================
def find_or_append_date_row(ws: gspread.Worksheet, date_label: str) -> int:
    vals = _retry(ws.get_all_values)
    if not vals:
        return 2
    for i, row in enumerate(vals[1:], start=2):
        if (len(row)>0) and str(row[0]).strip()==date_label:
            return i
    return len(vals)+1

def write_counts_row(ws: gspread.Worksheet, date_label: str, counts: Dict[str,int], scope: str):
    """
    scope: '전국' or '서울' — '총합계'에 들어갈 기준만 다름.
    - 1행 헤더를 기준으로, '날짜'와 '총합계' 이외의 모든 헤더 이름을 그대로 key로 사용
    - counts에 해당 키가 있으면 그 값 기록
    - 모르는 헤더는 손대지 않음
    """
    # 거래요약 보호
    if re.sub(r"\s+","", ws.title) == re.sub(r"\s+","", SUMMARY_SHEET_NAME):
        return

    header = _retry(ws.row_values, 1) or []
    header = [str(h).strip() for h in header]
    hmap = {h: i+1 for i,h in enumerate(header) if h}

    row_idx = find_or_append_date_row(ws, date_label)

    payload = [{"range": f"A{row_idx}", "values": [[date_label]]}]  # 날짜는 항상 기록

    for h in header:
        if not h or h == "날짜":
            continue
        if h in ("총합계","합계"):
            key = "전국" if scope == "전국" else "서울"
            val = int(counts.get(key, 0))
        else:
            if h not in counts:
                continue
            val = int(counts.get(h, 0) or 0)

        c = hmap.get(h)
        if c:
            payload.append({"range": f"{a1_col(c)}{row_idx}", "values": [[val]]})

    if payload:
        values_batch_update(ws, payload)
        log(f"[ws] {ws.title} -> {date_label} row={row_idx}")
        note_where(f"{ws.title}\t(id={ws.id})\tA{row_idx}:{a1_col(len(header))}{row_idx}")

# ===================== 압구정동 (원본 그대로 + 변동요약) =====================
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
        log("[압구정동] no rows")
        return

    for c in APGU_BASE_COLS:
        if c not in df.columns:
            df[c] = ""

    for c in ["계약년","계약월","계약일"]:
        if c not in df.columns:
            df[c] = ""
    df = df.sort_values(["계약년","계약월","계약일"], ascending=[True,True,True], kind="mergesort")

    vals = _retry(ws.get_all_values) or []
    if not vals:
        _retry(ws.update, [APGU_BASE_COLS], "A1")
        vals = [APGU_BASE_COLS]
    header = vals[0]
    if header != APGU_BASE_COLS:
        _retry(ws.update, [APGU_BASE_COLS], "A1")
        header = APGU_BASE_COLS

    all_now = _retry(ws.get_all_values) or [header]
    body = all_now[1:]
    base_rows_old: List[List[str]] = []
    for r in body:
        if r and r[0] in ("변경구분","(신규)","(삭제)"):
            break
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

# ===================== 파일명 → 탭명 =====================
def ym_from_filename(fname: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    # '전국 2509_250930.xlsx' → ('전국 2025년 9월','서울 2025년 9월','25/9')
    m = re.search(r"(\d{2})(\d{2})_", fname)
    if not m:
        return None, None, None
    yy, mm = m.group(1), int(m.group(2))
    nat = f"전국 20{yy}년 {mm}월"
    se =  f"서울 20{yy}년 {mm}월"
    ym = f"{yy}/{mm}"
    return nat, se, ym

# ===================== 메인 =====================
def main():
    try:
        RUN_LOG.write_text("", encoding="utf-8")
        WHERE_TXT.write_text("", encoding="utf-8")
    except Exception:
        pass

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

    # 날짜 라벨 (KST)
    today_label = kdate(now_kst())
    run_day = now_kst().date()

    apgu_all: List[pd.DataFrame] = []

    for p in files:
        nat_title, se_title, ym = ym_from_filename(p.name)
        if not ym: 
            continue
        log(f"[file] {p.name} -> nat='{nat_title}' / seoul='{se_title}' / ym={ym}")

        df = read_month_df(p)
        log(f"[read] rows={len(df)} cols={len(df.columns)}")

        # === 전국 탭: 광역 기준 집계 ===
        ws_nat = fuzzy_ws(sh, nat_title)
        if ws_nat:
            nat_counts = counts_by_province(df)  # {'경기도': n, '인천광역시': n, ..., '전국': total}
            write_counts_row(ws_nat, today_label, nat_counts, scope="전국")
        else:
            log(f"[전국] sheet not found: '{nat_title}' (skip)")

        # === 서울 탭: '서울특별시' 필터 → 구 기준 집계 ===
        ws_se = fuzzy_ws(sh, se_title)
        if ws_se:
            se_counts = counts_by_seoul_gu(df)   # {'서울': total_in_seoul, '강남구': n, ...}
            write_counts_row(ws_se, today_label, se_counts, scope="서울")
        else:
            log(f"[서울] sheet not found: '{se_title}' (skip)")

        # 압구정동 원본 누적(그대로)
        ap = df[(df.get("광역","")=="서울특별시") & (df.get("법정동","")=="압구정동")].copy()
        if not ap.empty:
            apgu_all.append(ap)

    # 거래요약(요청으로 이번 버전에선 비활성화)
    if WRITE_SUMMARY:
        log("[summary] skipped by config")

    # 압구정동 원본 그대로 + 변동요약
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
