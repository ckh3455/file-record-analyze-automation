# scripts/analyze_and_update.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os, re, sys, json, time, math, random
from pathlib import Path
from datetime import datetime, date
from typing import Dict, List, Tuple, Optional

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

# ===================== 기본 설정 =====================
LOG_DIR = Path("analyze_report")
ART_DIR_DEFAULT = "artifacts"
SHEET_NAME_DATA = "data"

SUMMARY_SHEET = "거래요약"

SUMMARY_COLS = [
    "전국","서울","강남구","압구정동","경기도","인천광역시","세종시","서초구","송파구","용산구",
    "강동구","성동구","마포구","양천구","동작구","영등포구","종로구","광진구","강서구",
    "강북구","관악구","구로구","금천구","도봉구","동대문구","서대문구","성북구","은평구",
    "중구","중랑구","부산","대구","광주","대전","강원도","경남","경북","전남","전북","충남","충북","제주"
]

# 광역 표기 → 요약 컬럼명 매핑
PROV_MAP = {
    "서울특별시": "서울",
    "세종특별자치시": "세종시",
    "강원특별자치도": "강원도",
    "경기도": "경기도",
    "인천광역시": "인천광역시",
    "부산광역시": "부산",
    "대구광역시": "대구",
    "광주광역시": "광주",
    "대전광역시": "대전",
    "울산광역시": "울산",
    "전라남도": "전남",
    "전북특별자치도": "전북",
    "경상남도": "경남",
    "경상북도": "경북",
    "충청남도": "충남",
    "충청북도": "충북",
    "제주특별자치도": "제주",
}

APGU_SHEET = "압구정동"
APGU_CHANGE_HEADER = ["변경구분","기록일","계약년","계약월","계약일","단지명","전용면적(㎡)","동","층","거래금액(만원)"]

NEEDED_COLS = ["광역","구","법정동","계약년","계약월","계약일","거래금액(만원)","단지명","전용면적(㎡)","동","층"]

# ===================== 로깅 =====================
def _ensure_logdir():
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
    except FileExistsError:
        if LOG_DIR.exists() and LOG_DIR.is_file():
            LOG_DIR.unlink()
            LOG_DIR.mkdir(parents=True, exist_ok=True)

def log(msg: str):
    _ensure_logdir()
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}\n"
    p = LOG_DIR / "latest.log"
    try:
        if p.exists():
            p.write_text(p.read_text(encoding="utf-8") + line, encoding="utf-8")
        else:
            p.write_text(line, encoding="utf-8")
    except Exception:
        pass
    print(line, end="")

def note_written(s: str):
    try:
        with open(LOG_DIR/"where_written.txt","a",encoding="utf-8") as f:
            f.write(s.rstrip()+"\n")
    except Exception:
        pass

# ===================== 유틸 =====================
def kdate(d: date) -> str:
    return f"{d.year}. {d.month}. {d.day}"

def ym_label(y: int, m: int) -> str:
    return f"{str(y%100).zfill(2)}/{m}"

def prev_ym_label(ym: str) -> str:
    yy, mm = ym.split("/")
    y, m = 2000+int(yy), int(mm)
    if m == 1: return f"{str((y-1)%100).zfill(2)}/12"
    return f"{yy}/{m-1}"

def norm_title(s: str) -> str:
    # 공백제거 + 2025년 → 25년 통일
    s = re.sub(r"\s+","", str(s or ""))
    s = re.sub(r"20(\d{2})년", r"\1년", s)
    return s

def find_ws_exact(sh: gspread.Spreadsheet, title: str) -> Optional[gspread.Worksheet]:
    want = norm_title(title)
    for ws in sh.worksheets():
        if norm_title(ws.title) == want:
            log(f"[ws] matched: '{ws.title}'")
            return ws
    log(f"[ws] sheet not found: '{title}' (skip)")
    return None

# ============ gspread throttling / batch helpers ============
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
            if any(x in s for x in ["429","500","502","503"]):
                time.sleep(base * (2**i) + random.uniform(0,0.3))
                continue
            raise

def values_batch_update(ws: gspread.Worksheet, payload: List[Dict]):
    body = {"valueInputOption":"USER_ENTERED", "data": payload}
    return _retry(ws.spreadsheet.values_batch_update, body=body)

def batch_format(ws: gspread.Worksheet, requests: List[dict]):
    if not requests: return
    _retry(ws.spreadsheet.batch_update, {"requests": requests})

# ===================== 파일 읽기 / 집계 =====================
def read_month_df(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=SHEET_NAME_DATA, dtype=str, engine="openpyxl")
    df = df.fillna("")
    # 숫자화
    for c in ["계약년","계약월","계약일","거래금액(만원)"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in NEEDED_COLS:
        if c not in df.columns:
            df[c] = pd.NA
    return df[NEEDED_COLS].copy()

def eok_series(ser: pd.Series) -> pd.Series:
    s = pd.to_numeric(ser, errors="coerce").dropna()
    if s.empty: return pd.Series([], dtype=float)
    return s / 10000.0

def round2(v) -> str:
    try: return f"{float(v):.2f}"
    except Exception: return ""

def agg_all_stats(df: pd.DataFrame):
    counts = {c:0 for c in SUMMARY_COLS}
    med = {c:"" for c in SUMMARY_COLS}
    mean = {c:"" for c in SUMMARY_COLS}

    # 전국
    all_eok = eok_series(df["거래금액(만원)"])
    counts["전국"] = int(len(df))
    if not all_eok.empty:
        med["전국"] = round2(all_eok.median())
        mean["전국"] = round2(all_eok.mean())

    # 광역
    for prov, sub in df.groupby("광역"):
        prov_std = PROV_MAP.get(str(prov), str(prov))
        if prov_std in counts:
            counts[prov_std] += int(len(sub))
            s = eok_series(sub["거래금액(만원)"])
            if not s.empty:
                med[prov_std] = round2(s.median())
                mean[prov_std] = round2(s.mean())

    # 서울/구
    seoul = df[df.get("광역","")=="서울특별시"].copy()
    counts["서울"] = int(len(seoul))
    if len(seoul)>0:
        s = eok_series(seoul["거래금액(만원)"])
        if not s.empty:
            med["서울"] = round2(s.median())
            mean["서울"] = round2(s.mean())
    if "구" in seoul.columns:
        for gu, sub in seoul.groupby("구"):
            gu = str(gu)
            if gu in counts:
                counts[gu] += int(len(sub))
                s = eok_series(sub["거래금액(만원)"])
                if not s.empty:
                    med[gu] = round2(s.median())
                    mean[gu] = round2(s.mean())

    # 압구정동 건수
    ap = seoul[seoul.get("법정동","")=="압구정동"]
    counts["압구정동"] = int(len(ap))
    if len(ap)>0:
        s = eok_series(ap["거래금액(만원)"])
        med["압구정동"] = round2(s.median())
        mean["압구정동"] = round2(s.mean())

    return counts, med, mean

# ===================== 월별 탭(전국/서울) 쓰기 =====================
def detect_date_col(header: List[str]) -> int:
    for i,v in enumerate(header, start=1):
        if str(v).strip() == "날짜":
            return i
    return 1

def find_or_append_date_row(ws: gspread.Worksheet, day: date) -> Tuple[str,int]:
    header = _retry(ws.row_values, 1) or []
    date_col = detect_date_col(header)
    # 날짜 검색
    col_vals = _retry(ws.col_values, date_col)
    target = kdate(day)
    for i, v in enumerate(col_vals[1:], start=2):
        if str(v).strip() == target:
            return "update", i
    # append → 마지막 사용행 찾기
    used = 1
    for i in range(len(col_vals), 1, -1):
        if str(col_vals[i-1]).strip():
            used = i; break
    return "append", used+1

def write_counts_to_month_sheet(ws: gspread.Worksheet, day: date, counts: dict, sum_col_name: str = "총합계"):
    header = _retry(ws.row_values, 1) or []
    mode, row = find_or_append_date_row(ws, day)
    hmap = {h:i+1 for i,h in enumerate(header) if h}
    out = [[None]*(len(header))]
    # 날짜
    out[0][0] = kdate(day)
    # 각 열 채우기
    total = 0
    for h, idx in hmap.items():
        if h == "날짜": continue
        if h == sum_col_name:
            continue  # 총합계는 나중에
        val = int(counts.get(h, 0) or 0)
        out[0][idx-1] = val
        if h != "전국" and "전국 " not in ws.title:  # 전국 탭은 총합계=전국 건수, 서울 탭은 합계=서울 건수
            total += val
    # 총합계
    if sum_col_name in hmap:
        if "전국 " in ws.title:
            out[0][hmap[sum_col_name]-1] = int(counts.get("전국", total))
        elif "서울 " in ws.title:
            out[0][hmap[sum_col_name]-1] = int(counts.get("서울", total))
        else:
            out[0][hmap[sum_col_name]-1] = total

    # Range 준비(그리드 초과 방지 위해 필요시 행 추가)
    need_rows = row
    if ws.row_count < need_rows:
        _retry(ws.add_rows, need_rows - ws.row_count)
    last_col_letter = gspread.utils.rowcol_to_a1(1, len(header)).rstrip("1")
    rng = f"A{row}:{last_col_letter}{row}"
    values_batch_update(ws, [{"range": rng, "values": out}])
    log(f"[ws] {ws.title} -> {kdate(day)} {mode} row={row}")
    note_written(f"{ws.title}\t{kdate(day)}\t{mode}\t{row}")

# ===================== 거래요약 탭 =====================
def ensure_summary_header(ws: gspread.Worksheet):
    vals = _retry(ws.get_all_values) or []
    if not vals:
        header = ["년월","구분"] + SUMMARY_COLS
        values_batch_update(ws, [{"range": "A1", "values": [header]}])

def find_summary_row(ws: gspread.Worksheet, ym: str, label: str) -> int:
    vals = _retry(ws.get_all_values) or []
    for i, r in enumerate(vals[1:], start=2):
        a = (r[0] if len(r)>0 else "").strip()
        b = (r[1] if len(r)>1 else "").strip()
        if a == ym and b == label:
            return i
    return len(vals)+1

def put_summary_line(ws: gspread.Worksheet, row_idx: int, ym: str, label: str, line_map: dict):
    header = _retry(ws.row_values, 1) or []
    hmap = {h:i+1 for i,h in enumerate(header) if h}
    # ensure grid
    need_rows = row_idx
    if ws.row_count < need_rows:
        _retry(ws.add_rows, need_rows - ws.row_count)
    payload = [
        {"range": f"A{row_idx}:B{row_idx}", "values": [[ym, label]]}
    ]
    for h in SUMMARY_COLS:
        if h in hmap:
            payload.append({"range": f"{gspread.utils.rowcol_to_a1(row_idx, hmap[h])}", "values": [[line_map.get(h,"")]]})
    values_batch_update(ws, payload)

def color_deltas(ws: gspread.Worksheet, row_idx: int, deltas: dict):
    header = _retry(ws.row_values, 1) or []
    hmap = {h:i+1 for i,h in enumerate(header) if h}
    reqs = []
    for h, v in deltas.items():
        if h not in hmap: continue
        txt = str(v)
        if not txt or txt == "0": continue
        if txt.startswith("+"):
            color = {"red":0.0,"green":0.35,"blue":1.0}
        elif txt.startswith("-"):
            color = {"red":1.0,"green":0.0,"blue":0.0}
        else:
            continue
        reqs.append({
            "repeatCell": {
                "range": {
                    "sheetId": ws.id,
                    "startRowIndex": row_idx-1,
                    "endRowIndex": row_idx,
                    "startColumnIndex": hmap[h]-1,
                    "endColumnIndex": hmap[h]
                },
                "cell": {"userEnteredFormat": {"textFormat": {"foregroundColor": color}}},
                "fields": "userEnteredFormat.textFormat.foregroundColor"
            }
        })
    batch_format(ws, reqs)

def bold_counts_row(ws: gspread.Worksheet, row_idx: int):
    header = _retry(ws.row_values, 1) or []
    if not header: return
    req = {
        "repeatCell": {
            "range": {
                "sheetId": ws.id,
                "startRowIndex": row_idx-1,
                "endRowIndex": row_idx,
                "startColumnIndex": 2,  # C열부터
                "endColumnIndex": len(header)
            },
            "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
            "fields": "userEnteredFormat.textFormat.bold"
        }
    }
    batch_format(ws, [req])

def write_month_summary(ws: gspread.Worksheet, y: int, m: int,
                        counts: dict, med: dict, mean: dict,
                        prev_counts: Optional[dict]):
    ensure_summary_header(ws)
    ym = ym_label(y,m)

    r1 = find_summary_row(ws, ym, "거래건수")
    put_summary_line(ws, r1, ym, "거래건수", counts)
    bold_counts_row(ws, r1)
    log(f"[summary] {ym} 거래건수 -> row={r1}")

    r2 = find_summary_row(ws, ym, "중앙값(단위:억)")
    put_summary_line(ws, r2, ym, "중앙값(단위:억)", med)
    log(f"[summary] {ym} 중앙값 -> row={r2}")

    r3 = find_summary_row(ws, ym, "평균가(단위:억)")
    put_summary_line(ws, r3, ym, "평균가(단위:억)", mean)
    log(f"[summary] {ym} 평균가 -> row={r3}")

    r4 = find_summary_row(ws, ym, "전월대비 건수증감")
    diffs = {c:"" for c in SUMMARY_COLS}
    if prev_counts:
        for c in SUMMARY_COLS:
            cur = int(counts.get(c,0) or 0)
            prv = int(prev_counts.get(c,0) or 0)
            d = cur - prv
            diffs[c] = f"+{d}" if d>0 else (str(d) if d<0 else "0")
    put_summary_line(ws, r4, ym, "전월대비 건수증감", diffs)
    color_deltas(ws, r4, diffs)
    log(f"[summary] {ym} 전월대비 -> row={r4}")

    r5 = find_summary_row(ws, ym, "예상건수")
    blanks = {c:"" for c in SUMMARY_COLS}
    put_summary_line(ws, r5, ym, "예상건수", blanks)
    log(f"[summary] {ym} 예상건수 -> row={r5}")

# ===================== 압구정동 탭 (원본 + 변동 로그) =====================
def ensure_apgu_sheet(sh: gspread.Spreadsheet) -> gspread.Worksheet:
    ws = find_ws_exact(sh, APGU_SHEET)
    if ws: return ws
    return _retry(sh.add_worksheet, title=APGU_SHEET, rows=4000, cols=80)

def number_or_blank(v):
    if v is None: return ""
    if isinstance(v,float) and (pd.isna(v) or math.isnan(v)): return ""
    return v

def apgu_key(d: dict) -> str:
    # 중복/변동 판단 키
    parts = [
        d.get("계약년",""), d.get("계약월",""), d.get("계약일",""),
        d.get("단지명",""), d.get("전용면적(㎡)",""), d.get("동",""), d.get("층",""), d.get("거래금액(만원)","")
    ]
    return "|".join(str(x).strip() for x in parts)

def upsert_apgu_month(ws: gspread.Worksheet, df_all: pd.DataFrame, y: int, m: int, run_day: date):
    # 해당 월(계약년/계약월) & 압구정동 행만
    cond = (
        (df_all.get("광역","")=="서울특별시") &
        (df_all.get("법정동","")=="압구정동") &
        (pd.to_numeric(df_all.get("계약년",0), errors="coerce")==y) &
        (pd.to_numeric(df_all.get("계약월",0), errors="coerce")==m)
    )
    df = df_all[cond].copy()
    if df.empty:
        log(f"[압구정동] {ym_label(y,m)} no rows")
        return

    # 정렬: 계약일 오름차순
    for c in ["계약년","계약월","계약일"]:
        if c not in df.columns: df[c] = pd.NA
    df = df.sort_values(["계약년","계약월","계약일"], ascending=[True,True,True], kind="mergesort")

    # 시트 전체 읽기
    vals = _retry(ws.get_all_values) or []
    if not vals:
        header = list(df.columns) + ["기록일"]
        values_batch_update(ws, [{"range":"A1","values":[header]}])
        vals = [header]
    else:
        header = vals[0]
        if "기록일" not in header:
            header = header + ["기록일"]
            values_batch_update(ws, [{"range":"A1","values":[header]}])
            vals = _retry(ws.get_all_values) or [header]
        # 헤더 합치기(새 컬럼 발생 대비)
        union = list(dict.fromkeys(header + [c for c in df.columns if c not in header]))
        if union != header:
            header = union
            values_batch_update(ws, [{"range":"A1","values":[header]}])
            vals = _retry(ws.get_all_values) or [header]

    existing = vals[1:]
    idx = {h:i for i,h in enumerate(header)}
    def row_to_dict(r):
        return {k:(r[i] if i<len(r) else "") for k,i in idx.items()}

    # 기존 키 셋
    existing_keys = set()
    for r in existing:
        existing_keys.add(apgu_key(row_to_dict(r)))

    # 이번 파일의 키
    file_keys = set()
    for _, r in df.iterrows():
        d = {k: r.get(k, "") for k in header if k in df.columns}
        file_keys.add(apgu_key(d))

    # 신규 추가
    new_records = []
    for _, r in df.iterrows():
        d = {k: r.get(k, "") for k in header if k in df.columns}
        k = apgu_key(d)
        if k not in existing_keys:
            rec = [number_or_blank(r.get(col,"")) for col in header if col!="기록일"]
            rec.append(kdate(run_day))
            new_records.append(rec)
            existing_keys.add(k)

    # 삭제된 것(시트엔 있는데 파일에는 없는 키)
    removed_rows = []
    for r in existing:
        d = row_to_dict(r)
        if apgu_key(d) not in file_keys:
            removed_rows.append([d.get(h,"") for h in header])

    # append 신규
    if new_records:
        start = len(vals)+1
        end = start + len(new_records) - 1
        if ws.row_count < end:
            _retry(ws.add_rows, end - ws.row_count)
        rng = f"A{start}:{gspread.utils.rowcol_to_a1(1,len(header)).rstrip('1')}{end}"
        values_batch_update(ws, [{"range": rng, "values": new_records}])
        log(f"[압구정동] {ym_label(y,m)} new={len(new_records)}")
        vals += new_records

    # 맨 아래 변동 로그 추가(빨간 글씨)
    logs = []
    for r in new_records:
        # r은 헤더 순 배열, 다시 딕트로
        d = {h:(r[idx[h]] if h in idx and idx[h] < len(r) else "") for h in header}
        logs.append(["(신규)", kdate(run_day), d.get("계약년",""), d.get("계약월",""), d.get("계약일",""),
                     d.get("단지명",""), d.get("전용면적(㎡)",""), d.get("동",""), d.get("층",""), d.get("거래금액(만원)","")])
    for r in removed_rows:
        d = {h:(r[idx[h]] if h in idx and idx[h] < len(r) else "") for h in header}
        logs.append(["(삭제)", kdate(run_day), d.get("계약년",""), d.get("계약월",""), d.get("계약일",""),
                     d.get("단지명",""), d.get("전용면적(㎡)",""), d.get("동",""), d.get("층",""), d.get("거래금액(만원)","")])

    if logs:
        start = (len(_retry(ws.get_all_values) or [])) + 1
        end = start + len(logs)
        if ws.row_count < end:
            _retry(ws.add_rows, end - ws.row_count)
        values_batch_update(ws, [
            {"range": f"A{start}:J{start}", "values": [APGU_CHANGE_HEADER]},
            {"range": f"A{start+1}:J{end}", "values": logs},
        ])
        # 빨간 글씨
        req = {
            "repeatCell": {
                "range": {
                    "sheetId": ws.id,
                    "startRowIndex": start-1,
                    "endRowIndex": end,
                    "startColumnIndex": 0,
                    "endColumnIndex": 10
                },
                "cell": {"userEnteredFormat": {"textFormat": {"foregroundColor":{"red":1.0,"green":0.0,"blue":0.0}}}},
                "fields": "userEnteredFormat.textFormat.foregroundColor"
            }
        }
        batch_format(ws, [req])
        log(f"[압구정동] {ym_label(y,m)} removed={len(removed_rows)}")

# ===================== 메인 =====================
def parse_file_meta(p: Path) -> Optional[Tuple[int,int]]:
    # "... 2507_250928.xlsx" → (2025, 7)
    m = re.search(r"\s(\d{2})(\d{2})_", p.stem)
    if not m: return None
    yy, mm = int(m.group(1)), int(m.group(2))
    return 2000+yy, mm

def main():
    artifacts_dir = os.environ.get("ARTIFACTS_DIR", ART_DIR_DEFAULT)
    sheet_id = os.environ.get("SHEET_ID", "").strip()
    sa_json_env = os.environ.get("SA_JSON", "").strip()
    sa_path = os.environ.get("SA_PATH", "sa.json")

    # auth
    if sa_json_env:
        creds = Credentials.from_service_account_info(
            json.loads(sa_json_env),
            scopes=["https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive"]
        )
    else:
        creds = Credentials.from_service_account_file(
            sa_path,
            scopes=["https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive"]
        )
    gc = gspread.authorize(creds)
    sh = _retry(gc.open_by_key, sheet_id)
    log("[gspread] spreadsheet opened")

    files = sorted(Path(artifacts_dir).rglob("*.xlsx"))
    log(f"[collect] found {len(files)} xlsx files")

    month_cache: Dict[Tuple[int,int], Dict] = {}
    df_all_for_apgu: List[pd.DataFrame] = []

    today = datetime.now().date()

    for f in files:
        meta = parse_file_meta(f)
        if not meta: continue
        y, m = meta
        nat_title = f"전국 {y}년 {m}월"     # 실제 탭은 25년이지만 find_ws_exact가 25/2025 차이를 흡수
        se_title  = f"서울 {y}년 {m}월"

        log(f"[file] {f.name} -> nat='{nat_title}' / seoul='{se_title}' / ym={ym_label(y,m)}")

        df = read_month_df(f)
        log(f"[read] rows={len(df)} cols={len(df.columns)}")

        counts, med, mean = agg_all_stats(df)
        month_cache[(y,m)] = {"counts": counts, "med": med, "mean": mean}

        # 월별 탭 기록(오늘 날짜)
        ws_nat = find_ws_exact(sh, nat_title)
        if ws_nat:
            write_counts_to_month_sheet(ws_nat, today, counts, sum_col_name="총합계")
        ws_se = find_ws_exact(sh, se_title)
        if ws_se:
            write_counts_to_month_sheet(ws_se, today, counts, sum_col_name="총합계")

        # 압구정동 원본 저장(전체 모은 후 월별로 처리)
        df_all_for_apgu.append(df)

    # 거래요약
    ws_sum = find_ws_exact(sh, SUMMARY_SHEET)
    if ws_sum:
        for (y,m) in sorted(month_cache.keys()):
            cur = month_cache[(y,m)]
            prev = month_cache.get((y, m-1)) if m>1 else month_cache.get((y-1, 12))
            prev_counts = prev["counts"] if prev else None
            write_month_summary(ws_sum, y, m, cur["counts"], cur["med"], cur["mean"], prev_counts)

    # 압구정동: 월별 원본 + 변동 로그
    if df_all_for_apgu:
        ws_ap = ensure_apgu_sheet(sh)
        all_df = pd.concat(df_all_for_apgu, ignore_index=True)
        months = sorted({(int(x),int(y)) for x,y in zip(pd.to_numeric(all_df["계약년"], errors="coerce").fillna(0).astype(int),
                                                      pd.to_numeric(all_df["계약월"], errors="coerce").fillna(0).astype(int))
                         if x and y})
        run_day = today
        for (yy,mm) in months:
            upsert_apgu_month(ws_ap, all_df, yy, mm, run_day)

    log("[main] done")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"[ERROR] {repr(e)}")
        raise
