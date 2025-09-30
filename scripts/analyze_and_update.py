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
REF_SHEET_PATH = os.environ.get("REF_SHEET_PATH", "ref_sheets.xlsx")  # 네가 준 파일 경로를 여기 넣어도 됨

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

# 광역 → 요약 열 표준 매핑
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

# 압구정동 본표 고정 열
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

def _ns(s: str) -> str:
    return re.sub(r"\s+","", s or "")

# ===================== 참조 엑셀 로딩 =====================
class RefBook:
    def __init__(self, path: Path):
        self.ok = False
        self.path = path
        self.sheets: List[str] = []
        self.header_map: Dict[str, List[str]] = {}  # sheet_name -> header_row1
        if path.exists():
            try:
                xf = pd.ExcelFile(path)
                self.sheets = list(xf.sheet_names)
                for sn in self.sheets:
                    try:
                        df = pd.read_excel(path, sheet_name=sn, nrows=1, header=None, dtype=str)
                        headers = [str(x).strip() for x in list(df.iloc[0].fillna(""))]
                        self.header_map[sn] = headers
                    except Exception:
                        self.header_map[sn] = []
                self.ok = True
                log(f"[ref] loaded: {path} sheets={len(self.sheets)}")
            except Exception as e:
                log(f"[ref] failed to load: {path} ({e})")

    def find_month_tab(self, kind: str, yy: str, mm: int) -> Optional[str]:
        """
        kind: '전국' or '서울'
        yy: '24' 같은 2자리 문자열
        """
        # 25년, 2025년 두 표기 모두 지원
        patterns = [
            rf"^{kind}\s*{yy}년\s*{mm}월$",
            rf"^{kind}\s*20{yy}년\s*{mm}월$",
        ]
        for name in self.sheets:
            ns = _ns(name)
            for pat in patterns:
                if re.match(pat.replace(" ", ""), ns):
                    return name
        return None

    def get_headers(self, sheet_name: str) -> List[str]:
        return self.header_map.get(sheet_name, [])

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
    집계 규칙:
      - 전국: '광역' 기준 합계
      - 서울: 광역=='서울특별시' 필터 후 '구' 기준 합계 + '서울' 총합 + '압구정동' 별도
    """
    counts = {col:0 for col in SUMMARY_COLS}
    med = {col:"" for col in SUMMARY_COLS}
    mean = {col:"" for col in SUMMARY_COLS}

    # 전국 집계 (광역 합)
    all_eok = eok_series(df.get("거래금액(만원)", []))
    counts["전국"] = int(len(df))
    if not all_eok.empty:
        med["전국"] = round2(all_eok.median())
        mean["전국"] = round2(all_eok.mean())

    if "광역" in df.columns:
        for prov, sub in df.groupby("광역", dropna=False):
            prov = str(prov)
            prov_std = PROV_MAP.get(prov, prov)
            if prov_std in counts:
                counts[prov_std] += int(len(sub))
                s = eok_series(sub.get("거래금액(만원)", []))
                if not s.empty:
                    med[prov_std] = round2(s.median())
                    mean[prov_std] = round2(s.mean())

    # 서울 집계
    seoul = df[df.get("광역","") == "서울특별시"].copy()
    counts["서울"] = int(len(seoul))
    if len(seoul)>0:
        s = eok_series(seoul.get("거래금액(만원)", []))
        if not s.empty:
            med["서울"] = round2(s.median())
            mean["서울"] = round2(s.mean())
        if "구" in seoul.columns:
            for gu, sub in seoul.groupby("구", dropna=False):
                gu = str(gu)
                if gu in counts:
                    counts[gu] += int(len(sub))
                    s2 = eok_series(sub.get("거래금액(만원)", []))
                    if not s2.empty:
                        med[gu] = round2(s2.median())
                        mean[gu] = round2(s2.mean())

        ap = seoul[seoul.get("법정동","")=="압구정동"]
        counts["압구정동"] = int(len(ap))
        if len(ap)>0:
            s = eok_series(ap.get("거래금액(만원)", []))
            if not s.empty:
                med["압구정동"] = round2(s.median())
                mean["압구정동"] = round2(s.mean())

    return counts, med, mean

# ===================== 탭 기록 (전국/서울) =====================
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

def write_month_sheet(ws, date_label: str, header_whitelist: List[str], values_by_colname: Dict[str,int]):
    """
    - 실제 시트의 1행을 읽어서 교집합만 기록
    - header_whitelist(참조 엑셀의 헤더 1행)를 우선 화이트리스트로 사용
    - '총합계'/'합계' 있으면 전국 또는 서울 총합 채움
    """
    real_header = _retry(ws.row_values, 1) or []
    real_header = [str(h).strip() for h in real_header]

    # 화이트리스트: 참조 헤더 ∩ 실제 헤더
    if header_whitelist:
        allowed = [h for h in header_whitelist if h in real_header]
    else:
        allowed = real_header

    hmap = {h: i+1 for i,h in enumerate(real_header) if h}
    row_idx = find_or_append_date_row(ws, date_label)
    payload = [{"range": f"A{row_idx}", "values": [[date_label]]}]

    # 아는 열만 기록
    for h in allowed:
        if h in ("", "날짜"): 
            continue
        if h in ("총합계","합계"):
            # 총합계는 아래에서 처리
            continue
        if h in hmap and (h in values_by_colname):
            payload.append({"range": f"{a1_col(hmap[h])}{row_idx}", "values": [[values_by_colname[h]]]})

    # 총합계/합계(있으면)
    total_key = "전국" if "전국" in values_by_colname else ("서울" if "서울" in values_by_colname else None)
    if total_key:
        for possible in ("총합계","합계"):
            if possible in hmap and possible in allowed:
                payload.append({"range": f"{a1_col(hmap[possible])}{row_idx}", "values": [[int(values_by_colname.get(total_key,0))]]})
                break

    if len(payload) > 1:
        values_batch_update(ws, payload)
        log(f"[ws] {ws.title} -> {date_label} row={row_idx}")
        note_where(f"{ws.title}\t(id={ws.id})\t{a1_col(1)}{row_idx}:{a1_col(len(real_header))}{row_idx}")

# ===================== 거래요약 =====================
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
            payload.append({"range": f"{a1_col(hmap[c])}{row_idx}", "values": [[line_map.get(c,"")]]})
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

    # 본표 덮어쓰기
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
    # 로그 초기화
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

    # 참조 엑셀 로드 (있으면 사용)
    ref = RefBook(Path(REF_SHEET_PATH))

    # 파일 수집
    files = sorted(Path(ARTIFACTS_DIR).rglob("전국 *.xlsx"))
    log(f"[collect] found {len(files)} xlsx files")

    month_cache = {}  # ym -> {counts, med, mean}
    today_label = kdate(datetime.now())
    run_day = datetime.now().date()
    apgu_all: List[pd.DataFrame] = []

    for p in files:
        m = re.search(r"(\d{2})(\d{2})_", p.name)
        if not m: 
            continue
        yy, mm = m.group(1), int(m.group(2))
        ym = f"{yy}/{mm}"

        # 참조 엑셀로 탭 이름 확정
        if ref.ok:
            nat_name = ref.find_month_tab("전국", yy, mm)
            se_name = ref.find_month_tab("서울", yy, mm)
            log(f"[file] {p.name} -> nat='{nat_name}' / seoul='{se_name}' / ym={ym}")
        else:
            nat_name = f"전국 20{yy}년 {mm}월"
            se_name  = f"서울 20{yy}년 {mm}월"
            log(f"[file] {p.name} -> fallback nat='{nat_name}' / seoul='{se_name}' / ym={ym}")

        df = read_month_df(p)
        log(f"[read] rows={len(df)} cols={len(df.columns)}")

        counts, med, mean = agg_all_stats(df)
        month_cache[ym] = {"counts": counts, "med": med, "mean": mean}

        # 월별 탭 기록 (참조 탭 이름으로 정확히 찾음)
        if nat_name:
            # Google Sheet에서 같은 이름의 시트를 찾기(공백무시 완전일치)
            ws_nat = None
            for ws in sh.worksheets():
                if _ns(ws.title) == _ns(nat_name):
                    ws_nat = ws; break
            if ws_nat:
                # 이 탭에서 사용할 화이트리스트 헤더
                nat_headers = ref.get_headers(nat_name) if ref.ok else (_retry(ws_nat.row_values,1) or [])
                values_nat: Dict[str,int] = {}
                # 화이트리스트에 포함된 헤더만 작성
                for h in nat_headers:
                    if not h or h == "날짜": 
                        continue
                    if h == "총합계":
                        values_nat[h] = int(counts.get("전국",0))
                    elif h in counts:
                        values_nat[h] = int(counts[h])
                write_month_sheet(ws_nat, today_label, nat_headers, values_nat)
            else:
                log(f"[전국] sheet not found: '{nat_name}' (skip)")

        if se_name:
            ws_se = None
            for ws in sh.worksheets():
                if _ns(ws.title) == _ns(se_name):
                    ws_se = ws; break
            if ws_se:
                se_headers = ref.get_headers(se_name) if ref.ok else (_retry(ws_se.row_values,1) or [])
                values_se: Dict[str,int] = {}
                for h in se_headers:
                    if not h or h == "날짜":
                        continue
                    if h == "총합계":
                        values_se[h] = int(counts.get("서울",0))
                    elif h in counts:
                        values_se[h] = int(counts[h])
                write_month_sheet(ws_se, today_label, se_headers, values_se)
            else:
                log(f"[서울] sheet not found: '{se_name}' (skip)")

        # 압구정동 원본 누적
        ap = df[(df.get("광역","")=="서울특별시") & (df.get("법정동","")=="압구정동")].copy()
        if not ap.empty:
            apgu_all.append(ap)

    # 거래요약
    ws_sum = None
    for ws in sh.worksheets():
        if _ns(ws.title) == _ns(SUMMARY_SHEET_NAME):
            ws_sum = ws; break

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
            time.sleep(0.2)

    # 압구정동 원본 그대로 + 변동요약
    if apgu_all:
        ws_ap = None
        for ws in sh.worksheets():
            if _ns(ws.title) == _ns("압구정동"):
                ws_ap = ws; break
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
