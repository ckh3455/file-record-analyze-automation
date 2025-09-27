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

# ===================== Logging =====================
LOG_DIR = Path("analyze_report")
LOG_DIR.mkdir(parents=True, exist_ok=True)
RUN_LOG = LOG_DIR / f"run-{datetime.now(ZoneInfo('Asia/Seoul')).strftime('%Y%m%dT%H%M%S%z')}.log"
LATEST  = LOG_DIR / "latest.log"
WRITTEN = LOG_DIR / "where_written.txt"

def _t(): return datetime.now(ZoneInfo("Asia/Seoul")).strftime("[%H:%M:%S]")

def _write_file(path: Path, text: str):
    with path.open("a", encoding="utf-8") as f: f.write(text+"\n")

def log(msg: str):
    line = f"{_t()} {msg}"
    print(line)
    _write_file(RUN_LOG, line)
    _write_file(LATEST, line)

def log_error(msg: str, e: Optional[BaseException]=None):
    log(f"[ERROR] {msg}")
    if e:
        import traceback
        tb = "".join(traceback.format_exception(type(e), e, e.__traceback__))
        print(tb, file=sys.stderr)
        _write_file(RUN_LOG, tb); _write_file(LATEST, tb)

def note_written(s: str):
    _write_file(WRITTEN, s.rstrip())

# ===================== Normalizers / Maps =====================
def norm(s: str) -> str:
    if s is None: return ""
    return str(s).replace("\u00A0","").replace(" ","").strip()

# 원본 → 정식명
CANON_REG = {
    "서울특별시":"서울특별시",
    "세종특별자치시":"세종특별자치시",
    "강원도":"강원특별자치도",
    "강원특별자치도":"강원특별자치도",
    "경기도":"경기도",
    "경상남도":"경상남도",
    "경상북도":"경상북도",
    "광주광역시":"광주광역시",
    "대구광역시":"대구광역시",
    "대전광역시":"대전광역시",
    "부산광역시":"부산광역시",
    "울산광역시":"울산광역시",
    "인천광역시":"인천광역시",
    "전라남도":"전라남도",
    "전라북도":"전북특별자치도",
    "전북특별자치도":"전북특별자치도",
    "제주특별자치도":"제주특별자치도",
    "충청남도":"충청남도",
    "충청북도":"충청북도",
}
CANON_REG_N = {norm(k): norm(v) for k,v in CANON_REG.items()}
SEOUL_N = norm("서울특별시")

# 거래요약 탭의 "열 제목" → 우리가 집계할 키(정식명 또는 구/동)
# 사용자가 요구한 축약 헤더 명칭 반영
SUMMARY_COL_TO_KEY = {
    "전국"  : "전국",
    "서울"  : "서울특별시",
    "세종시": "세종특별자치시",
    "강원도": "강원특별자치도",
    "경남"  : "경상남도",
    "경북"  : "경상북도",
    "광주"  : "광주광역시",
    "대구"  : "대구광역시",
    "대전"  : "대전광역시",
    "부산"  : "부산광역시",
    "울산"  : "울산광역시",
    "전남"  : "전라남도",
    "전북"  : "전북특별자치도",
    "제주"  : "제주특별자치도",
    "충남"  : "충청남도",
    "충북"  : "충청북도",

    # 서울 구들
    "강남구":"강남구","서초구":"서초구","송파구":"송파구","용산구":"용산구","강동구":"강동구",
    "성동구":"성동구","마포구":"마포구","양천구":"양천구","동작구":"동작구","영등포구":"영등포구",
    "종로구":"종로구","광진구":"광진구","강서구":"강서구",

    # 법정동(압구정동)
    "압구정동":"압구정동",
}
SUMMARY_COL_TO_KEY_N = {norm(k): SUMMARY_COL_TO_KEY[k] for k in SUMMARY_COL_TO_KEY}

# ===================== File name / date utils =====================
FN_RE = re.compile(r".*?(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})\.xlsx$")
def parse_filename(fname: str) -> Tuple[int,int,int]:
    m = FN_RE.match(fname)
    if not m: raise ValueError(f"unexpected filename: {fname}")
    y = 2000 + int(m.group(1))
    mth = int(m.group(2))
    day = int(m.group(5))
    return y, mth, day

def ym_label(y: int, m: int) -> str:
    return f"{y%100:02d}/{m:02d}"

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

# ===================== Read & aggregate =====================
def read_molit_xlsx(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name="data", dtype=str, engine="openpyxl")
    df = df.fillna("")
    must = ["광역","구","법정동","계약년","계약월","계약일","거래금액(만원)"]
    missing = [c for c in must if c not in df.columns]
    if missing: raise ValueError(f"missing columns: {missing}")

    # 숫자정리
    for c in ["계약년","계약월","계약일","거래금액(만원)"]:
        df[c] = df[c].astype(str).str.replace(r"[^\d]", "", regex=True)

    return df

def canon_region(s: str) -> str:
    return CANON_REG_N.get(norm(s), norm(s))

def canon_gu(s: str) -> str:
    return norm(s)

def agg_counts(df: pd.DataFrame) -> Dict[str,int]:
    # 전국/광역/구/압구정동 등 모든 키별 건수 집계
    out: Dict[str,int] = {}

    # 전국 총건수
    out["전국"] = int(len(df))

    # 광역
    ser_reg = df["광역"].map(canon_region)
    for k, c in ser_reg.value_counts().items():
        out[k] = int(c)

    # 서울 구
    is_seoul = ser_reg == SEOUL_N
    if is_seoul.any():
        ser_gu = df.loc[is_seoul, "구"].map(canon_gu)
        for k, c in ser_gu.value_counts().items():
            out[k] = int(c)

    # 압구정동 (서울/강남구/압구정동)
    mask_apgu = (ser_reg==SEOUL_N) & \
                (df["구"].map(canon_gu)==norm("강남구")) & \
                (df["법정동"].map(norm)==norm("압구정동"))
    out["압구정동"] = int(mask_apgu.sum())

    return out

def agg_prices(df: pd.DataFrame) -> Dict[str, Dict[str, float]]:
    """ 각 키별 중앙값/평균(억) """
    price_eok = pd.to_numeric(df["거래금액(만원)"], errors="coerce").fillna(0)/10000.0
    res: Dict[str, Dict[str, float]] = {}

    def put(key: str, mask):
        sub = price_eok[mask]
        if sub.empty:
            res[key] = {"median":0.0, "mean":0.0}
        else:
            res[key] = {"median": round(float(sub.median()),2),
                        "mean":   round(float(sub.mean()),2)}

    # 전국
    put("전국", price_eok.index==price_eok.index)  # all True

    # 광역
    ser_reg = df["광역"].map(canon_region)
    for reg in ser_reg.unique():
        if not reg: continue
        put(reg, ser_reg==reg)

    # 서울 구
    is_seoul = ser_reg==SEOUL_N
    if is_seoul.any():
        ser_gu = df["구"].map(canon_gu)
        for gu in ser_gu[is_seoul].unique():
            if not gu: continue
            put(gu, (is_seoul & (ser_gu==gu)))

    # 압구정동
    ser_dong = df["법정동"].map(norm)
    put("압구정동", is_seoul & (df["구"].map(canon_gu)==norm("강남구")) & (ser_dong==norm("압구정동")))

    return res

# ===================== Sheets helpers =====================
def fuzzy_ws(sh, want_title: str):
    want = norm(want_title)
    for ws in sh.worksheets():
        if norm(ws.title) == want:
            log(f"[ws] fuzzy matched: '{ws.title}'")
            return ws
    return None

def find_summary_ws(sh) -> gspread.Worksheet:
    ws = fuzzy_ws(sh, "거래요약")
    if not ws:
        raise RuntimeError("거래요약 탭이 없습니다. 시트에 기존 탭을 만들어 주세요.")
    return ws

def get_header_map(ws) -> Dict[str,int]:
    header = ws.row_values(1)
    mp: Dict[str,int] = {}
    for idx, h in enumerate(header, start=1):
        nh = norm(h)
        if nh: mp[nh] = idx
    return mp

def find_row_by_ym_and_section(ws, ym: str, section: str) -> Optional[int]:
    vals = ws.get_all_values()
    for r, row in enumerate(vals[1:], start=2):
        if len(row) >= 2 and row[0].strip()==ym and row[1].strip()==section:
            return r
    return None

def append_row_idx(ws) -> int:
    used = len(ws.get_all_values())
    return used + 1

def a1(row: int, col: int) -> str:
    return gspread.utils.rowcol_to_a1(row, col)

def batch_format_color(ws: gspread.Worksheet, ranges_colors: List[Tuple[str, Tuple[float,float,float]]]):
    # 텍스트 색상 지정 (RGB 0~1)
    if not ranges_colors: return
    req = []
    sid = ws._properties["sheetId"]
    for rng, (r,g,b) in ranges_colors:
        req.append({
          "repeatCell":{
            "range": {
              "sheetId": sid,
              "startRowIndex": ws.range(rng)[0].row-1,
              "endRowIndex": ws.range(rng)[-1].row,
              "startColumnIndex": ws.range(rng)[0].col-1,
              "endColumnIndex": ws.range(rng)[-1].col,
            },
            "cell": {"userEnteredFormat":{"textFormat":{"foregroundColor":{"red":r,"green":g,"blue":b}}}},
            "fields":"userEnteredFormat.textFormat.foregroundColor"
          }
        })
    ws.spreadsheet.batch_update({"requests": req})

# ===================== Write “월 요약” =====================
def write_month_summary(ws: gspread.Worksheet,
                        y:int, m:int,
                        counts: Dict[str,int],
                        prices: Dict[str, Dict[str,float]],
                        prev_counts: Optional[Dict[str,int]]):
    ym = ym_label(y,m)
    header_map = get_header_map(ws)

    # 열 이름 → 키 매핑을 통해, 실제 쓰일 열 index를 구한다.
    # (없으면 건너뜀)
    def col_for(label: str) -> Optional[int]:
        nh = norm(label)
        if nh not in header_map: return None
        return header_map[nh]

    # 1) 거래건수
    row = find_row_by_ym_and_section(ws, ym, "거래건수")
    mode = "update" if row else "append"
    if not row: row = append_row_idx(ws)
    ws.update([[ym, "거래건수"]], f"A{row}:B{row}")

    for head, key in SUMMARY_COL_TO_KEY.items():
        cidx = col_for(head)
        if not cidx: continue
        val = int(counts.get(key, 0))
        ws.update([[val]], a1(row, cidx))

    log(f"[summary] {ym} 거래건수 -> {mode} row={row}")

    # 2) 중앙값(단위:억)
    row2 = find_row_by_ym_and_section(ws, ym, "중앙값(단위:억)")
    mode2 = "update" if row2 else "append"
    if not row2: row2 = append_row_idx(ws)
    ws.update([[ym, "중앙값(단위:억)"]], f"A{row2}:B{row2}")

    for head, key in SUMMARY_COL_TO_KEY.items():
        cidx = col_for(head)
        if not cidx: continue
        med = float(prices.get(key, {}).get("median", 0.0))
        ws.update([[med]], a1(row2, cidx))
    log(f"[summary] {ym} 중앙값 -> {mode2} row={row2}")

    # 3) 평균가(단위:억)
    row3 = find_row_by_ym_and_section(ws, ym, "평균가(단위:억)")
    mode3 = "update" if row3 else "append"
    if not row3: row3 = append_row_idx(ws)
    ws.update([[ym, "평균가(단위:억)"]], f"A{row3}:B{row3}")

    for head, key in SUMMARY_COL_TO_KEY.items():
        cidx = col_for(head)
        if not cidx: continue
        meanv = float(prices.get(key, {}).get("mean", 0.0))
        ws.update([[meanv]], a1(row3, cidx))
    log(f"[summary] {ym} 평균가 -> {mode3} row={row3}")

    # 4) 전월대비 건수증감 (+파랑 / -빨강, 표시값은 +n / -n)
    row4 = find_row_by_ym_and_section(ws, ym, "전월대비 건수증감")
    mode4 = "update" if row4 else "append"
    if not row4: row4 = append_row_idx(ws)
    ws.update([[ym, "전월대비 건수증감"]], f"A{row4}:B{row4}")

    color_updates: List[Tuple[str, Tuple[float,float,float]]] = []
    for head, key in SUMMARY_COL_TO_KEY.items():
        cidx = col_for(head)
        if not cidx: continue
        cur = int(counts.get(key, 0))
        prv = int(prev_counts.get(key, 0)) if prev_counts else 0
        diff = cur - prv
        display = f"+{diff}" if diff > 0 else (f"{diff}" if diff < 0 else "0")
        cell = a1(row4, cidx)
        ws.update([[display]], cell)

        if diff > 0:
            color_updates.append((cell, (0.0, 0.35, 1.0)))  # 파랑
        elif diff < 0:
            color_updates.append((cell, (1.0, 0.2, 0.2)))   # 빨강

    if color_updates:
        # 여러 칸 색칠
        # (gspread의 repeatCell 범위 계산을 위해 하나씩 range()를 부르므로 소량에서도 충분)
        # 여기선 단일셀들만 지정
        reqs = []
        sid = ws._properties["sheetId"]
        for cell, (r,g,b) in color_updates:
            c = ws.range(cell)[0]
            reqs.append({
              "repeatCell":{
                "range": {
                  "sheetId": sid,
                  "startRowIndex": c.row-1,
                  "endRowIndex": c.row,
                  "startColumnIndex": c.col-1,
                  "endColumnIndex": c.col
                },
                "cell": {"userEnteredFormat":{"textFormat":{"foregroundColor":{"red":r,"green":g,"blue":b}}}},
                "fields":"userEnteredFormat.textFormat.foregroundColor"
              }
            })
        ws.spreadsheet.batch_update({"requests": reqs})

    log(f"[summary] {ym} 전월대비 건수증감 -> {mode4} row={row4}")

# ===================== 전국/서울 시트 일일기록 =====================
def tab_titles_from_file(y: int, m: int) -> Tuple[str,str]:
    return (f"전국 {y%100}년 {m}월", f"서울 {y%100}년 {m}월")

def find_date_col_idx(ws) -> int:
    header = ws.row_values(1)
    for i,v in enumerate(header, start=1):
        if str(v).strip() == "날짜":
            return i
    return 1

def find_date_row(ws, target: date, date_col_idx: int=1, header_row: int=1) -> Optional[int]:
    rng = ws.range(header_row+1, date_col_idx, ws.row_count, date_col_idx)
    for off, cell in enumerate(rng, start=header_row+1):
        v = str(cell.value).strip()
        if not v: continue
        d = parse_any_date(v)
        if d and d == target:
            return off
    return None

def build_row_by_header(header: List[str], day: date, series_norm: Dict[str,int]) -> List:
    row: List = []
    total = 0
    alias = {
        norm("강원도"): norm("강원특별자치도"),
        norm("전라북도"): norm("전북특별자치도"),
    }
    for i, h in enumerate(header):
        if i==0:
            row.append(kdate_str(day)); continue
        nh = norm(h)
        if nh in (norm("총합계"), norm("전체개수"), norm("전체 개수"), norm("합계")):
            row.append(total); continue
        val = series_norm.get(nh)
        if val is None and nh in alias:
            val = series_norm.get(alias[nh])
        if val is None: val = 0
        row.append(int(val)); total += int(val)
    return row

def upsert_row(ws, day: date, series: Dict[str,int]) -> Tuple[str,int]:
    header = ws.row_values(1)
    series_norm = {norm(k): int(v) for k,v in series.items()}
    date_col = find_date_col_idx(ws)
    row_idx = find_date_row(ws, day, date_col_idx=date_col, header_row=1)
    mode = "update" if row_idx else "append"
    if not row_idx:
        col_vals = ws.col_values(date_col)
        used = 1
        for i in range(len(col_vals), 1, -1):
            if str(col_vals[i-1]).strip():
                used = i; break
        row_idx = used + 1
    out = build_row_by_header(header, day, series_norm)

    last_a1 = gspread.utils.rowcol_to_a1(1, len(header))
    last_col = re.sub(r"\d+","", last_a1)
    ws.update([out], f"A{row_idx}:{last_col}{row_idx}")
    return mode, row_idx

# ===================== Main =====================
def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--artifacts-dir", required=True)
    ap.add_argument("--sheet-id", required=True)
    args = ap.parse_args()

    # latest.log 초기화
    try:
        if LATEST.exists(): LATEST.unlink()
    except Exception: pass

    log("[MAIN]")
    art = Path(args.artifacts_dir)
    xlsx_paths: List[Path] = sorted(art.rglob("*.xlsx"))
    log(f"[COLLECT] artifacts_dir={art}")
    log(f"[COLLECT] total xlsx under work_dir: {len(xlsx_paths)}")

    nat_files = [p for p in xlsx_paths if p.name.startswith("전국 ")]
    log(f"[COLLECT] national files count= {len(nat_files)}")

    # gspread
    sa_raw = os.environ.get("SA_JSON","").strip()
    if not sa_raw: raise RuntimeError("SA_JSON is empty")
    creds = Credentials.from_service_account_info(json.loads(sa_raw),
              scopes=["https://www.googleapis.com/auth/spreadsheets",
                      "https://www.googleapis.com/auth/drive"])
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(args.sheet_id)
    log("[gspread] spreadsheet opened")

    today_kst = datetime.now(ZoneInfo("Asia/Seoul")).date()
    log(f"[date] using today (KST) = {kdate_str(today_kst)}")

    # 월 요약용 버킷
    monthly_counts: Dict[Tuple[int,int], Dict[str,int]] = {}
    monthly_prices: Dict[Tuple[int,int], Dict[str, Dict[str,float]]] = {}

    # 1) 파일별 집계 + 일일기록(전국/서울 탭)
    for x in nat_files:
        try:
            y, m, file_day = parse_filename(x.name)
        except Exception as e:
            log_error(f"filename parse failed: {x.name}", e); continue

        nat_title, se_title = tab_titles_from_file(y,m)
        log(f"[file] {x.name} -> nat='{nat_title}' seoul='{se_title}' (file_day={file_day})")

        try:
            df = read_molit_xlsx(x)
        except Exception as e:
            log_error(f"read error: {x}", e); continue

        log(f"[read] rows={len(df)} cols={len(df.columns)}")

        # 건수/가격 집계
        counts = agg_counts(df)
        prices = agg_prices(df)
        monthly_counts[(y,m)] = counts
        monthly_prices[(y,m)] = prices

        # 전국/서울 탭 일일 기록 (오늘 날짜)
        ws_nat = fuzzy_ws(sh, nat_title)
        ws_se  = fuzzy_ws(sh, se_title)
        if ws_nat:
            mode, row = upsert_row(ws_nat, today_kst, counts)
            log(f"[전국] {ws_nat.title} -> {kdate_str(today_kst)} {mode} row={row}")
            note_written(f"{ws_nat.title}\t{kdate_str(today_kst)}\t{mode}\t{row}")
        else:
            log(f"[전국] sheet not found: '{nat_title}' (skip)")
        if ws_se:
            mode, row = upsert_row(ws_se, today_kst, counts)  # counts에는 서울 구/압구정동도 포함
            log(f"[서울] {ws_se.title} -> {kdate_str(today_kst)} {mode} row={row}")
            note_written(f"{ws_se.title}\t{kdate_str(today_kst)}\t{mode}\t{row}")
        else:
            log(f"[서울] sheet not found: '{se_title}' (skip)")

    # 2) 거래요약 탭 갱신
    if monthly_counts:
        ws_sum = find_summary_ws(sh)
        # 월 정렬
        yms = sorted(monthly_counts.keys())
        for i, (y,m) in enumerate(yms):
            counts = monthly_counts[(y,m)]
            prices = monthly_prices.get((y,m), {})
            prev = monthly_counts.get(yms[i-1]) if i>0 else None
            write_month_summary(ws_sum, y, m, counts, prices, prev)

    log("[main] logs written to analyze_report/")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log_error(str(e), e)
        sys.exit(1)
