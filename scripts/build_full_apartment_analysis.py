# -*- coding: utf-8 -*-
from __future__ import annotations

import io
import json
import math
import os
import re
import tempfile
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload


# ===================== 설정 =====================
APT_FOLDER_ID = os.environ.get(
    "APT_ANALYSIS_FOLDER_ID", "1ZlhCi3vth9OXW7zw8E7xWX-_5YUEZiFY"
).strip()
OUTPUT_NAME = os.environ.get("APT_ANALYSIS_SHEET_NAME", "아파트 실거래분석").strip()
REPORT_DIR = Path(os.environ.get("ANALYZE_REPORT_DIR", "analyze_report"))
REPORT_DIR.mkdir(parents=True, exist_ok=True)

FILE_RE = re.compile(r"^아파트\s*(20\d{4})\.(csv|xlsx)$", re.IGNORECASE)
KST = ZoneInfo("Asia/Seoul")

PROVINCE_ORDER = [
    "서울특별시", "부산광역시", "대구광역시", "인천광역시", "광주광역시",
    "대전광역시", "울산광역시", "세종특별자치시", "경기도", "강원도",
    "강원특별자치도", "충청북도", "충청남도", "전북특별자치도", "전라북도",
    "전라남도", "경상북도", "경상남도", "제주특별자치도",
]
SEOUL_GU_ORDER = [
    "강남구", "강동구", "강북구", "강서구", "관악구", "광진구", "구로구", "금천구",
    "노원구", "도봉구", "동대문구", "동작구", "마포구", "서대문구", "서초구", "성동구",
    "성북구", "송파구", "양천구", "영등포구", "용산구", "은평구", "종로구", "중구", "중랑구",
]


def log(msg: str) -> None:
    print(msg, flush=True)


def load_credentials() -> Credentials:
    raw = (os.environ.get("GDRIVE_SA_JSON") or os.environ.get("SA_JSON") or "").strip()
    if not raw:
        raise RuntimeError("GDRIVE_SA_JSON 또는 SA_JSON 환경변수가 필요합니다.")
    info = json.loads(raw)
    scopes = [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets",
    ]
    return Credentials.from_service_account_info(info, scopes=scopes)


def drive_list_all(drive, q: str, drive_id: Optional[str] = None) -> List[dict]:
    out: List[dict] = []
    token = None
    while True:
        kwargs = {
            "q": q,
            "fields": "nextPageToken,files(id,name,mimeType,modifiedTime,createdTime,size,parents,driveId)",
            "pageSize": 1000,
            "supportsAllDrives": True,
            "includeItemsFromAllDrives": True,
        }
        if drive_id:
            kwargs.update({"corpora": "drive", "driveId": drive_id})
        else:
            kwargs.update({"corpora": "allDrives"})
        if token:
            kwargs["pageToken"] = token
        resp = drive.files().list(**kwargs).execute()
        out.extend(resp.get("files", []))
        token = resp.get("nextPageToken")
        if not token:
            return out


def _ts(file_meta: dict) -> float:
    s = file_meta.get("modifiedTime") or file_meta.get("createdTime") or ""
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0


def pick_monthly_sources(drive) -> Tuple[List[dict], dict]:
    folder = drive.files().get(
        fileId=APT_FOLDER_ID,
        fields="id,name,driveId,mimeType",
        supportsAllDrives=True,
    ).execute()
    drive_id = folder.get("driveId")
    q = f"'{APT_FOLDER_ID}' in parents and trashed=false"
    items = drive_list_all(drive, q=q, drive_id=drive_id)

    by_ym: Dict[str, List[dict]] = defaultdict(list)
    for item in items:
        m = FILE_RE.match(str(item.get("name", "")).strip())
        if not m:
            continue
        item = dict(item)
        item["ym"] = m.group(1)
        item["ext"] = m.group(2).lower()
        by_ym[item["ym"]].append(item)

    if not by_ym:
        raise RuntimeError("아파트 YYYYMM.csv/xlsx 파일을 찾지 못했습니다.")

    chosen: List[dict] = []
    for ym, group in by_ym.items():
        # 같은 월의 중복 집계를 막기 위해 CSV를 우선하고, 같은 형식이면 최신 수정본 하나만 선택.
        group = sorted(group, key=lambda x: (x["ext"] == "csv", _ts(x)), reverse=True)
        chosen.append(group[0])

    chosen.sort(key=lambda x: x["ym"])
    log(
        f"[source] folder={folder.get('name')} drive_id={drive_id} "
        f"listed={len(items)} matched_months={len(chosen)} "
        f"period={chosen[0]['ym']}~{chosen[-1]['ym']}"
    )
    return chosen, folder


def download_drive_file(drive, meta: dict, target_dir: Path) -> Path:
    out = target_dir / meta["name"]
    req = drive.files().get_media(fileId=meta["id"], supportsAllDrives=True)
    with out.open("wb") as f:
        dl = MediaIoBaseDownload(f, req, chunksize=4 * 1024 * 1024)
        done = False
        while not done:
            _, done = dl.next_chunk()
    return out


def read_csv_flexible(path: Path) -> pd.DataFrame:
    last: Optional[Exception] = None
    for enc in ("utf-8-sig", "utf-8", "cp949", "euc-kr"):
        try:
            return pd.read_csv(path, dtype=str, encoding=enc, low_memory=False).fillna("")
        except Exception as e:
            last = e
    raise RuntimeError(f"CSV 읽기 실패: {path.name}: {last}")


def read_month_file(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        return read_csv_flexible(path)
    xls = pd.ExcelFile(path)
    names = list(xls.sheet_names or [])
    if not names:
        raise RuntimeError(f"시트 없음: {path.name}")
    picked = "data" if "data" in names else ("Sheet1" if "Sheet1" in names else names[0])
    return pd.read_excel(path, sheet_name=picked, dtype=str).fillna("")


def norm_col(c: str) -> str:
    return re.sub(r"\s+", "", str(c)).replace("\ufeff", "")


def find_col(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    cmap = {norm_col(c): c for c in df.columns}
    for cand in candidates:
        key = norm_col(cand)
        if key in cmap:
            return cmap[key]
    return None


def clean_text_series(s: pd.Series) -> pd.Series:
    return s.astype(str).str.replace("\u3000", " ", regex=False).str.strip()


def parse_amount_manwon(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.replace(",", "", regex=False)
    s = s.str.replace(r"[^0-9.\-]", "", regex=True)
    return pd.to_numeric(s, errors="coerce")


def derive_province_and_gu(df: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
    prov_col = find_col(df, ["광역", "시도", "시도명", "광역시도"])
    gu_col = find_col(df, ["구", "시군구명", "자치구"])
    sigungu_col = find_col(df, ["시군구", "소재지"])

    if prov_col:
        province = clean_text_series(df[prov_col])
    else:
        if not sigungu_col:
            raise RuntimeError("광역 또는 시군구 열이 없어 지역을 판별할 수 없습니다.")
        full = clean_text_series(df[sigungu_col])
        province = full.str.split().str[0].fillna("")

    if gu_col:
        gu = clean_text_series(df[gu_col])
    elif sigungu_col:
        full = clean_text_series(df[sigungu_col])
        gu = full.str.extract(r"서울특별시\s+([^\s]+구)", expand=False).fillna("")
    else:
        gu = pd.Series([""] * len(df), index=df.index, dtype="object")

    return province, gu


def valid_transaction_mask(df: pd.DataFrame) -> pd.Series:
    cancel_col = find_col(df, ["해제사유발생일", "해제사유 발생일", "해제일자", "해제일"])
    if not cancel_col:
        return pd.Series(True, index=df.index)
    s = clean_text_series(df[cancel_col]).str.lower()
    return s.isin(["", "-", "nan", "none", "nat"])


def band_upper_from_eok(price_eok: pd.Series) -> pd.Series:
    # 1.0000억은 '1억 이하', 1.0001억은 '1억 초과~2억 이하'.
    return np.ceil(price_eok).astype("Int64")


def band_label(upper: int) -> str:
    if upper <= 1:
        return "1억 이하"
    return f"{upper - 1}억 초과~{upper}억 이하"


def aggregate_region(
    ym: str,
    file_name: str,
    price_eok: pd.Series,
    region_series: pd.Series,
    level: str,
    preferred_order: List[str],
    include_overall_name: Optional[str] = None,
    overall_level: Optional[str] = None,
) -> List[dict]:
    tmp = pd.DataFrame({"price_eok": price_eok, "region": region_series}).dropna(subset=["price_eok"])
    tmp = tmp[tmp["price_eok"] > 0].copy()
    tmp["region"] = clean_text_series(tmp["region"])
    tmp = tmp[tmp["region"] != ""]
    if tmp.empty:
        return []

    records: List[dict] = []

    def one_region(region_name: str, sub: pd.DataFrame, out_level: str) -> None:
        total_sum = float(sub["price_eok"].sum())
        total_count = int(len(sub))
        records.append({
            "연월": ym,
            "구분": out_level,
            "지역": region_name,
            "금액구간": "전체",
            "구간상한(억원)": 0,
            "거래건수": total_count,
            "거래금액합계(억원)": round(total_sum, 4),
            "평균거래금액(억원)": round(total_sum / total_count, 4) if total_count else 0,
            "원본파일": file_name,
        })
        s = sub.copy()
        s["upper"] = band_upper_from_eok(s["price_eok"])
        grouped = s.groupby("upper", dropna=True)["price_eok"].agg(["count", "sum", "mean"]).reset_index()
        for _, r in grouped.iterrows():
            upper = int(r["upper"])
            records.append({
                "연월": ym,
                "구분": out_level,
                "지역": region_name,
                "금액구간": band_label(upper),
                "구간상한(억원)": upper,
                "거래건수": int(r["count"]),
                "거래금액합계(억원)": round(float(r["sum"]), 4),
                "평균거래금액(억원)": round(float(r["mean"]), 4),
                "원본파일": file_name,
            })

    if include_overall_name:
        one_region(include_overall_name, tmp, overall_level or level)

    present = list(dict.fromkeys(tmp["region"].tolist()))
    order_map = {v: i for i, v in enumerate(preferred_order)}
    present.sort(key=lambda x: (order_map.get(x, 9999), x))
    for region in present:
        one_region(region, tmp[tmp["region"] == region], level)

    return records


def process_month(df: pd.DataFrame, ym: str, file_name: str) -> Tuple[List[dict], List[dict], dict]:
    amount_col = find_col(df, ["거래금액(만원)", "거래금액", "거래금액만원"])
    if not amount_col:
        raise RuntimeError(f"거래금액 열을 찾지 못했습니다. columns={list(df.columns)}")

    province, gu = derive_province_and_gu(df)
    amount = parse_amount_manwon(df[amount_col])
    valid = valid_transaction_mask(df) & amount.notna() & (amount > 0)

    work = pd.DataFrame({
        "amount_manwon": amount[valid],
        "province": province[valid],
        "gu": gu[valid],
    }).copy()
    work["price_eok"] = work["amount_manwon"] / 10000.0

    national_records = aggregate_region(
        ym=ym,
        file_name=file_name,
        price_eok=work["price_eok"],
        region_series=work["province"],
        level="광역",
        preferred_order=PROVINCE_ORDER,
        include_overall_name="전국",
        overall_level="전국",
    )

    seoul = work[work["province"] == "서울특별시"].copy()
    seoul_records: List[dict] = []
    if not seoul.empty:
        seoul_records = aggregate_region(
            ym=ym,
            file_name=file_name,
            price_eok=seoul["price_eok"],
            region_series=seoul["gu"],
            level="자치구",
            preferred_order=SEOUL_GU_ORDER,
            include_overall_name="서울전체",
            overall_level="서울전체",
        )

    stat = {
        "ym": ym,
        "file": file_name,
        "rows_raw": int(len(df)),
        "rows_valid": int(valid.sum()),
        "rows_cancel_or_invalid": int(len(df) - valid.sum()),
        "seoul_valid": int(len(seoul)),
    }
    return national_records, seoul_records, stat


def to_output_df(records: List[dict], region_order: List[str], overall_name: str) -> pd.DataFrame:
    cols = [
        "연월", "구분", "지역", "금액구간", "구간상한(억원)",
        "거래건수", "거래금액합계(억원)", "평균거래금액(억원)", "원본파일",
    ]
    if not records:
        return pd.DataFrame(columns=cols)
    df = pd.DataFrame(records, columns=cols)
    order_map = {overall_name: -1, **{v: i for i, v in enumerate(region_order)}}
    df["_region_sort"] = df["지역"].map(order_map).fillna(9999)
    df["_band_sort"] = pd.to_numeric(df["구간상한(억원)"], errors="coerce").fillna(0)
    df = df.sort_values(
        ["연월", "_region_sort", "지역", "_band_sort"],
        ascending=[False, True, True, True],
        kind="stable",
    ).drop(columns=["_region_sort", "_band_sort"])
    return df.reset_index(drop=True)


def find_or_create_output_sheet(drive) -> Tuple[str, str]:
    folder = drive.files().get(
        fileId=APT_FOLDER_ID,
        fields="id,driveId",
        supportsAllDrives=True,
    ).execute()
    drive_id = folder.get("driveId")
    escaped = OUTPUT_NAME.replace("'", "\\'")
    q = (
        f"'{APT_FOLDER_ID}' in parents and trashed=false and "
        f"mimeType='application/vnd.google-apps.spreadsheet' and name='{escaped}'"
    )
    existing = drive_list_all(drive, q=q, drive_id=drive_id)
    if existing:
        existing.sort(key=_ts, reverse=True)
        fid = existing[0]["id"]
        meta = drive.files().get(
            fileId=fid, fields="id,name,webViewLink", supportsAllDrives=True
        ).execute()
        log(f"[output] reuse spreadsheet id={fid}")
        return fid, meta.get("webViewLink", "")

    meta = drive.files().create(
        body={
            "name": OUTPUT_NAME,
            "mimeType": "application/vnd.google-apps.spreadsheet",
            "parents": [APT_FOLDER_ID],
        },
        fields="id,name,webViewLink",
        supportsAllDrives=True,
    ).execute()
    log(f"[output] created spreadsheet id={meta['id']}")
    return meta["id"], meta.get("webViewLink", "")


def ensure_tabs(sheets, spreadsheet_id: str) -> Dict[str, int]:
    meta = sheets.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    existing = {s["properties"]["title"]: s["properties"]["sheetId"] for s in meta.get("sheets", [])}
    reqs = []
    if "전국" not in existing:
        # 새 파일의 기본 Sheet1은 전국으로 재사용.
        if "Sheet1" in existing:
            reqs.append({
                "updateSheetProperties": {
                    "properties": {"sheetId": existing["Sheet1"], "title": "전국"},
                    "fields": "title",
                }
            })
        else:
            reqs.append({"addSheet": {"properties": {"title": "전국"}}})
    if "서울" not in existing:
        reqs.append({"addSheet": {"properties": {"title": "서울"}}})
    if reqs:
        sheets.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id, body={"requests": reqs}
        ).execute()
    meta = sheets.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    return {s["properties"]["title"]: s["properties"]["sheetId"] for s in meta.get("sheets", [])}


def a1_col(n: int) -> str:
    out = ""
    while n:
        n, r = divmod(n - 1, 26)
        out = chr(65 + r) + out
    return out


def resize_and_clear(sheets, spreadsheet_id: str, title: str, sheet_id: int, rows_needed: int, cols_needed: int) -> None:
    reqs = [{
        "updateSheetProperties": {
            "properties": {
                "sheetId": sheet_id,
                "gridProperties": {
                    "rowCount": max(rows_needed + 20, 1000),
                    "columnCount": max(cols_needed + 2, 12),
                    "frozenRowCount": 4,
                },
            },
            "fields": "gridProperties(rowCount,columnCount,frozenRowCount)",
        }
    }]
    sheets.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": reqs}).execute()
    sheets.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=f"'{title}'",
        body={},
    ).execute()


def write_tab(
    sheets,
    spreadsheet_id: str,
    title: str,
    sheet_id: int,
    df: pd.DataFrame,
    source_month_count: int,
    period: str,
) -> None:
    cols = list(df.columns)
    rows_needed = len(df) + 4
    resize_and_clear(sheets, spreadsheet_id, title, sheet_id, rows_needed, len(cols))

    now = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S %Z")
    meta_rows = [
        ["분석명", "아파트 실거래 금액구간 분석"],
        ["분석기준", "계약해제·금액오류 거래 제외 / 1억원 단위 금액구간 / 월별 원본은 CSV 우선, 없으면 XLSX"],
        ["생성시각", now, "원본월수", source_month_count, "기간", period],
        cols,
    ]
    sheets.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"'{title}'!A1:{a1_col(max(len(cols), 6))}4",
        valueInputOption="RAW",
        body={"values": meta_rows},
    ).execute()

    if not df.empty:
        values = df.where(pd.notna(df), "").values.tolist()
        chunk = 5000
        for i in range(0, len(values), chunk):
            part = values[i:i + chunk]
            start = 5 + i
            end = start + len(part) - 1
            sheets.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"'{title}'!A{start}:{a1_col(len(cols))}{end}",
                valueInputOption="RAW",
                body={"values": part},
            ).execute()
            log(f"[write] {title}: {end - 4:,}/{len(values):,} rows")

    # 헤더, 숫자열 서식, 필터
    reqs = [
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 3, "endRowIndex": 4, "startColumnIndex": 0, "endColumnIndex": len(cols)},
                "cell": {"userEnteredFormat": {"textFormat": {"bold": True}, "wrapStrategy": "WRAP"}},
                "fields": "userEnteredFormat(textFormat,wrapStrategy)",
            }
        },
        {
            "setBasicFilter": {
                "filter": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 3,
                        "endRowIndex": max(4, len(df) + 4),
                        "startColumnIndex": 0,
                        "endColumnIndex": len(cols),
                    }
                }
            }
        },
        {
            "autoResizeDimensions": {
                "dimensions": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": 0,
                    "endIndex": len(cols),
                }
            }
        },
    ]
    sheets.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": reqs}).execute()


def main() -> None:
    creds = load_credentials()
    drive = build("drive", "v3", credentials=creds, cache_discovery=False)
    sheets = build("sheets", "v4", credentials=creds, cache_discovery=False)

    sources, folder = pick_monthly_sources(drive)
    national_all: List[dict] = []
    seoul_all: List[dict] = []
    stats: List[dict] = []

    with tempfile.TemporaryDirectory(prefix="apt-analysis-") as td:
        tdir = Path(td)
        for idx, meta in enumerate(sources, start=1):
            log(f"[read] {idx}/{len(sources)} {meta['name']}")
            path = download_drive_file(drive, meta, tdir)
            try:
                df = read_month_file(path)
                nat, seo, stat = process_month(df, meta["ym"], meta["name"])
                national_all.extend(nat)
                seoul_all.extend(seo)
                stats.append(stat)
                log(
                    f"[agg] {meta['ym']} raw={stat['rows_raw']:,} valid={stat['rows_valid']:,} "
                    f"seoul={stat['seoul_valid']:,} nat_rows={len(nat):,} seoul_rows={len(seo):,}"
                )
            finally:
                try:
                    path.unlink()
                except Exception:
                    pass

    national_df = to_output_df(national_all, PROVINCE_ORDER, "전국")
    seoul_df = to_output_df(seoul_all, SEOUL_GU_ORDER, "서울전체")
    log(f"[result] 전국 rows={len(national_df):,}, 서울 rows={len(seoul_df):,}")

    spreadsheet_id, web_link = find_or_create_output_sheet(drive)
    tabs = ensure_tabs(sheets, spreadsheet_id)
    period = f"{sources[0]['ym']}~{sources[-1]['ym']}"
    write_tab(sheets, spreadsheet_id, "전국", tabs["전국"], national_df, len(sources), period)
    write_tab(sheets, spreadsheet_id, "서울", tabs["서울"], seoul_df, len(sources), period)

    summary = {
        "spreadsheet_id": spreadsheet_id,
        "web_link": web_link,
        "folder_id": APT_FOLDER_ID,
        "folder_name": folder.get("name"),
        "source_month_count": len(sources),
        "period": period,
        "national_rows": len(national_df),
        "seoul_rows": len(seoul_df),
        "source_stats": stats,
    }
    (REPORT_DIR / "full_apartment_analysis_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    log(f"[DONE] {web_link or spreadsheet_id}")


if __name__ == "__main__":
    main()
