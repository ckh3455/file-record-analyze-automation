# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo

import gspread
from gspread.exceptions import WorksheetNotFound
from google.oauth2.service_account import Credentials


ANCHOR_SHEET_NAME = "압구정동 거래데이터"
MONTH_ROWS = 400
MONTH_COLS = 40

SEOUL_REGIONS = [
    "강남구", "강동구", "강북구", "강서구", "관악구", "광진구", "구로구", "금천구", "노원구", "도봉구",
    "동대문구", "동작구", "마포구", "서대문구", "서초구", "성동구", "성북구", "송파구", "양천구", "영등포구",
    "용산구", "은평구", "종로구", "중구", "중랑구", "총합계",
]

NATION_REGIONS = [
    "전국", "서울특별시", "부산광역시", "대구광역시", "인천광역시", "광주광역시",
    "대전광역시", "울산광역시", "세종특별자치시", "경기도", "강원도",
    "충청북도", "충청남도", "전북특별자치도", "전라남도",
    "경상북도", "경상남도", "제주특별자치도",
]


def a1_col(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def load_creds() -> Credentials:
    raw = (os.environ.get("SA_JSON") or os.environ.get("GDRIVE_SA_JSON") or "").strip()
    if not raw:
        raise RuntimeError("SA_JSON(또는 GDRIVE_SA_JSON) 환경변수가 필요합니다.")
    info = json.loads(raw)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    return Credentials.from_service_account_info(info, scopes=scopes)


def ensure_sheet(sh: gspread.Spreadsheet, title: str, header: list[str]) -> gspread.Worksheet:
    try:
        ws = sh.worksheet(title)
        print(f"[month-tab] exists: {title}")
        return ws
    except WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=MONTH_ROWS, cols=MONTH_COLS)
        ws.update([header], f"A1:{a1_col(len(header))}1")
        print(f"[month-tab] created: {title}")
        return ws


def move_pair_after_anchor(sh: gspread.Spreadsheet, seoul_ws: gspread.Worksheet, nation_ws: gspread.Worksheet) -> None:
    sheets = sh.worksheets()
    anchor_index = next((i for i, ws in enumerate(sheets) if ws.title == ANCHOR_SHEET_NAME), None)
    if anchor_index is None:
        raise RuntimeError(f"기준 탭 '{ANCHOR_SHEET_NAME}'을 찾지 못했습니다.")

    sh.batch_update({
        "requests": [
            {
                "updateSheetProperties": {
                    "properties": {"sheetId": seoul_ws.id, "index": anchor_index + 1},
                    "fields": "index",
                }
            },
            {
                "updateSheetProperties": {
                    "properties": {"sheetId": nation_ws.id, "index": anchor_index + 2},
                    "fields": "index",
                }
            },
        ]
    })
    print(
        f"[month-tab] positioned after '{ANCHOR_SHEET_NAME}': "
        f"{seoul_ws.title} -> {anchor_index + 1}, {nation_ws.title} -> {anchor_index + 2}"
    )


def main() -> None:
    sheet_id = os.environ.get("SHEET_ID", "").strip()
    if not sheet_id:
        raise RuntimeError("SHEET_ID 환경변수가 필요합니다.")

    now = datetime.now(ZoneInfo("Asia/Seoul"))
    yy = now.year % 100
    month = now.month

    seoul_title = f"서울 {yy}년 {month}월"
    nation_title = f"전국 {yy}년 {month}월"

    creds = load_creds()
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)

    # 생성 규칙은 반드시 '서울 YY년 M월', '전국 YY년 M월'만 사용한다.
    # 연도 없는 '서울 M월', '전국 M월' 탭은 현재월 탭으로 사용하지 않는다.
    seoul_ws = ensure_sheet(sh, seoul_title, ["날짜", *SEOUL_REGIONS])
    nation_ws = ensure_sheet(sh, nation_title, ["날짜", *NATION_REGIONS])

    # 최신 월 탭 2개는 항상 '압구정동 거래데이터' 바로 뒤에 서울 -> 전국 순서로 고정한다.
    move_pair_after_anchor(sh, seoul_ws, nation_ws)


if __name__ == "__main__":
    main()
