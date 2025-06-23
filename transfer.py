
# transfer.py

import pathlib
import json
import time
import re
import requests
import sys
from datetime import datetime
from rapidfuzz import process, fuzz
from groq import Groq
import pandas as pd

LOG_STORE = []

def log_message(msg):
    print(msg)
    LOG_STORE.append(msg)
    if len(LOG_STORE) > 100:
        LOG_STORE.pop(0)

# --- Константы ---
WB_TOKEN = "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjQxMjE3djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTc1MTkzOTcwNSwiaWQiOiIwMTk0M2JlNS1kNDIzLTc0OGQtOGM4NC01ZmMyMjA3ZDY1YzUiLCJpaWQiOjcxOTUyMDQzLCJvaWQiOjI3NjkwNywicyI6NzkzNCwic2lkIjoiZDMyZjgyMjQtNjY4Mi00ZmI2LWJkNWUtMDU3ZjA3NmE5NjllIiwidCI6ZmFsc2UsInVpZCI6NzE5NTIwNDN9.9piJOR1Z9w9kRx5KSZKJ5aN1yG4clHaCUF9oujD5buYQIZf_5c9tB6G7rb5UOL-ZoQGIAIWYFUM9rhhAmG-enA"
WB_URL = "https://content-api.wildberries.ru/content/v2/get/cards/list"
WB_HEAD = {"Authorization": WB_TOKEN, "Content-Type": "application/json"}

GROQ_API_KEY = "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"
CLIENT_ID = "341544"
API_KEY = "bd9477e7-0475-4f1e-a4bb-2c25f4861781"
OZ_HEAD = {"Client-Id": CLIENT_ID, "Api-Key": API_KEY, "Content-Type": "application/json"}
BASE_URL = "https://api-seller.ozon.ru"
HEADERS = OZ_HEAD

FIXED_PRICE, CURRENCY_CODE = "500", "RUB"
POLL_ATTEMPTS = 18

# --- Загружаем vendorCode из Excel ---
def load_vendor_codes(xlsx="articuls.xlsx") -> set[str]:
    df = pd.read_excel(xlsx, dtype=str)
    for col in df.columns:
        if col.strip().lower() in ("артикулы", "артикул", "vendorcode"):
            codes = df[col].dropna().astype(str).str.strip()
            return set(codes)
    raise RuntimeError("❗ В Excel не найден столбец с артикулами")

# --- Получаем карточки с WB ---
def wb_get_all(limit=100):
    all_cards, cursor = [], {"updatedAt": None, "nmID": None}
    while True:
        body = {"settings":{"cursor":{"limit":limit, **cursor}, "filter":{"withPhoto":-1}}}
        r = requests.post(WB_URL, headers=WB_HEAD, json=body, timeout=15)
        r.raise_for_status()
        page = r.json().get("cards", [])
        all_cards.extend(page)
        if len(page) < limit: break
        last = page[-1]
        cursor = {"updatedAt": last["updatedAt"], "nmID": last["nmID"]}
    return all_cards

# --- Фильтруем по vendorCode и сохраняем локально ---
def dump_filtered(cards, vcodes:set):
    keep = [c for c in cards if str(c.get("vendorCode","")).strip() in vcodes]
    fname = f"wb_cards_{datetime.now():%Y-%m-%d}.json"
    pathlib.Path(fname).write_text(json.dumps(keep, ensure_ascii=False, indent=2), encoding="utf-8")
    return keep

# --- Категория книги через LLM ---
BOOK_TYPES = [(200001483, 971445093, "Печатная книга: Комикс")]  # Укорочено

_llm = Groq(api_key=GROQ_API_KEY)
_SYS = 'Отвечай JSON без комментариев вида {"description_category_id":…, "type_id":…}'

def choose_cat(title: str) -> tuple[int, int]:
    cats = "\n".join(f"{cid}:{tid} — {name}" for cid, tid, name in BOOK_TYPES)
    prompt = f"Название книги: {title}\n\nКатегории:\n{cats}"
    raw = _llm.chat.completions.create(
        model="gemma2-9b-it",
        messages=[{"role":"system","content":_SYS},
                  {"role":"user","content":prompt}],
        temperature=0,max_completion_tokens=100
    ).choices[0].message.content
    m = re.search(r"\{.*?\}", raw, re.S)
    if not m:
        raise RuntimeError("LLM вернул не-JSON:\n"+raw)
    data = json.loads(m.group(0))
    if ":" in str(data.get("description_category_id","")):
        cid, tid = data["description_category_id"].split(":")
        return int(cid), int(tid)
    return int(data["description_category_id"]), int(data["type_id"])

def get_attrs(desc:int, typ:int):
    body = {"description_category_id": desc, "type_id": typ, "language":"RU"}
    r = requests.post(BASE_URL+"/v1/description-category/attribute", headers=HEADERS, json=body, timeout=30)
    r.raise_for_status()
    return r.json()["result"]

def build_ozon_card(wb:dict, desc:int, typ:int, attrs:list[dict]) -> dict:
    return {"description_category_id": desc, "type_id": typ, "offer_id": wb.get("vendorCode","unknown"),
            "name": wb.get("title","Без названия"), "price": FIXED_PRICE, "currency_code": CURRENCY_CODE,
            "depth": 100, "width": 100, "height": 10, "dimension_unit": "mm", "weight": 250, "weight_unit": "g",
            "images": [], "attributes": []}  # Упрощено

def ozon_import_batch(cards:list[dict]):
    r = requests.post(BASE_URL+"/v3/product/import", headers=HEADERS, json={"items":cards}, timeout=30)
    r.raise_for_status()
    return str(r.json()["result"]["task_id"])

def ozon_poll(task_id:str):
    for i in range(1, POLL_ATTEMPTS+1):
        time.sleep(5)
        info = requests.post(BASE_URL+"/v1/product/import/info", headers=HEADERS, json={"task_id":task_id}, timeout=15).json()
        if info["result"].get("items"): return info
    return info

def run_transfer(xlsx_path: str, log=log_message):
    try:
        log("🚀 Запущен процесс переноса карточек")
        vcodes = load_vendor_codes(xlsx_path)
        log(f"Получено {len(vcodes)} артикулов")

        wb_all = wb_get_all()
        log(f"С Wildberries получено {len(wb_all)} карточек")

        wb_need = dump_filtered(wb_all, vcodes)
        log(f"Отфильтровано карточек: {len(wb_need)}")

        if not wb_need:
            log("❗ Ничего не найдено по этим vendorCode")
            return

        for idx in range(0, len(wb_need), 100):
            batch = wb_need[idx:idx+100]
            oz_cards = []
            for wb in batch:
                desc, typ = choose_cat(wb["title"])
                attrs = get_attrs(desc, typ)
                oz_cards.append(build_ozon_card(wb, desc, typ, attrs))

            log(f"► Отправляю партию {idx//100+1}: {len(oz_cards)} шт.")
            task = ozon_import_batch(oz_cards)
            result = ozon_poll(task)

            log_path = f"ozon_result_{task}.json"
            pathlib.Path(log_path).write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
            log(f"✔ Завершена партия, лог в {log_path}")

        log("🎉 Перенос завершён успешно.")
    except Exception as e:
        import traceback
        log_message("[ОШИБКА] run_transfer сломался:")
        log_message(traceback.format_exc())
