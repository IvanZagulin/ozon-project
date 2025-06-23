import pathlib
import json
import time
import pandas as pd
import requests
from datetime import datetime
from rapidfuzz import process, fuzz
from groq import Groq

LOG_STORE = []

def log_message(msg):
    print(msg)
    LOG_STORE.append(msg)
    if len(LOG_STORE) > 100:
        LOG_STORE.pop(0)

# Константы
WB_TOKEN  = "..."  # ваш токен
WB_URL    = "https://content-api.wildberries.ru/content/v2/get/cards/list"
WB_HEAD   = {"Authorization": WB_TOKEN, "Content-Type": "application/json"}

GROQ_API_KEY = "..."  # ваш API ключ
CLIENT_ID    = "..."  # ваш клиент ID Ozon
API_KEY      = "..."  # ваш API ключ Ozon
OZ_HEAD      = {"Client-Id": CLIENT_ID, "Api-Key": API_KEY, "Content-Type": "application/json"}
BASE_URL = "https://api-seller.ozon.ru"

FIXED_PRICE, CURRENCY_CODE = "500", "RUB"
POLL_ATTEMPTS = 18

def load_vendor_codes(xlsx="articuls.xlsx") -> set[str]:
    try:
        df = pd.read_excel(xlsx, dtype=str)
        for col in df.columns:
            if col.strip().lower() in ("артикулы", "артикул", "vendorcode"):
                codes = df[col].dropna().astype(str).str.strip()
                return set(codes)
        log_message("❗ В Excel не нашёлся столбец «артикулы»")
        return set()
    except Exception as e:
        log_message(f"[ОШИБКА] не удалось загрузить артикула: {e}")
        return set()

def wb_get_all(limit=100):
    all_cards, cursor = [], {"updatedAt": None, "nmID": None}
    while True:
        body = {"settings":{"cursor":{"limit":limit, **cursor},"filter":{"withPhoto":-1}}}
        r = requests.post(WB_URL, headers=WB_HEAD, json=body, timeout=15)
        r.raise_for_status()
        page = r.json().get("cards", [])
        all_cards.extend(page)
        log_message(f"WB → +{len(page)} (итого {len(all_cards)})")
        if len(page) < limit:
            break
        last = page[-1]
        cursor = {"updatedAt": last["updatedAt"], "nmID": last["nmID"]}
    return all_cards

def dump_filtered(cards, vcodes:set):
    keep = [c for c in cards if str(c.get("vendorCode","")).strip() in vcodes]
    fname = f"wb_cards_{datetime.now():%Y-%m-%d}.json"
    pathlib.Path(fname).write_text(json.dumps(keep, ensure_ascii=False, indent=2), encoding="utf-8")
    log_message(f"✔ Сохранил {len(keep)} карточек в {fname}")
    return keep

def run_transfer(xlsx_path: str, log=log_message):
    try:
        log("🚀 Запущен процесс переноса карточек")
        log(f"Загружаю артикула из {xlsx_path}")
        vcodes = load_vendor_codes(xlsx_path)
        log(f"Получено {len(vcodes)} артикулов")

        wb_all = wb_get_all()
        log(f"С Wildberries получено {len(wb_all)} карточек")

        wb_need = dump_filtered(wb_all, vcodes)
        log(f"Отфильтровано карточек: {len(wb_need)}")

        if not wb_need:
            log("❗ Ничего не найдено по этим vendorCode")
            return

        for idx, wb in enumerate(wb_need):
            log(f"✔ Обработка карточки {wb.get('vendorCode')} — {wb.get('title')}")
            time.sleep(1)

        log("🎉 Перенос завершён успешно.")
    except Exception as e:
        import traceback
        log_message("[ОШИБКА] run_transfer сломался:")
        log_message(traceback.format_exc())
