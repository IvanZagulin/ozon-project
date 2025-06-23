
LOG_STORE = []

def log_message(msg):
    print(msg)
    LOG_STORE.append(msg)
    if len(LOG_STORE) > 100:
        LOG_STORE.pop(0)


import pathlib
import json
from datetime import datetime
import pandas as pd
import requests
import time

def load_vendor_codes(xlsx="articuls.xlsx") -> set[str]:
    df = pd.read_excel(xlsx, dtype=str)
    for col in df.columns:
        if col.strip().lower() in ("артикулы", "артикул", "vendorcode"):
            codes = df[col].dropna().astype(str).str.strip()
            return set(codes)
    raise RuntimeError("В Excel не найден столбец с артикулами")

def wb_get_all(limit=100):
    # упрощённая заглушка
    return [{"vendorCode": "123", "title": "Тестовая книга", "photos": [], "characteristics": [], "dimensions": {}}]

def dump_filtered(cards, vcodes: set):
    return [c for c in cards if c.get("vendorCode") in vcodes]

def run_transfer(xlsx_path: str, log=print):
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
