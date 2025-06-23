"""
Импорт карточек Wildberries → Ozon с «живым» логированием.
Экспортируемые имена:
    • run_transfer(xlsx_path) – запустить весь процесс
    • log_message(msg)        – добавить строку в лог
    • LOG_STORE               – кольцевой буфер последних 500 строк
"""

import json
import pathlib
import time
import re
import requests
from datetime import datetime
from rapidfuzz import process, fuzz
from groq import Groq
import pandas as pd
from typing import List, Dict, Tuple, Set

# ───────────────────── Логирование ──────────────────────────────
LOG_STORE: List[str] = []

def log_message(msg: str) -> None:
    """Пишет сообщение в stdout и в буфер LOG_STORE (≤500 строк)."""
    print(msg, flush=True)
    LOG_STORE.append(msg)
    if len(LOG_STORE) > 500:
        LOG_STORE.pop(0)

# ───────────────────── Константы окружения ──────────────────────
# !!! Замените токены/ключи на свои !!!
WB_TOKEN  = "YOUR_WB_JWT_TOKEN"
GROQ_API_KEY = "YOUR_GROQ_KEY"
CLIENT_ID    = "YOUR_OZON_CLIENT_ID"
API_KEY      = "YOUR_OZON_API_KEY"

WB_URL  = "https://content-api.wildberries.ru/content/v2/get/cards/list"
WB_HEAD = {"Authorization": WB_TOKEN, "Content-Type": "application/json"}

OZ_HEAD = {"Client-Id": CLIENT_ID, "Api-Key": API_KEY,
           "Content-Type": "application/json"}
BASE_URL = "https://api-seller.ozon.ru"
HEADERS  = OZ_HEAD  # алиас

FIXED_PRICE, CURRENCY_CODE = "500", "RUB"
POLL_ATTEMPTS = 18
BATCH_SIZE    = 100

# Папка для json-логов и выгрузок
LOG_DIR = pathlib.Path("logs_data")
LOG_DIR.mkdir(exist_ok=True)

# ───────────────────── 1. vendorCode из Excel ───────────────────
def load_vendor_codes(xlsx_path: str) -> Set[str]:
    """Читает Excel и возвращает множество артикулов."""
    try:
        df = pd.read_excel(xlsx_path, dtype=str)
        for col in df.columns:
            if col.strip().lower() in ("артикулы", "артикул", "vendorcode"):
                return set(df[col].dropna().astype(str).str.strip())
        log_message("❗ В Excel не найден столбец с артикулами")
    except Exception as e:
        log_message(f"[ОШИБКА] не удалось прочитать Excel: {e}")
    return set()

# ───────────────────── 2. тянем ВСЕ карточки WB ─────────────────
def wb_get_all(limit: int = 100) -> List[Dict]:
    all_cards, cursor = [], {"updatedAt": None, "nmID": None}
    while True:
        body = {"settings": {"cursor": {"limit": limit, **cursor},
                             "filter": {"withPhoto": -1}}}
        r = requests.post(WB_URL, headers=WB_HEAD, json=body, timeout=20)
        r.raise_for_status()
        page = r.json().get("cards", [])
        all_cards.extend(page)
        log_message(f"WB → +{len(page)} (итого {len(all_cards)})")
        if len(page) < limit:
            break
        last = page[-1]
        cursor = {"updatedAt": last["updatedAt"], "nmID": last["nmID"]}
    return all_cards

# ───────────────────── 3. фильтр + сохранение ───────────────────
def dump_filtered(cards: List[Dict], vcodes: Set[str]) -> List[Dict]:
    keep = [c for c in cards if str(c.get("vendorCode", "")).strip() in vcodes]
    fname = LOG_DIR / f"wb_cards_{datetime.now():%Y-%m-%d_%H-%M-%S}.json"
    fname.write_text(json.dumps(keep, ensure_ascii=False, indent=2), encoding="utf-8")
    log_message(f"✔ Сохранил {len(keep)} карточек в {fname.name}")
    return keep

# ───────────────────── 4. Категории книг Ozon ───────────────────
BOOK_TYPES = [
    (200001483, 971445093, "Печатная книга: Комикс"),
    (200001483, 971817987, "Печатная книга: Репринтное издание, подарочное издание под старину"),
    (200001483, 971817981, "Печатная книга: Приключения"),
    (200001483, 971445064, "Печатная книга: Религия"),
    (200001483, 971445068, "Печатная книга: Компьютерная литература"),
    (200001483, 971445095, "Печатная книга: Манхва"),
    (200001483, 971817989, "Печатная книга: Книга для чтения на иностранном языке"),
    (200001483, 971445096, "Печатная книга: Маньхуа"),
    (200001483, 971817986, "Печатная книга: Образовательная программа"),
    (200001483, 971445077, "Печатная книга: Пособие для подготовки к ЕГЭ"),
    (200001483, 971445082, "Печатная книга: Поэзия"),
    (200001483, 971445078, "Печатная книга: Пособие для школы"),
    (200001483, 971817983, "Печатная книга: Любовный роман"),
    (200001483, 971445079, "Печатная книга: Пособие для вузов, ссузов, аспирантуры"),
    (200001483, 971817979, "Печатная книга: Ужасы, триллер"),
    (200001483, 971445065, "Печатная книга: Красота, здоровье, спорт"),
    (200001483, 971817978, "Печатная книга: Пособие для подготовки к ОГЭ"),
    (200001483, 971817976, "Печатная книга: Медицина"),
    (200001483, 971445070, "Печатная книга: Публицистика, биография, мемуары"),
    (200001483, 971817980, "Печатная книга: Фольклор"),
    (200001483, 971445066, "Печатная книга: История, искусство, культура"),
    (200001483, 971817991, "Печатная книга: Развитие детей"),
    (200001483, 971445076, "Печатная книга: Пособие для изучения иностранных языков"),
    (200001483, 971817992, "Печатная книга: Художественная литература для детей"),
    (200001483, 971445074, "Печатная книга: Познавательная литература для детей"),
    (200001483, 971817990, "Печатная книга: Энциклопедия для детей"),
    (200001483, 971817982, "Печатная книга: Молодежная художественная литература"),
    (200001483, 971445083, "Печатная книга: Детектив"),
    (200001483, 971817984, "Печатная книга: Драматургия"),
    (200001483, 971445094, "Печатная книга: Манга"),
    (200001483, 971445069, "Печатная книга: Хобби"),
    (200001483, 971445081, "Печатная книга: Проза других жанров"),
    (200001483, 971445084, "Печатная книга: Фантастика"),
    (200001483, 971445075, "Печатная книга: Досуг и творчество детей"),
    (200001483, 971817977, "Печатная книга: Пособие для подготовки к итоговому тестированию и ВПР"),
    (200001483, 971817993, "Печатная книга: Комикс для детей"),
    (200001483, 971817974, "Печатная книга: Психология и саморазвитие"),
    (200001483, 971445072, "Печатная книга: Бизнес-литература"),
    (200001483, 971445080, "Печатная книга: Энциклопедия, справочник"),
    (200001483, 971445067, "Печатная книга: Научная и научно-популярная литература"),
    (200001483, 971817975, "Печатная книга: Эзотерика"),
    (200001483, 971445085, "Печатная книга: Фэнтези"),
    (200001483, 971445071, "Печатная книга: Юридическая, правовая литература"),
    (200001483, 971818440, "Печатная книга: Second-hand книга"),
    (200001483, 971818441, "Печатная книга: Антикварное издание"),
]

# ───────────────────── 5. LLM выбор категории/типа ──────────────
_llm = Groq(api_key=GROQ_API_KEY)
_SYS = 'Отвечай JSON без комментариев вида {"description_category_id":…, "type_id":…}'

def choose_cat(title: str) -> Tuple[int, int]:
    cats = "\n".join(f"{cid}:{tid} — {name}" for cid, tid, name in BOOK_TYPES)
    prompt = f"Название книги: {title}\n\nКатегории:\n{cats}"
    raw = _llm.chat.completions.create(
        model="gemma2-9b-it",
        messages=[{"role": "system", "content": _SYS},
                  {"role": "user",   "content": prompt}],
        temperature=0,
        max_completion_tokens=100
    ).choices[0].message.content
    m = re.search(r"\{.*?\}", raw, re.S)
    if not m:
        raise RuntimeError("LLM вернул не-JSON: " + raw)
    data = json.loads(m.group(0))
    if ":" in str(data.get("description_category_id", "")):
        cid, tid = data["description_category_id"].split(":")
        return int(cid), int(tid)
    return int(data["description_category_id"]), int(data["type_id"])

# ───────────────────── 6. Атрибуты категории и словари ──────────
def get_attrs(desc: int, typ: int) -> List[Dict]:
    body = {"description_category_id": desc, "type_id": typ, "language": "RU"}
    r = requests.post(BASE_URL + "/v1/description-category/attribute",
                      headers=HEADERS, json=body, timeout=30)
    r.raise_for_status()
    return r.json()["result"]

def dict_lookup(attr_id: int, desc: int, typ: int, raw: str):
    body = {"attribute_id": attr_id, "description_category_id": desc,
            "type_id": typ, "language": "RU",
            "last_value_id": 0, "limit": 2000}
    r = requests.post(BASE_URL + "/v1/description-category/attribute/values",
                      headers=HEADERS, json=body, timeout=30)
    r.raise_for_status()
    data = r.json()["result"]
    cand, score, *_ = process.extractOne(
        raw, [v["value"] for v in data], scorer=fuzz.token_sort_ratio)
    if score < 90:
        return None
    hit = next(v for v in data if v["value"] == cand)
    return hit["id"], hit["value"]

# ───────────────────── 7. Сопоставление ключей ──────────────────
RULES: Dict[str, List[str]] = {
    "isbn":                  ["isbn/issn", "isbn"],
    "автор на обложке":      ["автор"],
    "издательство":          ["издательство", "brand"],
    "язык издания":          ["языки", "язык"],
    "страна-изготовитель":   ["страна производства"],
    "количество страниц":    ["количество страниц"],
    "тип обложки":           ["обложка"],
    "возрастные ограничения": ["возрастные ограничения"],
    "серия":                 ["серия"],
    "ключевые слова":        ["жанры/тематика"],
}

# ───────────────────── 8. Сборка карточки Ozon ──────────────────
def build_ozon_card(wb: Dict, desc: int, typ: int,
                    attrs: List[Dict]) -> Dict:
    root  = {k.lower(): v for k, v in wb.items()}
    chars = {c["name"].lower():
             "; ".join(map(str, c["value"]))
             if isinstance(c["value"], list) else str(c["value"])
             for c in wb.get("characteristics", [])}
    dims  = wb.get("dimensions", {}) or {}

    def pick(name: str):
        ln = name.lower()
        for ok, keys in RULES.items():
            if ok in ln:
                for k in keys:
                    if chars.get(k): return chars[k]
                    if root.get(k):  return root[k]
        if ln in chars: return chars[ln]
        if ln in root:  return root[ln]
        if ln.startswith("издательство"): return wb.get("brand")
        if "размеры, мм" in ln and dims:
            return f"{dims.get('length',0)}x{dims.get('width',0)}x{dims.get('height',0)}"
        if "вес товара, г" in ln and dims:
            return str(int(round(float(dims.get("weightBrutto", .1))*1000)))
        return None

    oz, existing = [], set()

    # основные атрибуты
    for a in attrs:
        val = pick(a["name"])
        if not val:
            continue
        item = {"id": a["id"], "complex_id": 0, "values": []}
        if a["dictionary_id"]:
            hit = dict_lookup(a["id"], desc, typ, val)
            if hit:
                d_id, d_val = hit
                item["values"].append({"dictionary_value_id": d_id,
                                       "value": d_val})
            else:
                item["values"].append({"dictionary_value_id": 0,
                                       "value": str(val)})
        else:
            if a["type"].lower() in ("integer", "decimal"):
                try:
                    val = str(int(float(val)))
                except Exception:
                    continue
            item["values"].append({"value": str(val)})
        oz.append(item)
        existing.add(a["id"])

    # страховка критичных полей
    def ensure(aid: int, raw: str | None, dicted: bool = False):
        if not raw or aid in existing:
            return
        if dicted:
            hit = dict_lookup(aid, desc, typ, raw)
            if hit:
                d_id, d_val = hit
                oz.append({"id": aid, "complex_id": 0,
                           "values": [{"dictionary_value_id": d_id,
                                       "value": d_val}]})
                existing.add(aid)
                return
        oz.append({"id": aid, "complex_id": 0,
                   "values": [{"dictionary_value_id": 0,
                               "value": str(raw)}]})
        existing.add(aid)

    ensure(4184, chars.get("isbn/issn") or root.get("isbn"))   # ISBN
    ensure(4182, chars.get("автор")     or root.get("author")) # Автор
    ensure(7,    wb.get("brand"), True)                        # Издательство

    depth  = int(round(float(dims.get("length", 1)) * 10))
    width  = int(round(float(dims.get("width", 1)) * 10))
    height = int(round(float(dims.get("height", 1)) * 10))
    weight = int(round(float(dims.get("weightBrutto", .1)) * 1000))
    images = [p["big"] for p in wb.get("photos", []) if p.get("big")][:15]

    return {
        "description_category_id": desc,
        "type_id": typ,
        "offer_id": wb.get("vendorCode", "unknown"),
        "name": wb.get("title", "Без названия"),
        "price": FIXED_PRICE,
        "currency_code": CURRENCY_CODE,
        "depth": depth, "width": width, "height": height,
        "dimension_unit": "mm",
        "weight": weight, "weight_unit": "g",
        "images": images,
        "attributes": oz,
    }

# ───────────────────── 9. Импорт и polling ──────────────────────
def ozon_import_batch(cards: List[Dict]) -> str:
    r = requests.post(BASE_URL + "/v3/product/import",
                      headers=OZ_HEAD, json={"items": cards}, timeout=30)
    r.raise_for_status()
    return str(r.json()["result"]["task_id"])

def ozon_poll(task_id: str) -> Dict:
    for i in range(1, POLL_ATTEMPTS + 1):
        time.sleep(10)
        info = requests.post(BASE_URL + "/v1/product/import/info",
                             headers=OZ_HEAD,
                             json={"task_id": task_id},
                             timeout=15).json()
        status = info["result"].get("status")
        log_message(f"[{i}] {status}")
        if info["result"].get("items"):
            return info
    return info

# ───────────────────── 10. Точка входа ──────────────────────────
def run_transfer(xlsx_path: str) -> None:
    """
    Полный цикл импорта:
        1. Чтение Excel с артикулами.
        2. Скачивание всех карточек WB → фильтр.
        3. Формирование партий ≤100 и отправка в Ozon.
        4. Polling результата и сохранение логов в JSON.
    """
    log_message(f"=== Старт импорта: {xlsx_path} ===")

    vcodes = load_vendor_codes(xlsx_path)
    if not vcodes:
        log_message("⛔ Список vendorCode пуст – прекращаю работу")
        return

    wb_all  = wb_get_all()
    wb_need = dump_filtered(wb_all, vcodes)
    if not wb_need:
        log_message("⛔ Ничего не найдено по указанным vendorCode")
        return

    for idx in range(0, len(wb_need), BATCH_SIZE):
        batch = wb_need[idx: idx + BATCH_SIZE]
        oz_cards = []
        for wb in batch:
            try:
                desc, typ = choose_cat(wb["title"])
                attrs     = get_attrs(desc, typ)
                card      = build_ozon_card(wb, desc, typ, attrs)
                oz_cards.append(card)
            except Exception as e:
                log_message(f"[SKIP] {wb.get('vendorCode')}: {e}")

        if not oz_cards:
            continue

        log_message(f"► Отправляю партию {idx // BATCH_SIZE + 1}: "
                    f"{len(oz_cards)} шт.")
        try:
            task_id = ozon_import_batch(oz_cards)
            result  = ozon_poll(task_id)
            p = LOG_DIR / f\"ozon_result_{task_id}.json\"
            p.write_text(json.dumps(result, ensure_ascii=False, indent=2),
                         encoding=\"utf-8\")
            log_message(f\"✔ Партия завершена, лог {p.name}\")
        except Exception as e:
            log_message(f\"[ОШИБКА] импорт партии "
                        f"{idx // BATCH_SIZE + 1}: {e}\")\n")

    log_message(\"=== Импорт завершён ===\")

# ───────────────────── CLI запуск (необязательно) ───────────────
if __name__ == \"__main__\":
    import sys
    if len(sys.argv) != 2:
        print(\"Usage: python transfer.py articuls.xlsx\")
        sys.exit(1)
    run_transfer(sys.argv[1])
