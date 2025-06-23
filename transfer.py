import json
import pathlib
import time
import re
import requests
from datetime import datetime
from rapidfuzz import process, fuzz
from groq import Groq
import pandas as pd

# Constants
WB_TOKEN = "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjQxMjE3djEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTc1MTkzOTcwNSwiaWQiOiIwMTk0M2JlNS1kNDIzLTc0OGQtOGM4NC01ZmMyMjA3ZDY1YzUiLCJpaWQiOjcxOTUyMDQzLCJvaWQiOjI3NjkwNywicyI6NzkzNCwic2lkIjoiZDMyZjgyMjQtNjY4Mi00ZmI2LWJkNWUtMDU3ZjA3NmE5NjllIiwidCI6ZmFsc2UsInVpZCI6NzE5NTIwNDN9.9piJOR1Z9w9kRx5KSZKJ5aN1yG4clHaCUF9oujD5buYQIZf_5c9tB6G7rb5UOL-ZoQGIAIWYFUM9rhhAmG-enA"
WB_URL = "https://content-api.wildberries.ru/content/v2/get/cards/list"
WB_HEAD = {"Authorization": WB_TOKEN, "Content-Type": "application/json"}

GROQ_API_KEY = "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"
CLIENT_ID = "341544"
API_KEY = "bd9477e7-0475-4f1e-a4bb-2c25f4861781"
OZ_HEAD = {"Client-Id": CLIENT_ID, "Api-Key": API_KEY, "Content-Type": "application/json"}
BASE_URL = "https://api-seller.ozon.ru"
HEADERS = {"Client-Id": CLIENT_ID, "Api-Key": API_KEY, "Content-Type": "application/json"}

FIXED_PRICE = "500"
CURRENCY_CODE = "RUB"
POLL_ATTEMPTS = 18

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

_llm = Groq(api_key=GROQ_API_KEY)
_SYS = 'Отвечай JSON без комментариев вида {"description_category_id":…, "type_id":…}'

RULES = {
    "isbn": ["isbn/issn", "isbn"],
    "автор на обложке": ["автор"],
    "издательство": ["издательство", "brand"],
    "язык издания": ["языки", "язык"],
    "страна-изготовитель": ["страна производства"],
    "количество страниц": ["количество страниц"],
    "тип обложки": ["обложка"],
    "возрастные ограничения": ["возрастные ограничения"],
    "серия": ["серия"],
    "ключевые слова": ["жанры/тематика"],
}

def load_vendor_codes(xlsx="articuls.xlsx") -> set[str]:
    df = pd.read_excel(xlsx, dtype=str)
    for col in df.columns:
        if col.strip().lower() in ("артикулы", "артикул", "vendorcode"):
            codes = df[col].dropna().astype(str).str.strip()
            return set(codes)
    raise RuntimeError("В Excel не найден столбец с артикулами")


def wb_get_all(limit=100):
    all_cards, cursor = [], {"updatedAt": None, "nmID": None}
    while True:
        body = {"settings": {"cursor": {"limit": limit, **cursor}, "filter": {"withPhoto": -1}}}
        r = requests.post(WB_URL, headers=WB_HEAD, json=body, timeout=15)
        r.raise_for_status()
        page = r.json().get("cards", [])
        all_cards.extend(page)
        if len(page) < limit:
            break
        last = page[-1]
        cursor = {"updatedAt": last["updatedAt"], "nmID": last["nmID"]}
    return all_cards


def dump_filtered(cards, vcodes: set):
    keep = [c for c in cards if str(c.get("vendorCode", "")).strip() in vcodes]
    fname = f"wb_cards_{datetime.now():%Y-%m-%d}.json"
    pathlib.Path(fname).write_text(json.dumps(keep, ensure_ascii=False, indent=2), encoding="utf-8")
    return keep


def choose_cat(title: str) -> tuple[int, int]:
    cats = "\n".join(f"{cid}:{tid} — {name}" for cid, tid, name in BOOK_TYPES)
    prompt = f"Название книги: {title}\n\nКатегории:\n{cats}"
    raw = _llm.chat.completions.create(
        model="gemma2-9b-it",
        messages=[{"role": "system", "content": _SYS}, {"role": "user", "content": prompt}],
        temperature=0,
        max_completion_tokens=100,
    ).choices[0].message.content
    m = re.search(r"\{.*?\}", raw, re.S)
    if not m:
        raise RuntimeError("LLM вернул не-JSON:\n" + raw)
    data = json.loads(m.group(0))
    if ":" in str(data.get("description_category_id", "")):
        cid, tid = data["description_category_id"].split(":")
        return int(cid), int(tid)
    return int(data["description_category_id"]), int(data["type_id"])


def get_attrs(desc: int, typ: int):
    body = {"description_category_id": desc, "type_id": typ, "language": "RU"}
    r = requests.post(BASE_URL + "/v1/description-category/attribute", headers=HEADERS, json=body, timeout=30)
    r.raise_for_status()
    return r.json()["result"]


def dict_lookup(attr_id: int, desc: int, typ: int, raw: str):
    body = {
        "attribute_id": attr_id,
        "description_category_id": desc,
        "type_id": typ,
        "language": "RU",
        "last_value_id": 0,
        "limit": 2000,
    }
    r = requests.post(BASE_URL + "/v1/description-category/attribute/values", headers=HEADERS, json=body, timeout=30).json()
    cand, score, *_ = process.extractOne(raw, [v["value"] for v in r["result"]], scorer=fuzz.token_sort_ratio)
    if score < 90:
        return None
    hit = next(v for v in r["result"] if v["value"] == cand)
    return hit["id"], hit["value"]


def build_ozon_card(wb: dict, desc: int, typ: int, attrs: list[dict]) -> dict:
    root = {k.lower(): v for k, v in wb.items()}
    chars = {
        c["name"].lower(): "; ".join(map(str, c["value"])) if isinstance(c["value"], list) else str(c["value"])
        for c in wb.get("characteristics", [])
    }
    dims = wb.get("dimensions", {}) or {}

    def pick(name: str):
        ln = name.lower()
        for ok, keys in RULES.items():
            if ok in ln:
                for k in keys:
                    if chars.get(k):
                        return chars[k]
                    if root.get(k):
                        return root[k]
        if ln in chars:
            return chars[ln]
        if ln in root:
            return root[ln]
        if ln.startswith("издательство"):
            return wb.get("brand")
        if "размеры, мм" in ln and dims:
            return f"{dims.get('length', 0)}x{dims.get('width', 0)}x{dims.get('height', 0)}"
        if "вес товара, г" in ln and dims:
            return str(int(round(float(dims.get("weightBrutto", 0.1)) * 1000)))
        return None

    oz, existing = [], set()

    for a in attrs:
        val = pick(a["name"])
        if not val:
            continue
        item = {"id": a["id"], "complex_id": 0, "values": []}
        if a["dictionary_id"]:
            hit = dict_lookup(a["id"], desc, typ, val)
            if hit:
                d_id, d_val = hit
                item["values"].append({"dictionary_value_id": d_id, "value": d_val})
            else:
                item["values"].append({"dictionary_value_id": 0, "value": str(val)})
        else:
            if a["type"].lower() in ("integer", "decimal"):
                try:
                    val = str(int(float(val)))
                except Exception:
                    continue
            item["values"].append({"value": str(val)})
        oz.append(item)
        existing.add(a["id"])

    def ensure(aid: int, raw: str, dicted: bool = False):
        if not raw or aid in existing:
            return
        if dicted:
            hit = dict_lookup(aid, desc, typ, raw)
            if hit:
                d_id, d_val = hit
                oz.append(
                    {
                        "id": aid,
                        "complex_id": 0,
                        "values": [{"dictionary_value_id": d_id, "value": d_val}],
                    }
                )
                existing.add(aid)
                return
        oz.append({"id": aid, "complex_id": 0, "values": [{"dictionary_value_id": 0, "value": str(raw)}]})
        existing.add(aid)

    ensure(4184, chars.get("isbn/issn") or root.get("isbn"))
    ensure(4182, chars.get("автор") or root.get("author"))
    ensure(7, root.get("brand"), True)

    depth = int(round(float(dims.get("length", 1)) * 10))
    width = int(round(float(dims.get("width", 1)) * 10))
    height = int(round(float(dims.get("height", 1)) * 10))
    weight = int(round(float(dims.get("weightBrutto", 0.1)) * 1000))
    images = [p["big"] for p in wb.get("photos", []) if p.get("big")][:15]

    return {
        "description_category_id": desc,
        "type_id": typ,
        "offer_id": wb.get("vendorCode", "unknown"),
        "name": wb.get("title", "Без названия"),
        "price": FIXED_PRICE,
        "currency_code": CURRENCY_CODE,
        "depth": depth,
        "width": width,
        "height": height,
        "dimension_unit": "mm",
        "weight": weight,
        "weight_unit": "g",
        "images": images,
        "attributes": oz,
    }


def ozon_import_batch(cards: list[dict]):
    r = requests.post(BASE_URL + "/v3/product/import", headers=OZ_HEAD, json={"items": cards}, timeout=30)
    r.raise_for_status()
    return str(r.json()["result"]["task_id"])


def ozon_poll(task_id: str):
    for i in range(1, POLL_ATTEMPTS + 1):
        time.sleep(10)
        info = requests.post(BASE_URL + "/v1/product/import/info", headers=OZ_HEAD, json={"task_id": task_id}, timeout=15).json()
        status = info["result"].get("status")
        if info["result"].get("items"):
            return info
    return info


def run_transfer(xlsx_path: str, log=print):
    try:
    log(f"Загружаю артикула из {xlsx_path}")
    vcodes = load_vendor_codes(xlsx_path)
    log(f"Получено {len(vcodes)} артикулов")

    wb_all = wb_get_all()
    log(f"С Wildberries получено {len(wb_all)} карточек")
    wb_need = dump_filtered(wb_all, vcodes)
    log(f"Отфильтровано карточек: {len(wb_need)}")
    if not wb_need:
        log("Ничего не найдено по этим vendorCode")
        return

        pathlib.Path("logs_data").mkdir(exist_ok=True)
    for idx in range(0, len(wb_need), 100):
        batch = wb_need[idx : idx + 100]
        oz_cards = []
        for wb in batch:
            desc, typ = choose_cat(wb["title"])
            log(f"{wb.get('vendorCode')} → категория {desc}:{typ}")
            attrs = get_attrs(desc, typ)
            oz_cards.append(build_ozon_card(wb, desc, typ, attrs))

        log(f"Отправляю партию {idx//100 + 1}: {len(oz_cards)} шт.")
        task = ozon_import_batch(oz_cards)
        result = ozon_poll(task)
        fname = pathlib.Path("logs_data") / f"ozon_result_{task}.json"
        fname.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        log(f"✔ Завершена партия, лог в {fname}")

    except Exception as e:
        import traceback
        log('[ОШИБКА] run_transfer сломался:')
        log(traceback.format_exc())
