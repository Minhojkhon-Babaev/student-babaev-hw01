"""Шаг 0 своего источника: получение открытой выгрузки Open-Meteo и подготовка табличного CSV.

Запуск на хосте из каталога infra/ (нужен только Python 3, внешние пакеты не требуются):
    python3 scripts/hw01/fetch_source.py

Скрипт делает два независимых шага:
1. Скачивает почасовой архив ERA5 по каждому городу и кладёт ответ API как есть
   в data/hw01/raw_json/<city_id>.json. Эти файлы дальше не редактируются.
2. Отдельным воспроизводимым шагом разворачивает columnar-JSON в табличный
   data/hw01/source.csv. Порядок строк детерминирован (city_id, observed_at).

Рядом пишется data/hw01/source_manifest.json: URL, размер и sha256 каждого ответа,
чтобы выгрузку можно было получить заново и сверить.
"""

import argparse
import csv
import hashlib
import json
import pathlib
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timezone

API_URL = "https://archive-api.open-meteo.com/v1/archive"
START_DATE = "2024-01-01"
END_DATE = "2024-12-31"

# Почасовые переменные ERA5. Порядок важен: он же задаёт порядок колонок CSV.
HOURLY_VARS = [
    "temperature_2m",
    "apparent_temperature",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "surface_pressure",
    "cloud_cover",
]

CSV_COLUMNS = [
    "city_id",
    "city_name",
    "country_code",
    "latitude",
    "longitude",
    "elevation_m",
    "observed_at",
    "observed_date",
    "temperature_2m_c",
    "apparent_temperature_c",
    "relative_humidity_2m_pct",
    "precipitation_mm",
    "wind_speed_10m_kmh",
    "surface_pressure_hpa",
    "cloud_cover_pct",
]

# Точки запроса: широта/долгота центра города. Ответ API содержит координаты
# фактической ячейки сетки ERA5 — в CSV сохраняются именно они.
CITIES = [
    ("moscow", "Москва", "RU", 55.7558, 37.6173),
    ("saint_petersburg", "Санкт-Петербург", "RU", 59.9311, 30.3609),
    ("nizhny_novgorod", "Нижний Новгород", "RU", 56.3269, 44.0059),
    ("kazan", "Казань", "RU", 55.7963, 49.1088),
    ("rostov_on_don", "Ростов-на-Дону", "RU", 47.2357, 39.7015),
    ("sochi", "Сочи", "RU", 43.5855, 39.7231),
    ("murmansk", "Мурманск", "RU", 68.9585, 33.0827),
    ("yekaterinburg", "Екатеринбург", "RU", 56.8389, 60.6057),
    ("novosibirsk", "Новосибирск", "RU", 55.0084, 82.9357),
    ("krasnoyarsk", "Красноярск", "RU", 56.0153, 92.8932),
    ("yakutsk", "Якутск", "RU", 62.0355, 129.6755),
    ("vladivostok", "Владивосток", "RU", 43.1155, 131.8855),
    ("astana", "Астана", "KZ", 51.1694, 71.4491),
    ("minsk", "Минск", "BY", 53.9045, 27.5615),
]


def build_url(latitude, longitude):
    query = urllib.parse.urlencode({
        "latitude": latitude,
        "longitude": longitude,
        "start_date": START_DATE,
        "end_date": END_DATE,
        "hourly": ",".join(HOURLY_VARS),
        "timezone": "UTC",
    })
    return f"{API_URL}?{query}"


def download(url, attempts=4):
    """GET с повторами: публичный API может ответить 429 при частых запросах."""
    last_error = None
    for attempt in range(1, attempts + 1):
        try:
            with urllib.request.urlopen(url, timeout=120) as response:
                return response.read()
        except (urllib.error.URLError, TimeoutError) as error:
            last_error = error
            print(f"  попытка {attempt} не удалась: {error}")
            time.sleep(5 * attempt)
    raise SystemExit(f"Не удалось скачать {url}: {last_error}")


def fetch_raw(out_dir, refresh):
    raw_dir = out_dir / "raw_json"
    raw_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "source": "Open-Meteo Historical Weather API (ERA5 reanalysis)",
        "api_url": API_URL,
        "license": "CC BY 4.0, https://open-meteo.com/en/license",
        "start_date": START_DATE,
        "end_date": END_DATE,
        "hourly_variables": HOURLY_VARS,
        "downloaded_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "files": [],
    }
    for city_id, city_name, country, lat, lon in CITIES:
        target = raw_dir / f"{city_id}.json"
        url = build_url(lat, lon)
        if target.exists() and not refresh:
            print(f"{city_id}: файл уже есть, пропускаем скачивание")
        else:
            print(f"{city_id}: GET {url}")
            target.write_bytes(download(url))
        payload = target.read_bytes()
        manifest["files"].append({
            "city_id": city_id,
            "city_name": city_name,
            "country_code": country,
            "requested_latitude": lat,
            "requested_longitude": lon,
            "url": url,
            "path": f"raw_json/{city_id}.json",
            "bytes": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
        })
    manifest_path = out_dir / "source_manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nМанифест: {manifest_path}")
    return raw_dir


def format_number(value):
    """None -> пустая ячейка; остальное пишем без изменения значения."""
    if value is None:
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def json_to_csv(out_dir, raw_dir):
    """Второй воспроизводимый шаг: columnar JSON -> длинный CSV без правок значений."""
    csv_path = out_dir / "source.csv"
    written = 0
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(CSV_COLUMNS)
        for city_id, city_name, country, _, _ in CITIES:
            payload = json.loads((raw_dir / f"{city_id}.json").read_text(encoding="utf-8"))
            hourly = payload["hourly"]
            times = hourly["time"]
            columns = [hourly[name] for name in HOURLY_VARS]
            for index, moment in enumerate(times):
                observed_at = datetime.strptime(moment, "%Y-%m-%dT%H:%M")
                writer.writerow([
                    city_id,
                    city_name,
                    country,
                    payload["latitude"],
                    payload["longitude"],
                    payload["elevation"],
                    observed_at.strftime("%Y-%m-%d %H:%M:%S"),
                    observed_at.strftime("%Y-%m-%d"),
                    *[format_number(column[index]) for column in columns],
                ])
                written += 1
            print(f"{city_id}: {len(times):,} часовых наблюдений")
    print(f"\nCSV: {csv_path}; строк данных {written:,}; "
          f"размер {csv_path.stat().st_size / 1024 / 1024:.2f} МиБ")
    expected = (date.fromisoformat(END_DATE) - date.fromisoformat(START_DATE)).days + 1
    expected *= 24 * len(CITIES)
    if written != expected:
        print(f"ВНИМАНИЕ: ожидали {expected:,} строк, получили {written:,}. "
              "Проверьте период и список городов перед запуском pipeline.py")
    return csv_path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", default="data/hw01",
                        help="каталог внутри infra/, виден контейнеру Spark как /data/hw01")
    parser.add_argument("--refresh", action="store_true",
                        help="перекачать JSON, даже если файлы уже есть")
    args = parser.parse_args()

    out_dir = pathlib.Path(args.out_dir)
    if not out_dir.is_absolute() and not pathlib.Path("docker-compose.yml").exists():
        sys.exit("Запускайте из каталога infra/: относительный путь data/hw01 "
                 "должен попасть в том, смонтированный в контейнер Spark")
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"== Шаг 1: {len(CITIES)} запросов к Open-Meteo, период {START_DATE}..{END_DATE} ==")
    raw_dir = fetch_raw(out_dir, args.refresh)
    print("\n== Шаг 2: JSON -> табличный CSV ==")
    json_to_csv(out_dir, raw_dir)


if __name__ == "__main__":
    main()
