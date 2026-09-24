"""Шаг raw: перенос неизменённой выгрузки в бакет raw MinIO.

Запуск из infra/:
    docker compose exec -T spark python3 /scripts/hw01/upload_raw.py

Кладём три вещи под один ingestion_date:
  source_json/<city_id>.json  — оригинальные ответы API, байт в байт;
  source_manifest.json        — URL, размеры и sha256 оригиналов;
  source.csv                  — табличный файл, собранный шагом fetch_source.py.

Дата заезда берётся из манифеста, поэтому повторный запуск перезаписывает тот же
префикс, а не плодит новые. Скрипт ничего не правит в содержимом файлов.
"""

import hashlib
import json
import pathlib
import sys

import boto3

ENDPOINT = "http://minio:9000"
ACCESS_KEY = "admin"
SECRET_KEY = "hse2026minio"
BUCKET = "raw"
STUDENT = "babaev"
DATASET = "weather"
LOCAL_DIR = pathlib.Path("/data/hw01")


def sha256_of(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main():
    manifest_path = LOCAL_DIR / "source_manifest.json"
    csv_path = LOCAL_DIR / "source.csv"
    if not manifest_path.exists() or not csv_path.exists():
        sys.exit(f"Нет {manifest_path} или {csv_path}. Сначала выполните на хосте "
                 "python3 scripts/hw01/fetch_source.py из каталога infra/")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    ingestion_date = manifest["downloaded_at_utc"][:10]
    prefix = f"{STUDENT}/{DATASET}/ingestion_date={ingestion_date}"

    s3 = boto3.client("s3", endpoint_url=ENDPOINT,
                      aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    if BUCKET not in [bucket["Name"] for bucket in s3.list_buckets()["Buckets"]]:
        s3.create_bucket(Bucket=BUCKET)
        print(f"Создан бакет {BUCKET}")

    uploads = [(manifest_path, f"{prefix}/source_manifest.json"),
               (csv_path, f"{prefix}/source.csv")]
    for entry in manifest["files"]:
        uploads.append((LOCAL_DIR / entry["path"],
                        f"{prefix}/source_json/{pathlib.Path(entry['path']).name}"))

    print(f"== Загружаем {len(uploads)} объектов в s3://{BUCKET}/{prefix}/ ==")
    for local_path, key in uploads:
        s3.upload_file(str(local_path), BUCKET, key)

    print("\n== Листинг после загрузки ==")
    total = 0
    pages = s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix)
    for page in pages:
        for obj in page.get("Contents", []):
            total += obj["Size"]
            print(f"  s3://{BUCKET}/{obj['Key']}\t{obj['Size']:>12,} байт\t"
                  f"{obj['LastModified']:%Y-%m-%d %H:%M:%S%z}")
    print(f"\nВсего объектов под префиксом: {sum(1 for _ in uploads)}; "
          f"суммарный размер: {total / 1024 / 1024:.2f} МиБ")

    print("\n== Сверка: локальный sha256 = ETag загруженного объекта (одночастная загрузка) ==")
    for local_path, key in uploads[:3]:
        head = s3.head_object(Bucket=BUCKET, Key=key)
        print(f"  {key}: размер локально {local_path.stat().st_size:,}, "
              f"в S3 {head['ContentLength']:,}; sha256 локального файла {sha256_of(local_path)[:16]}…")
    print("Raw сохранён без ручных правок: файлы загружены как есть, "
          "дальнейшие преобразования происходят только в pipeline.py")


if __name__ == "__main__":
    main()
