# ДЗ 1. Open-Meteo → Iceberg

Почасовая погода за 2024 год, 14 городов, 122 976 строк. Таблица — `lakehouse.babaev.weather`.

Вопрос: как отличается суточный ход температуры (разница между самым тёплым и самым холодным часом) между городами и округами зимой и летом. Ответ и цифры — в `report.md`.

## Запуск

Docker, Python 3 и стенд из `lecture_01_student`. Команды — из `infra/`.

Если Docker Hub не отдаёт `minio/minio`, положите `docker-compose.override.yml` рядом с `docker-compose.yml` — тогда образы возьмутся с quay.io.

```bash
mkdir -p infra/scripts/hw01
cp fetch_source.py upload_raw.py pipeline.py infra/scripts/hw01/

cd infra
docker compose build && docker compose up -d
docker compose exec -T trino trino --execute "SELECT 1"

python3 scripts/hw01/fetch_source.py
docker compose exec -T spark python3 /scripts/hw01/upload_raw.py
docker compose exec -T spark spark-submit /scripts/hw01/pipeline.py
docker compose exec -T trino trino --output-format=ALIGNED < ../student-babaev-hw01/queries.sql
```

После прогона в таблице 122 976 строк, среднее температуры `5.642556`. SQL тот же в DataGrip: `jdbc:trino://localhost:8088/lakehouse/babaev`.

## Что здесь

- `pipeline.py` — схема, проверки, parquet, две записи в Iceberg
- `queries.sql` — проверки, аналитика и JOIN со справочником в `memory`
- `schema.md` — поля
- `evidence/` — вывод команд

Сам датасет не прилагаю, его качает `fetch_source.py`.

## Если что-то не запустилось

`pull access denied for minio/minio` — Docker Hub сейчас не отдаёт эти образы. Файл `docker-compose.override.yml` из этой папки положите в `infra/` и снова `docker compose up -d`. Сам `docker-compose.yml` из комплекта не меняйте.

Trino не отвечает сразу после старта — подождите и повторите `SELECT 1`. Логи: `docker compose logs --tail=60 trino`.

`В s3://raw/babaev/weather/ нет ingestion_date=*` — не отработал `upload_raw.py`.

После перезапуска Trino пропадает справочник в `memory`. Его создаёт блок 5 в `queries.sql`.

## Среда, на которой получены результаты в evidence/

macOS 26.4, arm64, 36 ГиБ RAM. Docker 29.5.2 в colima, 6 CPU и 12 ГиБ. Spark 3.5.9 `local[*]`, Iceberg 1.11.0, Trino 483. Порты только на `127.0.0.1`.
