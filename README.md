# student-babaev-hw01 — почасовой архив погоды Open-Meteo в Lakehouse

Открытая выгрузка Open-Meteo (реанализ ERA5) за 2024 год по 14 городам превращается в
таблицу Iceberg `lakehouse.babaev.weather`, которую одинаково читают Spark и Trino:
**источник → raw object → Parquet → Iceberg snapshot → SQL → вывод**.

Исследовательский вопрос: как различается суточный ход температуры (разница между самым
тёплым и самым холодным часом средних суток) между городами и федеральными округами и
как он меняется от зимы к лету. Ответ и все измерения — в [report.md](report.md).

## Состав сдачи

| Файл | Что внутри |
|---|---|
| `report.md` | паспорт источника, проверки, таблицы измерений, snapshots, федерация, архитектурный вывод |
| `schema.md` | поля, типы, смысл, nullable, партиционирование |
| `fetch_source.py` | запрос к API, оригиналы JSON и отдельный шаг JSON → CSV (только stdlib) |
| `upload_raw.py` | загрузка неизменённого raw в бакет `raw` MinIO |
| `pipeline.py` | raw → проверки качества → Parquet с измерениями → Iceberg в две записи |
| `queries.sql` | проверочные, аналитический и федеративный SQL для Trino |
| `docker-compose.override.yml` | поправка образов MinIO (см. «Если что-то не запустилось») |
| `make_chart.py`, `chart.svg` | необязательное дополнение: один график по результату SQL |
| `evidence/` | сохранённые выводы всех шагов |

Большие данные в сдачу не входят: ни исходный датасет, ни Docker volumes.

## Как повторить с чистого стенда

Нужны Docker и Python 3. Команды 2–8 выполняются **из каталога `infra/`** распакованного
комплекта `lecture_01_student/`. Пути `<путь-к-сдаче>` замените на свой.

```bash
# 1. Положить скрипты туда, где их видит контейнер Spark (infra/scripts -> /scripts)
mkdir -p infra/scripts/hw01
cp <путь-к-сдаче>/fetch_source.py <путь-к-сдаче>/upload_raw.py <путь-к-сдаче>/pipeline.py infra/scripts/hw01/

cd infra

# 2. Поднять стенд
docker compose build
docker compose up -d
docker compose ps --all                                  # minio-init должен быть Exited (0)
docker compose exec -T trino trino --execute "SELECT 1"

# 3. Скачать открытый источник и собрать табличный CSV (на хосте, ~1 минута, нужен интернет)
python3 scripts/hw01/fetch_source.py
#    -> data/hw01/raw_json/<city>.json, data/hw01/source.csv, data/hw01/source_manifest.json

# 4. Положить неизменённый raw в MinIO
docker compose exec -T spark python3 /scripts/hw01/upload_raw.py

# 5. Основной конвейер: проверки, Parquet с измерениями, Iceberg в две записи
docker compose exec -T spark spark-submit /scripts/hw01/pipeline.py

# 6. SQL в Trino: проверки, история snapshots, аналитика, федеративный JOIN
docker compose exec -T trino trino --output-format=ALIGNED < <путь-к-сдаче>/queries.sql
```

Необязательный шаг 7 — перерисовать график (команда выгрузки данных из Trino указана
в docstring скрипта):

```bash
cp <путь-к-сдаче>/make_chart.py scripts/hw01/
python3 scripts/hw01/make_chart.py <путь-к-сдаче>/evidence/chart_data.csv <путь-к-сдаче>/chart.svg
```

В DataGrip вместо шага 6 откройте `queries.sql`, назначьте файлу подключение Trino
(`jdbc:trino://localhost:8088/lakehouse/babaev`, пользователь любой, пароль не нужен)
и выполняйте блоки по порядку. Выбор клиента на результат не влияет.

Остановка без потери данных: `docker compose down` (без `-v`).

## Что должно получиться

| Шаг | Ожидаемый результат |
|---|---|
| `fetch_source.py` | 14 городов × 8 784 часа = 122 976 строк, `source.csv` ≈ 13.9 МиБ |
| `upload_raw.py` | 16 объектов под `s3://raw/babaev/weather/ingestion_date=<дата>/` |
| `pipeline.py` | raw = accepted + rejected: 122 976 = 122 976 + 0; Parquet 1.68 МиБ против CSV 14.05 МиБ; порции Iceberg 61 152 и 61 824 |
| `queries.sql` | 2 snapshot (`overwrite` и `append`), сверка со Spark: 122 976 строк и `mean_temperature_c = 5.642556`, федеративный JOIN с 17 568 несопоставленными наблюдениями |

Точные размеры и время зависят от машины; числа строк и агрегаты — нет.
Даты в `ingestion_date` и `committed_at` у вас будут свои.

Повторный запуск безопасен: `pipeline.py` перезаписывает только свои пути
`babaev/weather/*` и пересоздаёт свою таблицу, учебные пути `events` из семинара
он не трогает. В истории snapshots после нескольких прогонов может быть больше двух
записей — значимы две последние.

## Если что-то не запустилось

**`pull access denied for minio/minio`.** Docker Hub на момент выполнения ДЗ не отдаёт
образы MinIO. Скопируйте `docker-compose.override.yml` из сдачи в `infra/` и повторите
шаг 2 — Compose подхватит его автоматически и возьмёт те же версии с `quay.io`.
Исходный `docker-compose.yml` при этом не меняется.

**Trino отвечает не сразу.** Посмотрите `docker compose logs --tail=60 trino` и повторите
`SELECT 1` после готовности.

**`В s3://raw/babaev/weather/ нет ingestion_date=*`.** Не выполнен шаг 4;
`pipeline.py` сам находит самый свежий заезд, но нужен хотя бы один.
Конкретный заезд можно выбрать явно: `spark-submit /scripts/hw01/pipeline.py --ingestion-date 2026-09-23`.

**Справочник `memory.default.city_reference` не найден.** Каталог `memory` непостоянный:
после перезапуска Trino выполните блок 5 из `queries.sql`.

## Среда, на которой получены результаты в `evidence/`

macOS 26.4, arm64, 36 ГиБ RAM; Docker 29.5.2 в colima с лимитом 6 CPU и 12 ГиБ;
Spark 3.5.9 в режиме `local[*]` с драйвером 2 ГиБ, Iceberg 1.11.0, Trino 483.
Порты MinIO, PostgreSQL и Trino привязаны к `127.0.0.1`.
