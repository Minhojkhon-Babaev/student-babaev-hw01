"""ДЗ 1: raw -> проверки качества -> Parquet -> Iceberg для почасового архива погоды Open-Meteo.

Запуск из infra/ (raw уже должен лежать в MinIO, см. fetch_source.py и upload_raw.py):
    docker compose exec -T spark spark-submit /scripts/hw01/pipeline.py

Этапы:
  1. Чтение raw CSV с явной схемой (без inferSchema) и расчёт метрик качества.
  2. Разделение на accepted/rejected: отклонённые строки сохраняются целиком
     вместе с колонкой reject_reason, выполняется сверка raw = accepted + rejected.
  3. Запись принятых строк в двух форматах и измерение размера и времени
     одного и того же аналитического запроса.
  4. Две непересекающиеся записи в Iceberg-таблицу и вывод истории snapshots.

Исследовательский вопрос: как различается суточный ход температуры воздуха
(средняя температура по часам UTC и амплитуда день-ночь) между городами и
как он меняется между зимой и летом.
"""

import argparse
import time

import boto3
from pyspark.sql import SparkSession, functions as F, types as T

ENDPOINT = "http://minio:9000"
ACCESS_KEY = "admin"
SECRET_KEY = "hse2026minio"
RAW_BUCKET = "raw"
STUDENT = "babaev"
DATASET = "weather"
RAW_PREFIX = f"{STUDENT}/{DATASET}"

PARQUET_PATH = f"s3a://datalake/{STUDENT}/{DATASET}/parquet"
CSV_ACCEPTED_PATH = f"s3a://datalake/{STUDENT}/{DATASET}/csv_accepted"
REJECT_PATH = f"s3a://datalake/{STUDENT}/{DATASET}/rejects"
TABLE = f"lakehouse.{STUDENT}.{DATASET}"

PERIOD_START = "2024-01-01"
PERIOD_END = "2025-01-01"   # правая граница не включается
BATCH_BOUNDARY = "2024-07-01"
CORRUPT_COLUMN = "_corrupt_record"

# CSV не хранит типы: задаём их явно. inferSchema не используется нигде в решении.
WEATHER_SCHEMA = T.StructType([
    T.StructField("city_id", T.StringType()),
    T.StructField("city_name", T.StringType()),
    T.StructField("country_code", T.StringType()),
    T.StructField("latitude", T.DoubleType()),
    T.StructField("longitude", T.DoubleType()),
    T.StructField("elevation_m", T.DoubleType()),
    T.StructField("observed_at", T.TimestampType()),
    T.StructField("observed_date", T.DateType()),
    T.StructField("temperature_2m_c", T.DoubleType()),
    T.StructField("apparent_temperature_c", T.DoubleType()),
    T.StructField("relative_humidity_2m_pct", T.IntegerType()),
    T.StructField("precipitation_mm", T.DoubleType()),
    T.StructField("wind_speed_10m_kmh", T.DoubleType()),
    T.StructField("surface_pressure_hpa", T.DoubleType()),
    T.StructField("cloud_cover_pct", T.IntegerType()),
])

# Ключ наблюдения: один город в один час. Суррогатных идентификаторов не добавляем.
KEY_COLUMNS = ["city_id", "observed_at"]

# Физически возможные границы: правила задаются смыслом величины, а не статистикой.
# Отрицательная температура нормальна, отрицательные осадки — нет.
RANGE_RULES = [
    ("temperature_2m_c", -90.0, 60.0, "температура вне физически возможного диапазона"),
    ("apparent_temperature_c", -110.0, 70.0, "ощущаемая температура вне диапазона"),
    ("relative_humidity_2m_pct", 0, 100, "влажность вне 0..100 %"),
    ("precipitation_mm", 0.0, 500.0, "осадки за час вне 0..500 мм"),
    ("wind_speed_10m_kmh", 0.0, 500.0, "скорость ветра вне 0..500 км/ч"),
    ("surface_pressure_hpa", 300.0, 1100.0, "давление вне 300..1100 гПа"),
    ("cloud_cover_pct", 0, 100, "облачность вне 0..100 %"),
]

# Один и тот же запрос для CSV и Parquet: суточный профиль температуры летом.
MEASURE_QUERY = """
    SELECT city_id,
           hour(observed_at) AS hour_utc,
           round(avg(temperature_2m_c), 3) AS mean_temperature_c,
           count(*) AS observations
    FROM {view}
    WHERE observed_at >= TIMESTAMP '2024-06-01 00:00:00'
      AND observed_at <  TIMESTAMP '2024-09-01 00:00:00'
    GROUP BY city_id, hour(observed_at)
    ORDER BY city_id, hour_utc
"""


def latest_ingestion_date():
    """Берём самый свежий префикс ingestion_date= в бакете raw."""
    s3 = boto3.client("s3", endpoint_url=ENDPOINT,
                      aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    response = s3.list_objects_v2(Bucket=RAW_BUCKET, Prefix=f"{RAW_PREFIX}/", Delimiter="/")
    prefixes = [item["Prefix"] for item in response.get("CommonPrefixes", [])]
    dates = sorted(prefix.rstrip("/").split("ingestion_date=")[-1]
                   for prefix in prefixes if "ingestion_date=" in prefix)
    if not dates:
        raise SystemExit(f"В s3://{RAW_BUCKET}/{RAW_PREFIX}/ нет ingestion_date=*. "
                         "Сначала выполните upload_raw.py")
    return dates[-1]


def directory_size_mb(spark, path):
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI.create(path), conf)
    summary = fs.getContentSummary(jvm.org.apache.hadoop.fs.Path(path))
    return summary.getLength() / 1024 / 1024


def reject_reason_column():
    """Собираем человекочитаемую причину; concat_ws пропускает NULL от невыполненных when."""
    checks = [
        (F.col(CORRUPT_COLUMN).isNotNull(), "строка CSV не разобрана по схеме"),
        (F.col("observed_at").isNull(), "не разобрана метка времени observed_at"),
        (F.col("observed_date").isNull(), "не разобрана дата observed_date"),
        (F.col("city_id").isNull() | (F.trim(F.col("city_id")) == ""), "пустой city_id"),
        (F.col("temperature_2m_c").isNull(), "нет значения температуры"),
        (F.col("observed_at").isNotNull() & F.col("observed_date").isNotNull()
         & (F.col("observed_date") != F.to_date("observed_at")),
         "observed_date не совпадает с observed_at"),
        (F.col("observed_at").isNotNull()
         & ((F.col("observed_at") < F.lit(PERIOD_START).cast("timestamp"))
            | (F.col("observed_at") >= F.lit(PERIOD_END).cast("timestamp"))),
         "наблюдение вне заявленного периода 2024 года"),
    ]
    for column, low, high, message in RANGE_RULES:
        checks.append((F.col(column).isNotNull() & ~F.col(column).between(low, high), message))
    return F.concat_ws("; ", *[F.when(condition, F.lit(message)) for condition, message in checks])


def report_nulls(df, row_count):
    print("\n-- Доля NULL по колонкам исходного CSV --")
    aggregations = [F.sum(F.col(field.name).isNull().cast("long")).alias(field.name)
                    for field in WEATHER_SCHEMA.fields]
    nulls = df.agg(*aggregations).first().asDict()
    for name, count in nulls.items():
        share = count / row_count * 100 if row_count else 0.0
        print(f"  {name:<26} NULL: {count:>8,}  ({share:.4f} %)")
    key_nulls = nulls["temperature_2m_c"]
    print(f"Ключевое поле temperature_2m_c: доля NULL "
          f"{key_nulls / row_count * 100 if row_count else 0:.4f} %")
    return nulls


def validate(spark, raw_df):
    raw_count = raw_df.count()
    if raw_count == 0:
        raise ValueError("Raw CSV пуст: нечего обрабатывать")
    print(f"Строк в raw CSV: {raw_count:,}")

    missing = set(field.name for field in WEATHER_SCHEMA.fields) - set(raw_df.columns)
    if missing:
        raise ValueError(f"В raw нет обязательных колонок: {sorted(missing)}")
    print(f"Обязательные колонки на месте: {len(WEATHER_SCHEMA.fields)} полей")

    parsed_ts = raw_df.filter(F.col("observed_at").isNotNull()).count()
    print(f"Временной столбец observed_at разобран у {parsed_ts:,} строк "
          f"({raw_count - parsed_ts:,} не разобрано)")

    report_nulls(raw_df, raw_count)

    labelled = raw_df.withColumn("reject_reason", reject_reason_column()).cache()
    accepted = labelled.filter(F.col("reject_reason") == "").drop("reject_reason", CORRUPT_COLUMN)
    rejected = labelled.filter(F.col("reject_reason") != "")
    accepted_count, rejected_count = accepted.count(), rejected.count()

    print("\n-- Сверка raw = accepted + rejected --")
    print(f"  raw:      {raw_count:,}")
    print(f"  accepted: {accepted_count:,}")
    print(f"  rejected: {rejected_count:,}")
    if accepted_count + rejected_count != raw_count:
        raise ValueError("Строки потерялись: accepted + rejected != raw")
    print("  сходится")

    if rejected_count:
        print("\n-- Причины отклонения --")
        (rejected.groupBy("reject_reason").count()
         .orderBy(F.col("count").desc()).show(50, truncate=False))
    else:
        print("Отклонённых строк нет: проверка воспроизводима и вернула нулевой результат")

    duplicates = (accepted.groupBy(*KEY_COLUMNS).count()
                  .filter(F.col("count") > 1))
    duplicate_count = duplicates.count()
    print(f"\nДубли по ключу {KEY_COLUMNS}: {duplicate_count}")
    if duplicate_count:
        duplicates.show(20, truncate=False)
        raise ValueError("Ключ (city_id, observed_at) не уникален")

    print(f"\n-- Отклонённые строки -> {REJECT_PATH} (с колонкой reject_reason) --")
    (rejected.write.mode("overwrite").option("compression", "snappy").parquet(REJECT_PATH))
    # Схема задаётся явно: при нулевом числе отклонённых строк файлов нет и выводить схему не из чего.
    written_rejects = spark.read.schema(rejected.schema).parquet(REJECT_PATH).count()
    print(f"Записано отклонённых строк: {written_rejects:,}")
    if written_rejects != rejected_count:
        raise ValueError("Число отклонённых строк при записи изменилось")

    (accepted.groupBy("city_id", "city_name", "country_code")
     .agg(F.count("*").alias("observations"),
          F.min("observed_at").alias("first_observation"),
          F.max("observed_at").alias("last_observation"),
          F.round(F.min("temperature_2m_c"), 1).alias("min_temp_c"),
          F.round(F.max("temperature_2m_c"), 1).alias("max_temp_c"))
     .orderBy("city_id").show(50, truncate=False))
    return accepted, accepted_count


def measure(spark, accepted, accepted_count, raw_csv_path):
    print("\n== Запись принятых строк в двух форматах ==")
    print(f"CSV  -> {CSV_ACCEPTED_PATH} (без сжатия, с заголовком)")
    (accepted.write.mode("overwrite").option("header", True)
     .option("timestampFormat", "yyyy-MM-dd HH:mm:ss").csv(CSV_ACCEPTED_PATH))
    print(f"Parquet -> {PARQUET_PATH} (snappy, партиции по city_id)")
    (accepted.write.mode("overwrite").option("compression", "snappy")
     .partitionBy("city_id").parquet(PARQUET_PATH))

    csv_df = (spark.read.schema(WEATHER_SCHEMA).option("header", True)
              .option("mode", "FAILFAST")
              .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
              .csv(CSV_ACCEPTED_PATH))
    parquet_df = spark.read.parquet(PARQUET_PATH)

    csv_types = {field.name: field.dataType for field in csv_df.schema.fields}
    parquet_types = {field.name: field.dataType for field in parquet_df.schema.fields}
    if csv_types != parquet_types:
        raise ValueError(f"Типы разошлись: CSV {csv_types} против Parquet {parquet_types}")
    print(f"Имена и типы всех {len(csv_types)} полей совпадают в обоих форматах")

    csv_count, parquet_count = csv_df.count(), parquet_df.count()
    print(f"Строк: accepted {accepted_count:,}; CSV {csv_count:,}; Parquet {parquet_count:,}")
    if not accepted_count == csv_count == parquet_count:
        raise ValueError("Число строк изменилось при записи")

    raw_mb = directory_size_mb(spark, raw_csv_path)
    csv_mb = directory_size_mb(spark, CSV_ACCEPTED_PATH)
    parquet_mb = directory_size_mb(spark, PARQUET_PATH)
    print(f"\n-- Размеры --")
    print(f"  raw source.csv (как получен):      {raw_mb:.2f} МиБ")
    print(f"  CSV принятых строк:                {csv_mb:.2f} МиБ")
    print(f"  Parquet принятых строк (snappy):   {parquet_mb:.2f} МиБ")
    print(f"  Отношение CSV/Parquet:             {csv_mb / parquet_mb:.2f}")

    csv_df.createOrReplaceTempView("weather_csv")
    parquet_df.createOrReplaceTempView("weather_parquet")
    print("\n== Один и тот же аналитический запрос на обоих форматах ==")
    print(MEASURE_QUERY.format(view="<формат>"))
    print("Схема задана до таймера; замеряем выполнение запроса с материализацией "
          "результата, а не создание ленивого DataFrame.")
    print("Кэш Spark для этих таблиц не прогревался (.cache() не вызывался), "
          "но файлы уже прошли через страничный кэш ОС при записи — это не холодный старт.")

    reference = None
    timings = {"csv": [], "parquet": []}
    for trial in range(1, 4):
        # Чередуем порядок: он уменьшает, но не убирает влияние прогрева.
        formats = ("csv", "parquet") if trial % 2 else ("parquet", "csv")
        for fmt in formats:
            started = time.perf_counter()
            rows = spark.sql(MEASURE_QUERY.format(view=f"weather_{fmt}")).collect()
            elapsed = time.perf_counter() - started
            timings[fmt].append(elapsed)
            if reference is None:
                reference = rows
            elif rows != reference:
                raise ValueError(f"Результат запроса различается для формата {fmt}")
            label = "первый замер" if trial == 1 else f"повтор {trial}"
            print(f"  {fmt.upper():<8} {label:<12} {elapsed:6.3f} с; строк в ответе: {len(rows)}")
    print("Результаты запроса совпали во всех замерах для обоих форматов.")
    for fmt, values in timings.items():
        print(f"  {fmt.upper():<8} первый {values[0]:.3f} с; повторы "
              f"{', '.join(f'{value:.3f}' for value in values[1:])} с")
    print("Время зависит от прогрева, числа файлов и ресурсов ноутбука; "
          "ускорение не гарантировано и не требуется по условию.")
    return parquet_df


def write_iceberg(spark, parquet_df, accepted_count):
    print(f"\n== Iceberg: {TABLE} ==")
    first_batch = parquet_df.filter(F.col("observed_at") < F.lit(BATCH_BOUNDARY).cast("timestamp"))
    second_batch = parquet_df.filter(F.col("observed_at") >= F.lit(BATCH_BOUNDARY).cast("timestamp"))
    first_count, second_count = first_batch.count(), second_batch.count()
    print(f"Порция 1 (observed_at < {BATCH_BOUNDARY}): {first_count:,} строк")
    print(f"Порция 2 (observed_at >= {BATCH_BOUNDARY}): {second_count:,} строк")
    if not first_count or not second_count or first_count + second_count != accepted_count:
        raise ValueError("Порции должны быть непустыми, непересекающимися и покрывать все строки")
    print("Порции не пересекаются по времени и в сумме дают весь принятый набор")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS lakehouse.{STUDENT}")
    before = set()
    if spark.catalog.tableExists(TABLE):
        before = {row.snapshot_id for row in spark.sql(f"SELECT snapshot_id FROM {TABLE}.snapshots").collect()}

    print("\n-- Запись 1: CREATE OR REPLACE TABLE ... AS SELECT первой порции --")
    spark.sql(f"""
        CREATE OR REPLACE TABLE {TABLE}
        USING iceberg
        PARTITIONED BY (months(observed_at))
        AS SELECT * FROM parquet.`{PARQUET_PATH}`
        WHERE observed_at < TIMESTAMP '{BATCH_BOUNDARY} 00:00:00'
    """)
    after_first = spark.table(TABLE).count()
    if after_first != first_count:
        raise ValueError(f"После первой записи {after_first:,} строк вместо {first_count:,}")
    ids_first = {row.snapshot_id for row in spark.sql(f"SELECT snapshot_id FROM {TABLE}.snapshots").collect()}
    print(f"Строк в таблице: {after_first:,}; новые snapshots: {sorted(ids_first - before)}")

    print("\n-- Запись 2: INSERT INTO второй, непересекающейся порции --")
    spark.sql(f"""
        INSERT INTO {TABLE}
        SELECT * FROM parquet.`{PARQUET_PATH}`
        WHERE observed_at >= TIMESTAMP '{BATCH_BOUNDARY} 00:00:00'
    """)
    final = spark.table(TABLE).agg(
        F.count("*").alias("rows"),
        F.countDistinct("city_id", "observed_at").alias("unique_keys"),
    ).first()
    ids_second = {row.snapshot_id for row in spark.sql(f"SELECT snapshot_id FROM {TABLE}.snapshots").collect()}
    print(f"Строк в таблице: {final.rows:,}; уникальных ключей: {final.unique_keys:,}; "
          f"новые snapshots: {sorted(ids_second - ids_first)}")
    if final.rows != accepted_count or final.unique_keys != accepted_count:
        raise ValueError(f"Iceberg не совпал с принятым набором: {final.asDict()}")
    print("Обе записи добавили данные: строки Parquet перенесены без дублей и потерь")

    print("\n-- История snapshots (metadata table) --")
    spark.sql(f"""
        SELECT snapshot_id, committed_at, operation,
               summary['added-records']  AS added_records,
               summary['total-records']  AS total_records,
               summary['added-data-files'] AS added_files
        FROM {TABLE}.snapshots
        ORDER BY committed_at
    """).show(50, truncate=False)

    print("-- Схема таблицы из метаданных Iceberg --")
    spark.sql(f"DESCRIBE TABLE {TABLE}").show(30, truncate=False)
    return final.rows


def spark_answer(spark):
    print("\n== Ответ на исследовательский вопрос средствами Spark ==")
    print("Суточный ход температуры: средняя по часам UTC, зима (DJF) против лета (JJA)")
    spark.sql(f"""
        WITH seasonal AS (
            SELECT city_id,
                   CASE WHEN month(observed_at) IN (12, 1, 2) THEN 'winter'
                        WHEN month(observed_at) IN (6, 7, 8)  THEN 'summer' END AS season,
                   hour(observed_at) AS hour_utc,
                   temperature_2m_c
            FROM {TABLE}
            WHERE month(observed_at) IN (12, 1, 2, 6, 7, 8)
        ),
        hourly AS (
            SELECT city_id, season, hour_utc, avg(temperature_2m_c) AS mean_temp_c
            FROM seasonal GROUP BY city_id, season, hour_utc
        )
        SELECT city_id, season,
               round(min(mean_temp_c), 2) AS coldest_hour_mean_c,
               round(max(mean_temp_c), 2) AS warmest_hour_mean_c,
               round(max(mean_temp_c) - min(mean_temp_c), 2) AS daily_amplitude_c,
               min_by(hour_utc, mean_temp_c) AS coldest_hour_utc,
               max_by(hour_utc, mean_temp_c) AS warmest_hour_utc
        FROM hourly
        GROUP BY city_id, season
        ORDER BY season, daily_amplitude_c DESC
    """).show(60, truncate=False)

    print("-- Контрольный агрегат для сверки со Trino (тот же запрос выполняется в queries.sql) --")
    spark.sql(f"""
        SELECT count(*) AS observations,
               count(DISTINCT city_id) AS cities,
               sum(CAST(temperature_2m_c AS decimal(18,6))) AS sum_temperature_c,
               round(avg(CAST(temperature_2m_c AS decimal(18,6))), 6) AS mean_temperature_c
        FROM {TABLE}
    """).show(truncate=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ingestion-date", default=None,
                        help="по умолчанию берётся самый свежий префикс в бакете raw")
    args = parser.parse_args()
    ingestion_date = args.ingestion_date or latest_ingestion_date()
    raw_csv_path = f"s3a://{RAW_BUCKET}/{RAW_PREFIX}/ingestion_date={ingestion_date}/source.csv"

    spark = (SparkSession.builder.appName("hw01-weather-lakehouse")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        print(f"== Raw: {raw_csv_path} ==")
        read_schema = T.StructType(list(WEATHER_SCHEMA.fields)
                                   + [T.StructField(CORRUPT_COLUMN, T.StringType())])
        raw_df = (spark.read.schema(read_schema)
                  .option("header", True)
                  .option("enforceSchema", False)
                  .option("mode", "PERMISSIVE")
                  .option("columnNameOfCorruptRecord", CORRUPT_COLUMN)
                  .option("nullValue", "")
                  .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
                  .option("dateFormat", "yyyy-MM-dd")
                  .csv(raw_csv_path))
        raw_df.printSchema()

        print("\n== Проверки качества ==")
        accepted, accepted_count = validate(spark, raw_df)
        parquet_df = measure(spark, accepted, accepted_count, raw_csv_path)
        write_iceberg(spark, parquet_df, accepted_count)
        spark_answer(spark)
        print("\nГотово: raw -> проверки -> Parquet -> Iceberg выполнены полностью")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
