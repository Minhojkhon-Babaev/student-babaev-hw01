-- ДЗ 1, babaev, датасет weather (Open-Meteo ERA5, почасовые наблюдения за 2024 год).
-- Подключение: jdbc:trino://localhost:8088/lakehouse/babaev, пользователь student.
-- В DataGrip назначьте файлу подключение Trino и выполняйте блоки по порядку.
-- Из CLI целиком:  docker compose exec -T trino trino < /путь/queries.sql
-- Выполнять после успешного pipeline.py: таблица lakehouse.babaev.weather должна существовать.
-- Блок 5 каждый раз пересоздаёт справочник в memory: этот каталог не переживает перезапуск Trino.

-- =====================================================================
-- 1. Что видно через одно подключение
-- =====================================================================
SHOW CATALOGS;

SHOW SCHEMAS IN lakehouse;

SHOW TABLES IN lakehouse.babaev;

DESCRIBE lakehouse.babaev.weather;

-- =====================================================================
-- 2. Проверочные запросы: Trino читает ту же таблицу, что записал Spark
-- =====================================================================
SELECT count(*) AS observations,
       count(DISTINCT city_id) AS cities,
       min(observed_at) AS first_observation,
       max(observed_at) AS last_observation
FROM lakehouse.babaev.weather;

-- Ключ наблюдения уникален: запрос обязан вернуть 0 строк.
SELECT city_id, observed_at, count(*) AS duplicates
FROM lakehouse.babaev.weather
GROUP BY city_id, observed_at
HAVING count(*) > 1;

-- Сверка ключевого результата со Spark. DOUBLE приводим к DECIMAL(18,6):
-- точная десятичная арифметика даёт одинаковое значение в обоих движках,
-- тогда как порядок суммирования DOUBLE может дать расхождение в последних разрядах.
SELECT count(*) AS observations,
       count(DISTINCT city_id) AS cities,
       sum(CAST(temperature_2m_c AS decimal(18,6))) AS sum_temperature_c,
       round(avg(CAST(temperature_2m_c AS decimal(18,6))), 6) AS mean_temperature_c
FROM lakehouse.babaev.weather;

-- Первые строки: LIMIT ограничивает вывод, а не задаёт порядок.
SELECT city_id, observed_at, temperature_2m_c, relative_humidity_2m_pct, wind_speed_10m_kmh
FROM lakehouse.babaev.weather
ORDER BY city_id, observed_at
LIMIT 10;

-- =====================================================================
-- 3. История версий таблицы: два snapshot от двух записей
-- =====================================================================
SELECT snapshot_id,
       parent_id,
       committed_at,
       operation,
       summary['added-records'] AS added_records,
       summary['total-records'] AS total_records,
       summary['added-data-files'] AS added_data_files
FROM lakehouse.babaev."weather$snapshots"
ORDER BY committed_at;

-- Какие файлы и какие месяцы лежат в партициях (скрытое партиционирование по months(observed_at)).
SELECT partition, record_count, file_count
FROM lakehouse.babaev."weather$partitions"
ORDER BY 1;

-- =====================================================================
-- 4. Аналитический SQL: суточный ход температуры по городам, зима против лета
--    Исследовательский вопрос: как различается суточный ход температуры
--    (амплитуда между самым тёплым и самым холодным часом суток) между
--    городами и федеральными округами и как он меняется от зимы к лету.
-- =====================================================================
WITH hourly AS (
    SELECT city_id,
           CASE WHEN month(observed_at) IN (12, 1, 2) THEN 'winter' ELSE 'summer' END AS season,
           hour(observed_at) AS hour_utc,
           avg(temperature_2m_c) AS mean_temp_c
    FROM lakehouse.babaev.weather
    WHERE month(observed_at) IN (12, 1, 2, 6, 7, 8)
    GROUP BY city_id,
             CASE WHEN month(observed_at) IN (12, 1, 2) THEN 'winter' ELSE 'summer' END,
             hour(observed_at)
)
SELECT city_id,
       season,
       round(max(mean_temp_c) - min(mean_temp_c), 2) AS daily_amplitude_c,
       round(min(mean_temp_c), 2) AS coldest_hour_mean_c,
       round(max(mean_temp_c), 2) AS warmest_hour_mean_c,
       min_by(hour_utc, mean_temp_c) AS coldest_hour_utc,
       max_by(hour_utc, mean_temp_c) AS warmest_hour_utc
FROM hourly
GROUP BY city_id, season
ORDER BY season, daily_amplitude_c DESC;

-- =====================================================================
-- 5. Справочник в каталоге memory
--    Происхождение: составлен вручную по административному делению РФ
--    (федеральные округа) и по постоянному времени региона — в России
--    сезонный перевод часов отменён с 2014 года.
--    Правило: только города РФ из нашей выборки. Астана и Минск в справочник
--    не входят намеренно: они вне административного деления РФ.
-- =====================================================================
DROP TABLE IF EXISTS memory.default.city_reference;

CREATE TABLE memory.default.city_reference AS
SELECT * FROM (VALUES
    ('moscow',           'Москва',           'Центральный',     3),
    ('saint_petersburg', 'Санкт-Петербург',  'Северо-Западный', 3),
    ('murmansk',         'Мурманск',         'Северо-Западный', 3),
    ('nizhny_novgorod',  'Нижний Новгород',  'Приволжский',     3),
    ('kazan',            'Казань',           'Приволжский',     3),
    ('rostov_on_don',    'Ростов-на-Дону',   'Южный',           3),
    ('sochi',            'Сочи',             'Южный',           3),
    ('yekaterinburg',    'Екатеринбург',     'Уральский',       5),
    ('novosibirsk',      'Новосибирск',      'Сибирский',       7),
    ('krasnoyarsk',      'Красноярск',       'Сибирский',       7),
    ('yakutsk',          'Якутск',           'Дальневосточный', 9),
    ('vladivostok',      'Владивосток',      'Дальневосточный', 10)
) AS t(city_id, city_name_ru, federal_district, utc_offset_hours);

SELECT * FROM memory.default.city_reference ORDER BY federal_district, city_id;

-- Ключ справочника уникален: запрос обязан вернуть 0 строк,
-- иначе JOIN размножит наблюдения.
SELECT city_id, count(*) AS key_count
FROM memory.default.city_reference
GROUP BY city_id
HAVING count(*) > 1;

-- =====================================================================
-- 6. Федеративный SQL: Iceberg в lakehouse + справочник в memory
--    LEFT JOIN сохраняет города без соответствия в явной группе «Не сопоставлен».
-- =====================================================================
SELECT coalesce(r.federal_district, 'Не сопоставлен') AS federal_district,
       count(DISTINCT w.city_id) AS cities,
       count(*) AS observations,
       round(avg(CAST(w.temperature_2m_c AS decimal(18,6))), 2) AS mean_temperature_c,
       round(min(w.temperature_2m_c), 1) AS min_temperature_c,
       round(max(w.temperature_2m_c), 1) AS max_temperature_c
FROM lakehouse.babaev.weather w
LEFT JOIN memory.default.city_reference r ON w.city_id = r.city_id
GROUP BY coalesce(r.federal_district, 'Не сопоставлен')
ORDER BY observations DESC;

-- JOIN не размножает и не теряет строки: before_join = after_join,
-- unmatched показывает, сколько наблюдений попало в группу «Не сопоставлен».
WITH source AS (
    SELECT city_id FROM lakehouse.babaev.weather
)
SELECT (SELECT count(*) FROM source) AS before_join,
       count(*) AS after_join,
       count_if(r.city_id IS NULL) AS unmatched_observations,
       count(DISTINCT CASE WHEN r.city_id IS NULL THEN s.city_id END) AS unmatched_cities
FROM source s
LEFT JOIN memory.default.city_reference r ON s.city_id = r.city_id;

-- Ответ на исследовательский вопрос: справочник даёт смещение часового пояса,
-- поэтому суточный ход считается по местному времени, а не по UTC.
-- Здесь оставлены только сопоставленные города; их доля показана запросом выше.
WITH local_hourly AS (
    SELECT r.federal_district,
           w.city_id,
           CASE WHEN month(w.observed_at) IN (12, 1, 2) THEN 'winter' ELSE 'summer' END AS season,
           (hour(w.observed_at) + r.utc_offset_hours) % 24 AS hour_local,
           avg(w.temperature_2m_c) AS mean_temp_c
    FROM lakehouse.babaev.weather w
    JOIN memory.default.city_reference r ON w.city_id = r.city_id
    WHERE month(w.observed_at) IN (12, 1, 2, 6, 7, 8)
    GROUP BY r.federal_district, w.city_id,
             CASE WHEN month(w.observed_at) IN (12, 1, 2) THEN 'winter' ELSE 'summer' END,
             (hour(w.observed_at) + r.utc_offset_hours) % 24
),
per_city AS (
    SELECT federal_district, city_id, season,
           max(mean_temp_c) - min(mean_temp_c) AS daily_amplitude_c,
           min_by(hour_local, mean_temp_c) AS coldest_hour_local,
           max_by(hour_local, mean_temp_c) AS warmest_hour_local
    FROM local_hourly
    GROUP BY federal_district, city_id, season
)
SELECT federal_district,
       season,
       count(*) AS cities,
       round(avg(daily_amplitude_c), 2) AS mean_daily_amplitude_c,
       round(min(daily_amplitude_c), 2) AS min_city_amplitude_c,
       round(max(daily_amplitude_c), 2) AS max_city_amplitude_c,
       array_join(array_sort(array_agg(DISTINCT CAST(coldest_hour_local AS varchar))), ',') AS coldest_hours_local,
       array_join(array_sort(array_agg(DISTINCT CAST(warmest_hour_local AS varchar))), ',') AS warmest_hours_local
FROM per_city
GROUP BY federal_district, season
ORDER BY season, mean_daily_amplitude_c DESC;
