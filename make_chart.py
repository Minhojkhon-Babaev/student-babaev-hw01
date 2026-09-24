"""Необязательное дополнение: один график по результату аналитического SQL.

Данные не хардкодятся — CSV выгружается из Trino той же логикой, что в queries.sql:

    docker compose exec -T trino trino --output-format=CSV_HEADER --execute "
      WITH hourly AS (
          SELECT city_id,
                 CASE WHEN month(observed_at) IN (12,1,2) THEN 'winter' ELSE 'summer' END AS season,
                 hour(observed_at) AS hour_utc,
                 avg(temperature_2m_c) AS mean_temp_c
          FROM lakehouse.babaev.weather
          WHERE month(observed_at) IN (12,1,2,6,7,8)
          GROUP BY 1, 2, 3)
      SELECT city_id, season, round(max(mean_temp_c) - min(mean_temp_c), 2) AS daily_amplitude_c
      FROM hourly GROUP BY city_id, season" > chart_data.csv

    python3 scripts/hw01/make_chart.py chart_data.csv chart.svg

График отвечает на исследовательский вопрос: суточная амплитуда по городам, зима против лета.
"""

import csv
import sys

WIDTH = 1000
LEFT = 170
RIGHT = 90
ROW = 34
TOP = 96
BOTTOM = 130
TEXT_X = 24
COLORS = {"summer": "#e2673a", "winter": "#3a6ee2"}
TITLE = "Суточный ход температуры по городам: лето против зимы"
SUBTITLE = "Разница между самым тёплым и самым холодным часом средних суток. Open-Meteo ERA5, 2024 год, часы местного времени"
CAPTION = ("Вывод: летняя амплитуда в 2–5 раз больше зимней, и её размер задаёт не широта, "
           "а континентальность — приморские Владивосток и Сочи ровнее степного Ростова-на-Дону "
           "и резко континентального Якутска; в Мурманске зимой суточного хода почти нет: полярная ночь.")

CITY_NAMES = {
    "moscow": "Москва", "saint_petersburg": "Санкт-Петербург", "murmansk": "Мурманск",
    "nizhny_novgorod": "Нижний Новгород", "kazan": "Казань", "rostov_on_don": "Ростов-на-Дону",
    "sochi": "Сочи", "yekaterinburg": "Екатеринбург", "novosibirsk": "Новосибирск",
    "krasnoyarsk": "Красноярск", "yakutsk": "Якутск", "vladivostok": "Владивосток",
    "astana": "Астана", "minsk": "Минск",
}


def escape(text):
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def read_data(path):
    values = {}
    with open(path, encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            values.setdefault(row["city_id"], {})[row["season"]] = float(row["daily_amplitude_c"])
    if not values:
        sys.exit(f"Нет данных в {path}")
    return sorted(values.items(), key=lambda item: -item[1].get("summer", 0))


def render(cities, out_path):
    plot_width = WIDTH - LEFT - RIGHT
    height = TOP + ROW * len(cities) + BOTTOM
    top_value = max(max(seasons.values()) for _, seasons in cities)
    axis_max = (int(top_value) // 2 + 1) * 2
    scale = plot_width / axis_max
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{height}" '
        f'viewBox="0 0 {WIDTH} {height}" font-family="Helvetica, Arial, sans-serif">',
        f'<rect width="{WIDTH}" height="{height}" fill="#ffffff"/>',
        f'<text x="{TEXT_X}" y="34" font-size="16" font-weight="600" fill="#1d1d1f">{escape(TITLE)}</text>',
        f'<text x="{TEXT_X}" y="56" font-size="12" fill="#6b6b70">{escape(SUBTITLE)}</text>',
    ]

    legend_x = TEXT_X
    for season, label in (("summer", "лето (июнь–август)"), ("winter", "зима (декабрь–февраль)")):
        parts.append(f'<rect x="{legend_x}" y="68" width="12" height="12" fill="{COLORS[season]}"/>')
        parts.append(f'<text x="{legend_x + 18}" y="78" font-size="12" fill="#1d1d1f">{label}</text>')
        legend_x += 160

    plot_bottom = TOP + ROW * len(cities)
    for tick in range(0, axis_max + 1, 2):
        x = LEFT + tick * scale
        parts.append(f'<line x1="{x:.1f}" y1="{TOP}" x2="{x:.1f}" y2="{plot_bottom}" '
                     f'stroke="#e6e6ea" stroke-width="1"/>')
        parts.append(f'<text x="{x:.1f}" y="{plot_bottom + 18}" font-size="11" fill="#6b6b70" '
                     f'text-anchor="middle">{tick}</text>')
    parts.append(f'<text x="{LEFT + plot_width / 2:.1f}" y="{plot_bottom + 38}" font-size="12" '
                 f'fill="#6b6b70" text-anchor="middle">амплитуда, °C</text>')

    for index, (city_id, seasons) in enumerate(cities):
        row_top = TOP + index * ROW
        parts.append(f'<text x="{LEFT - 12}" y="{row_top + 21}" font-size="12" fill="#1d1d1f" '
                     f'text-anchor="end">{escape(CITY_NAMES.get(city_id, city_id))}</text>')
        for offset, season in enumerate(("summer", "winter")):
            value = seasons.get(season)
            if value is None:
                continue
            bar_width = max(value * scale, 1.0)
            y = row_top + 4 + offset * 12
            parts.append(f'<rect x="{LEFT}" y="{y}" width="{bar_width:.1f}" height="10" '
                         f'rx="2" fill="{COLORS[season]}"/>')
            parts.append(f'<text x="{LEFT + bar_width + 6:.1f}" y="{y + 9}" font-size="10.5" '
                         f'fill="#4a4a4f">{value:.2f}</text>')

    for line_index, line in enumerate(wrap(CAPTION, 118)):
        parts.append(f'<text x="{TEXT_X}" y="{plot_bottom + 62 + line_index * 17}" font-size="12" '
                     f'fill="#1d1d1f">{escape(line)}</text>')
    parts.append("</svg>")
    with open(out_path, "w", encoding="utf-8") as handle:
        handle.write("\n".join(parts))
    print(f"График: {out_path}; городов: {len(cities)}")


def wrap(text, limit):
    lines, current = [], ""
    for word in text.split():
        candidate = f"{current} {word}".strip()
        if len(candidate) > limit:
            lines.append(current)
            current = word
        else:
            current = candidate
    if current:
        lines.append(current)
    return lines


if __name__ == "__main__":
    source = sys.argv[1] if len(sys.argv) > 1 else "chart_data.csv"
    target = sys.argv[2] if len(sys.argv) > 2 else "chart.svg"
    render(read_data(source), target)
