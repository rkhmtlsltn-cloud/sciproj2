import os
import json
from datetime import datetime
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

print("start")

csv_path = os.path.join(BASE_DIR, "stations_new.csv")
print("reading csv...")

stations_df = pd.read_csv(
    csv_path,
    sep=";",
    encoding="utf-8-sig",
    engine="python"
)

print("csv loaded")
print("original columns:", stations_df.columns.tolist())
print("rows before cleaning:", len(stations_df))

stations_df.columns = [str(c).strip() for c in stations_df.columns]

col_map = {}
for c in stations_df.columns:
    col_map[str(c).strip().lower()] = c

name_col = None
pm25_col = None
lat_col = None
lon_col = None
district_col = None
district_ru_col = None
date_col = None

for key in ["name", "station_name"]:
    if key in col_map:
        name_col = col_map[key]
        break

for key in ["pm25", "pm_25", "pm2.5", "pm2_5"]:
    if key in col_map:
        pm25_col = col_map[key]
        break

for key in ["lat", "latitude"]:
    if key in col_map:
        lat_col = col_map[key]
        break

for key in ["lon", "lng", "longitude"]:
    if key in col_map:
        lon_col = col_map[key]
        break

for key in ["district"]:
    if key in col_map:
        district_col = col_map[key]
        break

for key in ["district_ru", "district_rus", "districtru"]:
    if key in col_map:
        district_ru_col = col_map[key]
        break

for key in ["date", "datetime", "time", "timestamp", "measured_at", "measuredat"]:
    if key in col_map:
        date_col = col_map[key]
        break

if name_col is None and len(stations_df.columns) > 2:
    name_col = stations_df.columns[2]

if date_col is None and len(stations_df.columns) > 3:
    date_col = stations_df.columns[3]

if lat_col is None:
    for c in stations_df.columns:
        if str(c).strip().lower() in ["lat", "latitude"]:
            lat_col = c
            break

if lon_col is None:
    for c in stations_df.columns:
        if str(c).strip().lower() in ["lon", "lng", "longitude"]:
            lon_col = c
            break

if district_col is None:
    for c in stations_df.columns:
        if str(c).strip().lower() == "district":
            district_col = c
            break

if district_ru_col is None:
    for c in stations_df.columns:
        low = str(c).strip().lower()
        if "district" in low and "ru" in low:
            district_ru_col = c
            break

if pm25_col is None:
    for c in stations_df.columns:
        low = str(c).strip().lower()
        if "pm25" in low or "pm2.5" in low:
            pm25_col = c
            break

need_cols = [name_col, pm25_col, lat_col, lon_col, district_col, district_ru_col, date_col]
if any(col is None for col in need_cols):
    print("columns found:", stations_df.columns.tolist())
    print("name_col =", name_col)
    print("pm25_col =", pm25_col)
    print("lat_col =", lat_col)
    print("lon_col =", lon_col)
    print("district_col =", district_col)
    print("district_ru_col =", district_ru_col)
    print("date_col =", date_col)
    raise ValueError("Не удалось определить нужные колонки в CSV")

stations_df = stations_df[[name_col, pm25_col, lat_col, lon_col, district_col, district_ru_col, date_col]].copy()

stations_df = stations_df.rename(columns={
    name_col: "station_name",
    pm25_col: "pm25",
    lat_col: "latitude",
    lon_col: "longitude",
    district_col: "district",
    district_ru_col: "district_ru",
    date_col: "date"
})

stations_df["type"] = "Air Station"

stations_df["latitude"] = (
    stations_df["latitude"]
    .astype(str)
    .str.replace(",", ".", regex=False)
    .str.strip()
)

stations_df["longitude"] = (
    stations_df["longitude"]
    .astype(str)
    .str.replace(",", ".", regex=False)
    .str.strip()
)

stations_df["pm25"] = (
    stations_df["pm25"]
    .astype(str)
    .str.replace(",", ".", regex=False)
    .str.strip()
)

stations_df["latitude"] = pd.to_numeric(stations_df["latitude"], errors="coerce")
stations_df["longitude"] = pd.to_numeric(stations_df["longitude"], errors="coerce")
stations_df["pm25"] = pd.to_numeric(stations_df["pm25"], errors="coerce")

stations_df["district_ru"] = stations_df["district_ru"].astype(str).str.strip()
stations_df["district"] = stations_df["district"].astype(str).str.strip()
stations_df["station_name"] = stations_df["station_name"].astype(str).str.strip()
stations_df["date"] = stations_df["date"].astype(str).str.strip()

stations_df["date_parsed"] = pd.to_datetime(
    stations_df["date"],
    format="%d.%m.%Y %H:%M",
    errors="coerce"
)

mask_bad = stations_df["date_parsed"].isna()
if mask_bad.sum() > 0:
    print("dates not parsed in first format:", int(mask_bad.sum()))
    stations_df.loc[mask_bad, "date_parsed"] = pd.to_datetime(
        stations_df.loc[mask_bad, "date"],
        errors="coerce",
        dayfirst=True
    )

stations_df["year"] = stations_df["date_parsed"].dt.year

print("rows before dropna:", len(stations_df))

stations_df = stations_df.dropna(subset=["latitude", "longitude", "district_ru", "date_parsed", "year"])

print("rows after dropna:", len(stations_df))

stations_df["year"] = stations_df["year"].astype(int)

print("years found:", sorted(stations_df["year"].unique().tolist()))
print(stations_df[["station_name", "district_ru", "year"]].head())

geojson_path = os.path.join(BASE_DIR, "almaty.geo.json")
with open(geojson_path, "r", encoding="utf-8") as f:
    geojson_data = json.load(f)

print("geojson loaded")

for feature in geojson_data.get("features", []):
    props = feature.setdefault("properties", {})
    if "district" not in props or not props["district"]:
        ru_name = props.get("nameRu")
        if ru_name:
            props["district"] = ru_name

years = sorted(stations_df["year"].unique().tolist())
years_str = [str(y) for y in years]

district_info = {}
for (year, district_ru), group in stations_df.groupby(["year", "district_ru"]):
    year = str(year)
    if year not in district_info:
        district_info[year] = {}

    district_info[year][district_ru] = {
        "district_ru": district_ru,
        "district_en": str(group["district"].iloc[0]) if len(group) > 0 else "",
        "pollution": round(group["pm25"].mean(), 2) if group["pm25"].notna().any() else None,
        "count": int(len(group))
    }

stations_df = stations_df.sort_values("date_parsed")
stations_df["year"] = stations_df["year"].astype(str)

station_groups = {}

for (year, district_ru, station_name), group in stations_df.groupby(
    ["year", "district_ru", "station_name"], sort=False
):
    group = group.sort_values("date_parsed")

    first_row = group.iloc[0]
    last_row = group.iloc[-1]

    measurements = []
    for _, row in group.iterrows():
        measurements.append({
            "date": pd.to_datetime(row["date_parsed"]).strftime("%d.%m.%Y %H:%M"),
            "pm25": round(float(row["pm25"]), 2) if pd.notna(row["pm25"]) else None
        })

    if year not in station_groups:
        station_groups[year] = {}

    if district_ru not in station_groups[year]:
        station_groups[year][district_ru] = []

    station_groups[year][district_ru].append({
        "station_name": str(station_name),
        "latitude": float(first_row["latitude"]),
        "longitude": float(first_row["longitude"]),
        "type": str(first_row["type"]),
        "district_ru": str(district_ru),
        "district_en": str(first_row["district"]),
        "year": str(year),
        "pm25_latest": round(float(last_row["pm25"]), 2) if pd.notna(last_row["pm25"]) else 0,
        "latest_date": pd.to_datetime(last_row["date_parsed"]).strftime("%d.%m.%Y %H:%M"),
        "measurements": measurements
    })

for year in station_groups:
    for district_ru in station_groups[year]:
        station_groups[year][district_ru].sort(key=lambda x: x["station_name"].lower())

print("stations with histories prepared")
print("data prepared")
print("years in final data:", years_str)

geojson_js = json.dumps(geojson_data, ensure_ascii=False)
district_info_js = json.dumps(district_info, ensure_ascii=False)
station_groups_js = json.dumps(station_groups, ensure_ascii=False)
years_js = json.dumps(years_str, ensure_ascii=False)

build_time = datetime.now().strftime("%H:%M:%S")

html = f"""<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Almaty District Monitor {build_time}</title>
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/leaflet@1.9.4/dist/leaflet.css"/>
    <style>
        html, body {{
            margin: 0;
            padding: 0;
            width: 100%;
            height: 100%;
            font-family: Arial, sans-serif;
            background: #f6f7f9;
        }}

        #app {{
            display: flex;
            width: 100%;
            height: 100%;
        }}

        #map {{
            flex: 1;
            height: 100%;
            background: #eef2f6;
        }}

        #sidebar {{
            width: 360px;
            background: #ffffff;
            border-left: 1px solid #e5e7eb;
            padding: 22px 20px;
            box-sizing: border-box;
            overflow-y: auto;
            z-index: 1000;
        }}

        .title {{
            font-size: 24px;
            font-weight: 700;
            margin-bottom: 8px;
            color: #111827;
        }}

        .subtitle {{
            font-size: 14px;
            color: #6b7280;
            margin-bottom: 18px;
        }}

        .card {{
            background: #ffffff;
            border: 1px solid #e5e7eb;
            border-radius: 16px;
            padding: 16px;
            margin-bottom: 16px;
        }}

        .district-name {{
            font-size: 20px;
            font-weight: 700;
            margin-bottom: 12px;
            color: #111827;
        }}

        .metric {{
            margin-bottom: 10px;
            font-size: 15px;
            color: #1f2937;
        }}

        .metric b {{
            color: #111827;
        }}

        .stations-title {{
            font-size: 16px;
            font-weight: 700;
            margin: 0 0 10px;
            color: #111827;
        }}

        .station-item {{
            border-bottom: 1px solid #e5e7eb;
            padding: 10px 0;
            font-size: 14px;
            color: #374151;
        }}

        .station-item:last-child {{
            border-bottom: none;
        }}

        .legend {{
            margin-top: 8px;
            font-size: 14px;
            color: #374151;
        }}

        .legend-row {{
            display: flex;
            align-items: center;
            margin-bottom: 8px;
        }}

        .dot {{
            width: 12px;
            height: 12px;
            border-radius: 50%;
            display: inline-block;
            margin-right: 10px;
        }}

        .hint {{
            font-size: 14px;
            color: #6b7280;
            line-height: 1.6;
        }}

        .top-btn {{
            margin-top: 10px;
            background: #0f172a;
            color: white;
            border: none;
            border-radius: 10px;
            padding: 10px 14px;
            font-size: 14px;
            cursor: pointer;
        }}

        .top-btn:hover {{
            background: #1e293b;
        }}

        .leaflet-tooltip {{
            font-size: 13px;
            padding: 6px 10px;
            border-radius: 8px;
            border: 1px solid #d1d5db;
            box-shadow: none;
        }}

        .year-select {{
            width: 100%;
            padding: 10px;
            border-radius: 10px;
            border: 1px solid #d1d5db;
            font-size: 14px;
            background: white;
        }}

        .leaflet-container {{
            font: inherit;
        }}
    </style>
</head>
<body>
<div id="app">
    <div id="map"></div>

    <div id="sidebar">
        <div class="title">Almaty District Monitor</div>
        <div class="subtitle">OpenAQ-style карта районов и станций</div>

        <div class="card">
            <div class="stations-title">Выбор года</div>
            <select id="year-select" class="year-select"></select>
        </div>

        <div class="card" id="info-card">
            <div class="district-name">Алматы</div>
            <div class="hint">
                Нажми на район на карте.<br>
                Потом нажми на конкретную станцию.
            </div>
            <button class="top-btn" onclick="resetView()">Сбросить выбор</button>
        </div>

        <div class="card">
            <div class="stations-title">Легенда</div>
            <div class="legend">
                <div class="legend-row"><span class="dot" style="background:green;"></span>PM2.5 меньше 50</div>
                <div class="legend-row"><span class="dot" style="background:orange;"></span>PM2.5 от 50 до 100</div>
                <div class="legend-row"><span class="dot" style="background:red;"></span>PM2.5 больше 100</div>
                <div class="legend-row"><span class="dot" style="background:#2563eb;"></span>Выбранный район</div>
            </div>
        </div>

        <div class="card">
            <div class="stations-title">Станции / измерения</div>
            <div id="station-list" class="hint">Выбери район, потом станцию</div>
        </div>
    </div>
</div>

<script src="https://cdn.jsdelivr.net/npm/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
const geojsonData = {geojson_js};
const districtInfo = {district_info_js};
const stationGroups = {station_groups_js};
const years = {years_js};

const map = L.map('map', {{
    preferCanvas: false,
    zoomControl: true
}}).setView([43.2389, 76.8897], 11);

L.tileLayer('https://tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
    maxZoom: 19,
    attribution: '&copy; OpenStreetMap contributors'
}}).addTo(map);

map.createPane('districtPane');
map.getPane('districtPane').style.zIndex = 400;

map.createPane('markerPane');
map.getPane('markerPane').style.zIndex = 650;

let currentMarkersLayer = L.layerGroup().addTo(map);
let selectedLayer = null;
let allDistrictLayers = [];
let selectedYear = years.length > 0 ? years[years.length - 1] : "";

const yearSelect = document.getElementById('year-select');

for (let i = 0; i < years.length; i++) {{
    const option = document.createElement('option');
    option.value = years[i];
    option.textContent = years[i];
    yearSelect.appendChild(option);
}}

if (years.length > 0) {{
    yearSelect.value = selectedYear;
}}

yearSelect.addEventListener('change', function() {{
    selectedYear = this.value;
    resetView();
}});

function getDefaultStyle() {{
    return {{
        pane: 'districtPane',
        color: '#111827',
        weight: 2,
        opacity: 0.9,
        fillOpacity: 0
    }};
}}

function getHoverStyle() {{
    return {{
        pane: 'districtPane',
        color: '#374151',
        weight: 3,
        opacity: 1,
        fillOpacity: 0
    }};
}}

function getHighlightStyle() {{
    return {{
        pane: 'districtPane',
        color: '#2563eb',
        weight: 4,
        opacity: 1,
        fillOpacity: 0
    }};
}}

function getColor(pm25) {{
    if (pm25 < 50) return 'green';
    if (pm25 < 100) return 'orange';
    return 'red';
}}

function clearMarkers() {{
    currentMarkersLayer.clearLayers();
}}

function showStationDetails(station) {{
    const stationList = document.getElementById('station-list');

    if (!station || !station.measurements || station.measurements.length === 0) {{
        stationList.innerHTML = 'Нет измерений';
        return;
    }}

    let html = `
        <div class="station-item">
            <b>${{station.station_name}}</b><br>
            Район: ${{station.district_ru}}<br>
            Всего измерений: ${{station.measurements.length}}
        </div>
    `;

    for (let i = station.measurements.length - 1; i >= 0; i--) {{
        const m = station.measurements[i];
        html += `
            <div class="station-item">
                <b>${{m.date}}</b><br>
                PM2.5: ${{m.pm25 ?? 'Нет данных'}}
            </div>
        `;
    }}

    stationList.innerHTML = html;
}}

function showMarkersForDistrict(districtRu) {{
    clearMarkers();

    const stations = (stationGroups[selectedYear] && stationGroups[selectedYear][districtRu]) || [];

    for (let i = 0; i < stations.length; i++) {{
        const s = stations[i];
        const pm25 = Number(s.pm25_latest) || 0;
        const color = getColor(pm25);

        const marker = L.circleMarker([s.latitude, s.longitude], {{
            pane: 'markerPane',
            radius: 7,
            color: color,
            fillColor: color,
            fillOpacity: 0.95,
            opacity: 1,
            weight: 2,
            interactive: true
        }});

        marker.bindPopup(
            `<b>${{s.station_name}}</b><br>Последний PM2.5: ${{pm25}}<br>District: ${{s.district_ru}}<br>Last date: ${{s.latest_date}}<br>Year: ${{s.year}}`
        );

        marker.on('click', function() {{
            showStationDetails(s);
        }});

        currentMarkersLayer.addLayer(marker);
    }}
}}

function updatePanel(districtRu) {{
    const data = (districtInfo[selectedYear] && districtInfo[selectedYear][districtRu]) || null;
    const panel = document.getElementById('info-card');
    const stationList = document.getElementById('station-list');

    if (!data) {{
        panel.innerHTML = `
            <div class="district-name">Нет данных</div>
            <div class="hint">Для этого района нет данных за ${{selectedYear}}</div>
            <button class="top-btn" onclick="resetView()">Сбросить выбор</button>
        `;
        stationList.innerHTML = 'Нет станций';
        return;
    }}

    panel.innerHTML = `
        <div class="district-name">${{data.district_ru}}</div>
        <div class="metric"><b>Год:</b> ${{selectedYear}}</div>
        <div class="metric"><b>Средний PM2.5:</b> ${{data.pollution ?? 'Нет данных'}}</div>
        <div class="metric"><b>Количество измерений:</b> ${{data.count}}</div>
        <button class="top-btn" onclick="resetView()">Сбросить выбор</button>
    `;

    const stations = (stationGroups[selectedYear] && stationGroups[selectedYear][districtRu]) || [];

    if (stations.length === 0) {{
        stationList.innerHTML = 'Нет станций';
    }} else {{
        stationList.innerHTML = stations.map(s => `
            <div class="station-item">
                <b>${{s.station_name}}</b><br>
                Последний PM2.5: ${{s.pm25_latest}}<br>
                Последняя дата: ${{s.latest_date}}
            </div>
        `).join('');
    }}
}}

function resetDistrictStyles() {{
    for (let i = 0; i < allDistrictLayers.length; i++) {{
        allDistrictLayers[i].setStyle(getDefaultStyle());
    }}
    selectedLayer = null;
}}

function resetView() {{
    resetDistrictStyles();
    clearMarkers();

    document.getElementById('info-card').innerHTML = `
        <div class="district-name">Алматы</div>
        <div class="hint">
            Выбран год: ${{selectedYear || 'нет'}}<br>
            Нажми на район на карте.<br>
            Потом нажми на конкретную станцию.
        </div>
        <button class="top-btn" onclick="resetView()">Сбросить выбор</button>
    `;

    document.getElementById('station-list').innerHTML = 'Выбери район, потом станцию';
    map.setView([43.2389, 76.8897], 11);
}}

L.geoJSON(geojsonData, {{
    style: function() {{
        return getDefaultStyle();
    }},
    onEachFeature: function(feature, layer) {{
        allDistrictLayers.push(layer);

        const districtRu = feature.properties?.district || feature.properties?.nameRu || feature.properties?.name || 'Unknown';
        const districtLabel = feature.properties?.nameRu || feature.properties?.district || feature.properties?.name || 'Unknown';

        layer.bindTooltip(districtLabel, {{
            sticky: true,
            direction: 'auto'
        }});

        layer.on('mouseover', function() {{
            if (selectedLayer !== layer) {{
                layer.setStyle(getHoverStyle());
            }}
        }});

        layer.on('mouseout', function() {{
            if (selectedLayer !== layer) {{
                layer.setStyle(getDefaultStyle());
            }}
        }});

        layer.on('click', function() {{
            resetDistrictStyles();
            selectedLayer = layer;
            layer.setStyle(getHighlightStyle());

            updatePanel(districtRu);
            showMarkersForDistrict(districtRu);

            currentMarkersLayer.eachLayer(function(marker) {{
                if (marker.bringToFront) {{
                    marker.bringToFront();
                }}
            }});

            if (typeof layer.getBounds === 'function') {{
                map.fitBounds(layer.getBounds(), {{ padding: [20, 20] }});
            }}
        }});
    }}
}}).addTo(map);

resetView();
</script>
</body>
</html>
"""

output_path = os.path.join(BASE_DIR, "final_map.html")

if os.path.exists(output_path):
    try:
        os.remove(output_path)
        print("old html removed")
    except Exception as e:
        print("could not remove old html:", e)

with open(output_path, "w", encoding="utf-8") as f:
    f.write(html)

print("html saved")
print(output_path)