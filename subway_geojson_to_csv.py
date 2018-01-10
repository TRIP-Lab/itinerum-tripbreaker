#!/usr/bin/env python3
# Kyle Fitzsimmons, 2017
import csv
import json

SUBWAY_STATIONS_JSON = './data/subway_stations.geojson'
SUBWAY_STATIONS_CSV = './data/subway_stations.csv'


with open(SUBWAY_STATIONS_JSON, 'r') as geojson_f:
    stations = json.loads(geojson_f.read())


csv_rows = [
    ['Station Name', 'Latitude', 'Longitude', 'OSM ID', 'Line']
]
for feature in stations['features']:
    print(feature['properties'])
    station_name = feature['properties'].get('name')
    longitude, latitude = feature['geometry']['coordinates']
    osm_id = feature['id']
    line = feature.get('line')
    csv_rows.append([station_name, latitude, longitude, osm_id, line])


with open(SUBWAY_STATIONS_CSV, 'w') as csv_f:
    writer = csv.writer(csv_f)
    writer.writerows(csv_rows)

