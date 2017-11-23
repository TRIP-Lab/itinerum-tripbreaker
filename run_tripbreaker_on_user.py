#!/usr/bin/env python3
# Kyle Fitzsimmons, 2017
from collections import OrderedDict
import csv
import dataset
from datetime import datetime
import json
import os

from tripbreaker import algorithm



CONFIG = {
    'in_db_uri': 'postgres://username:password@localhost:5432/input_db',
    'out_db_uri': 'postgres://username:password@localhost:5432/output_db',
    'survey_name': 'survey_name',
    'mobile_user_email': 'username@email.com',
    'start': datetime(2017, 11, 7, 0, 0, 0),
    'end': datetime(2017, 11, 8, 0, 0, 0),
    'tripbreaker_parameters': {
        'break_interval': 360,
        'subway_buffer': 300
    },
    'input_srid': 4326,
    'output_srid': 32618,    
}


# fetch a user's points by email between the configured timestamp
# from the itinerum database
in_db = dataset.connect(CONFIG['in_db_uri'])
sql = '''SELECT *
         FROM mobile_coordinates
         JOIN mobile_survey_responses
             ON (mobile_coordinates.survey_id = mobile_survey_responses.survey_id)
         WHERE LOWER(mobile_survey_responses.response->>'Email') = LOWER('{email}')
         AND mobile_coordinates.timestamp >= '{start}'
         AND mobile_coordinates.timestamp <= '{end}'
         ORDER BY mobile_coordinates.timestamp ASC;'''
coordinates = in_db.query(sql.format(email=CONFIG['mobile_user_email'],
                                     start=CONFIG['start'],
                                     end=CONFIG['end']))


# load the metro station coordinates from .csv
metro_stations = []
with open('metro_stations.csv', 'r') as stations_f:
    reader = csv.DictReader(stations_f)
    for row in reader:
        metro_stations.append({
            'latitude': float(row['Y']),
            'longitude': float(row['X'])
        })


# run tripbreaker algorithm on database points
trips, summaries = algorithm.run(CONFIG['tripbreaker_parameters'],
                                 metro_stations,
                                 coordinates)


out_db = dataset.connect(CONFIG['out_db_uri'])
out_db['detected_trips'].drop()

coltypes = ['id SERIAL PRIMARY KEY',
            'trip_id INTEGER',
            'start_time TIMESTAMP WITH TIME ZONE',
            'end_time TIMESTAMP WITH TIME ZONE',
            'direct_distance FLOAT',
            'cumulative_distance FLOAT',
            'trip_code INTEGER',
            'merge_codes TEXT',
            'geom GEOMETRY']
create_trips_table_sql = '''CREATE TABLE detected_trips ({coltypes});'''
out_db.query(create_trips_table_sql.format(coltypes=', '.join(coltypes)))

insert_trip_sql = '''INSERT INTO detected_trips ({columns}) VALUES ({values});'''
for trip_id, trip in trips.items():
    coordinate_pairs = []
    for point in trip:
        coordinate_pairs.append('{lng} {lat}'.format(lng=point['longitude'],
                                                     lat=point['latitude']))
    coordinates_wkt = '\'LINESTRING({})\''.format(', '.join(coordinate_pairs))
    print(coordinate_pairs)
    geom_str = 'ST_GeomFromText({wkt}, {in_srid})'.format(
        wkt=coordinates_wkt,
        in_srid=CONFIG['input_srid'])
    properties = summaries[trip_id]
    trip_row = OrderedDict([
        ('trip_id', properties['trip_id']),
        ('start_time', properties['start'].isoformat()),
        ('end_time', properties['end'].isoformat()),
        ('direct_distance', properties['direct_distance']),
        ('cumulative_distance', properties['cumulative_distance']),
        ('trip_code', properties['trip_code']),
        ('merge_codes', properties['merge_codes']),
        ('geom', geom_str)
    ])
    values_str = ', '.join(["'" + str(v) + "'" for k, v in trip_row.items() if k != 'geom'])
    values_str += ', {}'.format(trip_row['geom'])
    out_db.query(insert_trip_sql.format(columns=', '.join(trip_row.keys()),
                                        values=values_str))

