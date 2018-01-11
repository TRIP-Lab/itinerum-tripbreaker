#!/usr/bin/env python3
# Kyle Fitzsimmons, 2017
import ciso8601
from collections import OrderedDict
import csv
import dataset
from datetime import datetime
import json
import os
from sqlalchemy import exc as sa_exc
import warnings

from tripbreaker import algorithm


# disable warnings from SQLAlchemy about unrecognized geometry-type
warnings.catch_warnings()
warnings.simplefilter("ignore", category=sa_exc.SAWarning)


### config
# Setup note: Before running script, make sure the output database
# is accessible with the PostGIS extension enabled
SURVEY_NAME = 'survey'
DATASET_DATE = '2017-12-14'
CONFIG = {
    'in_db_uri': 'sqlite:///../data/{s}-processing-{d}.sqlite'.format(s=SURVEY_NAME,
                                                                      d=DATASET_DATE),
    'out_db_uri': 'postgresql://username:password@server/tripbreaking_{s}'.format(s=SURVEY_NAME),
    'tripbreaker_parameters': {
        'break_interval_seconds': 360,
        'subway_buffer_meters': 300,
        'accuracy_cutoff_meters': 30
    },
    'subway_stations_csv': '../data/subway_stations.csv',
    'input_srid': 4326,
    'output_srid': 32618,    
}
in_db = dataset.connect(CONFIG['in_db_uri'])
out_db = dataset.connect(CONFIG['out_db_uri'])



### main
# transforms the data from sqlite string-types to the declared
# Python types, could also be used with a .csv reader
def serialize_row_types(mobile_uuid, rows):
    types = {
        'id': int,
        'uuid': str,
        'latitude': float,
        'longitude': float,
        'h_accuracy': float,
        'v_accuracy': float,
        'speed': float,
        'altitude': float,
        'timestamp': ciso8601.parse_datetime,
        'recorded_at': ciso8601.parse_datetime
    }
    for r in rows:
        for col, cast_func in types.items():
            if col not in r:
                continue
            if cast_func == float:
                if r.get(col, '').strip() == '':
                    r[col] = 0.
            r[col] = cast_func(r[col])
        # add in the uuid to keep consistent with full survey tripbreaker
        r['uuid'] = mobile_uuid
        yield r



# (re)create the output table for the trips with a declared schema
def create_trips_postgis_table():
    out_db['detected_trips'].drop()
    coltypes = ['id SERIAL PRIMARY KEY',
                'uuid VARCHAR(36)',
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


# iterate through the trips and generate database inserts with WKT strings for geography
def write_trips_to_postgis(mobile_uuid, trips, summaries):
    for trip_id, trip in trips.items():
        coordinate_pairs = []
        for point in trip:
            coordinate_pairs.append('{lng} {lat}'.format(lng=point['longitude'],
                                                         lat=point['latitude']))
        if len(coordinate_pairs) > 1:
            coordinates_wkt = '\'LINESTRING({})\''.format(', '.join(coordinate_pairs))
        else:
            coordinates_wkt = '\'POINT({})\''.format(coordinate_pairs[0])

        geom_str = 'ST_GeomFromText({wkt}, {in_srid})'.format(
            wkt=coordinates_wkt,
            in_srid=CONFIG['input_srid'])
        properties = summaries[trip_id]
        trip_row = OrderedDict([
            ('uuid', mobile_uuid),
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

        insert_trip_sql = '''INSERT INTO detected_trips ({columns}) VALUES ({values});'''
        out_db.query(insert_trip_sql.format(columns=', '.join(trip_row.keys()),
                                            values=values_str))


# (re)create the output table for the user's raw coordinates with a declared schema
def create_coordinates_postgis_table():
    out_db['coordinates'].drop()
    coltypes = ['id SERIAL PRIMARY KEY',
                'uuid VARCHAR(36)',
                'latitude FLOAT',
                'longitude FLOAT',
                'h_accuracy FLOAT',
                'v_accuracy FLOAT',
                'speed FLOAT',
                'altitude FLOAT',
                'timestamp TIMESTAMP WITH TIME ZONE',
                'easting FLOAT',
                'northing FLOAT',
                'geom GEOMETRY']
    create_coordinates_table_sql = '''CREATE TABLE coordinates ({coltypes});'''
    out_db.query(create_coordinates_table_sql.format(coltypes=', '.join(coltypes)))


# iterate through the coordinates and generate database inserts with WKT strings for geography
def write_coordinates_to_postgis(mobile_uuid, coordinates):
    for c in coordinates:
        coordinate_wkt = '\'POINT({lon} {lat})\''.format(lon=c['longitude'],
                                                         lat=c['latitude'])
        geom_str = 'ST_GeomFromText({wkt}, {in_srid})'.format(
            wkt=coordinate_wkt,
            in_srid=CONFIG['input_srid'])

        coordinate_row = OrderedDict([
            ('uuid', mobile_uuid),
            ('latitude', c['latitude']),
            ('longitude', c['longitude']),
            ('h_accuracy', c['h_accuracy']),
            ('v_accuracy', c['v_accuracy']),
            ('speed', c['speed']),
            ('altitude', c['altitude']),
            ('timestamp', c['timestamp'].isoformat()),
            ('easting', c['easting']),
            ('northing', c['northing']),
            ('geom', geom_str)
        ])

        values = []
        for k, v in coordinate_row.items():
            if k == 'geom':
                continue
            if v is not None:
                values.append("'" + str(v) + "'")
            else:
                values.append('NULL')
        values_str = ', '.join(values)
        values_str += ', {}'.format(coordinate_row['geom'])

        insert_trip_sql = '''INSERT INTO coordinates ({columns}) VALUES ({values});'''
        out_db.query(insert_trip_sql.format(columns=', '.join(coordinate_row.keys()),
                                            values=values_str))


# (re)create the output table for the user's processed trip points with a declared schema
def create_trip_points_postgis_table():
    out_db['trip_points'].drop()
    coltypes = ['id SERIAL PRIMARY KEY',
                'uuid VARCHAR(36)',
                'latitude FLOAT',
                'longitude FLOAT',
                'h_accuracy FLOAT',
                'v_accuracy FLOAT',
                'speed FLOAT',
                'altitude FLOAT',
                'timestamp TIMESTAMP WITH TIME ZONE',
                'easting FLOAT',
                'northing FLOAT',
                'segment_group INTEGER',
                'break_period INTEGER',
                'note VARCHAR',
                'trip INTEGER',
                'distance FLOAT',
                'trip_distance FLOAT',
                'avg_speed FLOAT',
                'trip_code INTEGER',
                'geom GEOMETRY']
    create_trip_points_table_sql = '''CREATE TABLE trip_points ({coltypes});'''
    out_db.query(create_trip_points_table_sql.format(coltypes=', '.join(coltypes)))


# iterate through the processed trip points and generate
# database inserts with WKT strings for geography
def write_trip_points_to_postgis(mobile_uuid, trip_points):
    for p in trip_points:
        trip_point_wkt = '\'POINT({lon} {lat})\''.format(lon=p['longitude'],
                                                         lat=p['latitude'])
        geom_str = 'ST_GeomFromText({wkt}, {in_srid})'.format(
            wkt=trip_point_wkt,
            in_srid=CONFIG['input_srid'])

        point_row = OrderedDict([
            ('uuid', mobile_uuid),
            ('latitude', p['latitude']),
            ('longitude', p['longitude']),
            ('h_accuracy', p.get('h_accuracy')),
            ('v_accuracy', p.get('v_accuracy')),
            ('speed', p.get('speed')),
            ('altitude', p.get('altitude')),
            ('timestamp', p['timestamp'].isoformat()),
            ('easting', p['easting']),
            ('northing', p['northing']),
            ('segment_group', p.get('segment_group')),
            ('break_period', int(p['break_period'])),
            ('note', p['note']),
            ('trip', p.get('trip')),
            ('distance', p.get('distance')),
            ('trip_distance', p.get('trip_distance')),
            ('avg_speed', p.get('avg_speed')),
            ('trip_code', p.get('trip_code')),
            ('geom', geom_str)
        ])

        values = []
        for k, v in point_row.items():
            if k == 'geom':
                continue
            if v is not None:
                values.append("'" + str(v) + "'")
            else:
                values.append('NULL')
        values_str = ', '.join(values)
        values_str += ', {}'.format(point_row['geom'])

        insert_trip_points_sql = '''INSERT INTO trip_points ({columns}) VALUES ({values});'''
        out_db.query(insert_trip_points_sql.format(columns=', '.join(point_row.keys()),
                                                   values=values_str))


# (re)create the output table for the user's prompt points with a declared schema
def create_prompt_points_postgis_table():
    out_db['prompt_points'].drop()
    coltypes = ['id SERIAL PRIMARY KEY',
                'uuid VARCHAR(36)',
                'latitude FLOAT',
                'longitude FLOAT',
                'response_1 VARCHAR(64)',
                'response_2 VARCHAR(64)',
                'timestamp TIMESTAMP WITH TIME ZONE',
                'recorded_at TIMESTAMP WITH TIME ZONE',
                'geom GEOMETRY']
    create_prompt_points_table_sql = '''CREATE TABLE prompt_points ({coltypes});'''
    out_db.query(create_prompt_points_table_sql.format(coltypes=', '.join(coltypes)))


# iterate through the processed prompt points and generate
# database inserts with WKT strings for geography
def write_prompt_points_to_postgis(mobile_uuid, prompt_points):
    grouped_prompts = {}
    for p in prompt_points:
        timestamp = p['timestamp']
        prompt_num = p['prompt_num']

        grouped_prompts.setdefault(timestamp, {})[prompt_num] = p


    for timestamp, prompt_pair in sorted(grouped_prompts.items()):
        p = prompt_pair['1']

        trip_point_wkt = '\'POINT({lon} {lat})\''.format(lon=p['longitude'],
                                                         lat=p['latitude'])
        geom_str = 'ST_GeomFromText({wkt}, {in_srid})'.format(
            wkt=trip_point_wkt,
            in_srid=CONFIG['input_srid'])

        point_row = OrderedDict([
            ('uuid', mobile_uuid),
            ('latitude', p['latitude']),
            ('longitude', p['longitude']),
            ('response_1', p['response']),
            ('response_2', prompt_pair['1']['response']),
            ('timestamp', p['timestamp'].isoformat()),
            ('recorded_at', p['recorded_at'].isoformat()),
            ('geom', geom_str)
        ])

        values = []
        for k, v in point_row.items():
            if k == 'geom':
                continue
            if v is not None:
                values.append("'" + str(v) + "'")
            else:
                values.append('NULL')
        values_str = ', '.join(values)
        values_str += ', {}'.format(point_row['geom'])

        insert_prompt_points_sql = '''INSERT INTO prompt_points ({columns}) VALUES ({values});'''
        out_db.query(insert_prompt_points_sql.format(columns=', '.join(point_row.keys()),
                                                     values=values_str))


def run():
    # load the metro station coordinates from .csv
    metro_stations = []
    with open(CONFIG['subway_stations_csv'], 'r') as stations_f:
        reader = csv.DictReader(stations_f)
        for r in reader:
            row = {}
            for key, item in r.items():
                row[key.lower()] = item
                if 'x' and 'y' in row:
                    row['latitude'] = row['y']
                    row['longitude'] = row['x']

            metro_stations.append({
                'latitude': float(row['latitude']),
                'longitude': float(row['longitude'])
            })


    create_trips_postgis_table()
    create_trip_points_postgis_table()
    create_coordinates_postgis_table()
    create_prompt_points_postgis_table()


    mobile_uuids = [u['uuid'] for u in in_db['survey_responses'].distinct('uuid')]
    for mobile_uuid in mobile_uuids:
        # create the user's points by uuid query with cast to their Python types
        coordinates_rows = in_db['coordinates'].find(uuid=mobile_uuid,
                                                     order_by=mobile_uuid)
        coordinates = list(serialize_row_types(mobile_uuid, coordinates_rows))
        prompt_rows = in_db['prompt_responses'].find(uuid=mobile_uuid,
                                                     order_by='timestamp ')
        prompts = list(serialize_row_types(mobile_uuid, prompt_rows))

        # run tripbreaker algorithm on user coordinates
        trips, summaries = algorithm.run(CONFIG['tripbreaker_parameters'],
                                         metro_stations,
                                         coordinates)
     
        # # write the user's trip line features to database
        print('Writing trips for {uuid} to database...'.format(uuid=mobile_uuid))
        if trips:
            write_trips_to_postgis(mobile_uuid, trips, summaries)

        # write the user's processed points from tripbreaker to database
        print('Writing trip points for {uuid} to database...'.format(uuid=mobile_uuid))
        if trips:
            trip_points = []
            for trip_id, points in trips.items():
                trip_points.extend(points)
            write_trip_points_to_postgis(mobile_uuid, trip_points)    

        # write the user's coordinates from input database to output PostGIS table
        print('Writing input coordinates for {uuid} to database...'.format(uuid=mobile_uuid))
        write_coordinates_to_postgis(mobile_uuid, coordinates)

        # write the user's mode prompts point features to database
        print('Writing input mode prompts for {uuid} to database...'.format(uuid=mobile_uuid))
        write_prompt_points_to_postgis(mobile_uuid, prompts)


if __name__ == '__main__':
    run()
