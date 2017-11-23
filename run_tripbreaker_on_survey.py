# -*- coding: utf-8 -*-
#!/usr/bin/env python
# Kyle Fitzsimmons, 2017
import dataset
from datetime import datetime
from decimal import Decimal
from tripbreaker import algorithm


survey_name = 'survey_name'
itinerum_db = dataset.connect('postgres://username:password@localhost:5432/input_db')
trips_db = dataset.connect('sqlite:///{}-trips.sqlite'.format(survey_name))
tripbreaker_parameters = {
    'break_interval': 360,
    'subway_buffer': 300
}
metro_stations = []


def process_points(uuid, points, headers):
    rows = []
    for pt in points:
        pt_row = {'uuid': uuid}
        for h in headers:
            if h == 'uuid':
                continue
            processed = pt[h]
            if isinstance(processed, datetime):
                processed = processed.isoformat()
            if isinstance(processed, Decimal):
                processed = float(processed)
            pt_row[h] = processed
        rows.append(pt_row)
    return rows


# find the survey within the database
survey = itinerum_db['surveys'].find_one(name=survey_name.lower())

# fetch all mobile users associated with survey
users = itinerum_db['mobile_users'].find(survey_id=survey['id'], order_by='id')


# update output db with trips information
parameters = {
    'break_interval': 360,
    'subway_buffer': 300
}

columns = ['uuid', 'trip', 'latitude', 'longitude', 'trip_distance', 'distance',
           'timestamp', 'break_period', 'trip_code']

total_rows = 0
trips_rows = []
for user in users:
    print('Breaking trips for: {}'.format(user['id']))

    # get all the coordinates for mobile user and run tripbreak
    gps_points = itinerum_db['mobile_coordinates'].find(mobile_id=user['id'], order_by='timestamp')
    trips, summaries = algorithm.run(tripbreaker_parameters, metro_stations, gps_points)

    if not trips and not summaries:
        continue

    summary_rows = []
    for trip_id, summary in summaries.items():
        summary['uuid'] = user['uuid']
        summary_rows.append(summary)

    for trip_id, points in trips.iteritems():
        trips_rows.extend(process_points(user['uuid'], points, columns))
    print(len(trips_rows))

    # write rows to database
    trips_db['summaries'].insert_many(summary_rows)
trips_db['trips'].insert_many(trips_rows)
