#!/usr/bin/env python
# Kyle Fitzsimmons, 2017
#
# Determine how many trips have a matching validation

import dataset
import utm

from tripbreaker import algorithm
from tripbreaker.modules.tools import pythagoras
from itinerum_common_helpers import fetch_user, fetch_survey_mobile_ids


db = dataset.connect('postgres://username:password@server:port/db',
                     reflect_metadata=False, 
                     row_type=dict)


def get_distance(pt1, pt2):
    pt1_easting, pt1_northing, _, _ = utm.from_latlon(pt1['latitude'],
                                                      pt1['longitude'])
    pt2_easting, pt2_northing, _, _ = utm.from_latlon(pt2['latitude'],
                                                      pt2['longitude'])
    return pythagoras((pt1_easting, pt1_northing), (pt2_easting, pt2_northing))


def soonest_prompt(params, last_point, prompts):
    match = None
    for ts, prompt in sorted(prompts.items()):
        # find first prompt available after trip end
        if ts >= last_point['timestamp']:
            # match prompt when it falls within the given parameters
            time_diff = (ts - last_point['timestamp']).total_seconds()
            distance = get_distance(prompt[0], last_point)

            if (time_diff <= params['max_time_diff']) and (distance <= params['max_distance']):
                match = prompts.pop(ts)[0]
                match['match_time_diff'] = time_diff
                match['match_distance'] = distance
            break
    return match, prompts


# TODO: ADD METRO STATIONS DATA FOR TRIP BREAKER
def match_prompts_to_trips(mobile_id, parameters):
    # fetch user and check that they have at least 1 trip's worth of prompts
    user = fetch_user(db, mobile_id=mobile_id)
    if not user:
        return 0, 0
    num_prompts = db['mobile_prompt_responses'].count(mobile_id=user['mobile_id'])
    if not num_prompts:
        return 0, 0

    # run tripbreak on all user's points
    coordinates = db['mobile_coordinates']
    points = coordinates.find(coordinates.table.columns.timestamp >= '2017-01-01 00:00:00',
                              mobile_id=mobile_id,
                              order_by='timestamp')

    metro_stations = []
    trips, summaries = algorithm.run(parameters, metro_stations, points)

    # get a set of grouped prompt responses for each timestamp
    available_prompts = {}
    for prompt in db['mobile_prompt_responses'].find(mobile_id=mobile_id,
                                                     order_by='timestamp'):
        ts = prompt['timestamp']
        available_prompts.setdefault(ts, []).append(prompt)

    matched_prompts, unmatched_prompts = 0, 0
    for trip_id, trip in trips.items():
        last_point = trip[-1]
        matched_prompt, available_prompts = soonest_prompt(parameters, last_point, available_prompts)
        if matched_prompt:
            matched_prompts += 1

        # insert any prompts that have failed to match by comparing against the last point
        # of the next detected trip
        pruned_prompt_timestamps = []
        next_trip = trips.get(trip_id + 1)
        for ts, prompt in sorted(available_prompts.items()):
            if next_trip and (ts <= next_trip[-1]['timestamp']):
                missed_prompt = available_prompts[ts][0]
                pruned_prompt_timestamps.append(ts)
        for ts in pruned_prompt_timestamps:
            available_prompts.pop(ts)
            unmatched_prompts += 1
    return matched_prompts, unmatched_prompts


def main():
    # create an interleaved table of detected trips with and without matching mode prompts
    parameters = {
        'break_interval': 300,
        'subway_buffer': 250,
        'max_time_diff': 1800,
        'max_distance': 150
    }
    total_matched, total_unmatched = 0, 0
    for mobile_id in fetch_survey_mobile_ids(db, survey_id=15):
        matched, unmatched = match_prompts_to_trips(mobile_id, parameters)
        total_matched += matched
        total_unmatched += unmatched
        if (total_matched + total_unmatched) > 0:
            print('Matched: {} / Unmatched: {} / Pct: {:.2f}%'.format(total_matched,
                                                                      total_unmatched,
                                                                      float(total_matched) / (total_unmatched + total_matched) * 100))


if __name__ == '__main__':
    main()
