#!/usr/bin/env python3
# Kyle Fitzsimmons, 2015-2017


def filter_accuracy(points, cutoff=30):
    '''Filter out points with high reported horizontal accuracy values'''
    for p in points:
        if p['h_accuracy'] <= cutoff:
            yield p


def break_by_timegap(points, timegap=360):
    '''Break into trip segments when time recorded between points is
       > timegap variable and group points by segment number in a dictionary'''
    timebreak_groups = [[]]
    last_point = None
    for p in points:
    	if not last_point:
    		point = last_point






def run(parameters, metro_stations, points):
	points = tools.process_utm(points)
	if not points:
		return None, None

    high_accuracy_points = filter_accuracy(points, cutoff=parameters['accuracy_cutoff_meters'])
    segment_groups = break_by_timegap(high_accuracy_points, timegap=parameters['break_interval_seconds'])
    stations = metro_stations_utm(metro_stations)
    metro_linked_trips = find_metro_transfers(stations, segment_groups, buffer_m=parameters['subway_buffer_meters'])
    velocity_connected_trips = connect_by_velocity(metro_linked_trips)
    cleaned_trips = filter_single_points(velocity_connected_trips)
    missing_trips = infer_missing_trips(stations, cleaned_trips)
    rows = merge_trips(cleaned_trips, missing_trips, stations)
    trips, summaries = summarize(rows)
    return trips, summaries	