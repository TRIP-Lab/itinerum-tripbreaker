#!/usr/bin/env python3
# Kyle Fitzsimmons, 2017
import csv
import dataset
from datetime import date
import os


SURVEY_NAME = 'demo'
db = dataset.connect(
    'sqlite:///./data/{s}-processing-{date}.sqlite'.format(s=SURVEY_NAME,
                                                           date=date.today()))


def load_table_from_csv(table_name, csv_fp):
    print('Creating {t}...'.format(t=table_name))
    db[table_name].drop()
    with open(csv_fp, 'r', encoding='utf-8-sig') as csv_f:
        reader = csv.DictReader(csv_f)
        db[table_name].insert_many(reader)



### main
# 1: Load data from .csv to .sqlite tables
table_map = {
    'coordinates': '{s}-coordinates.csv'.format(s=SURVEY_NAME),
    'prompt_responses': '{s}-prompt_responses.csv'.format(s=SURVEY_NAME),
    'survey_responses': '{s}-survey_responses_fixed.csv'.format(s=SURVEY_NAME)
}
for table_name, csv_fn in table_map.items():
    csv_fp = os.path.join('data', csv_fn)
    load_table_from_csv(table_name, csv_fp)

# 2: create indexes for speeding up queries
index_map = {
    'coordinates': ['uuid', 'timestamp'],
    'prompt_responses': ['uuid', 'timestamp'],
    'survey_responses': ['uuid']
}
for table_name, index_columns in index_map.items():
    print('Creating indexes on {t}...'.format(t=table_name))
    db[table_name].create_index(index_columns)


