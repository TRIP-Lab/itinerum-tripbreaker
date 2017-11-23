#!/usr/bin/env python
# Kyle Fitzsimmons, 2017
#
# Perform common notebook tasks on Itinerum database


def fetch_survey_mobile_ids(db, survey_id):
    sql = '''SELECT id FROM mobile_users WHERE survey_id={};'''
    mobile_ids = [r['id'] for r in db.query(sql.format(survey_id))]
    return mobile_ids

# fetch an itinerum user by mobile id or randomly from a survey
def fetch_user(db, mobile_id=None, survey_id=None):
    if mobile_id:
        sql = '''SELECT * FROM mobile_users
                 JOIN mobile_survey_responses ON (mobile_users.id=mobile_survey_responses.mobile_id)
                 WHERE mobile_users.id={mobile_id}
                 LIMIT 1;'''
        result = db.query(sql.format(mobile_id=mobile_id))
    elif survey_id:
        sql = '''SELECT * FROM mobile_users
                 JOIN mobile_survey_responses ON (mobile_users.id=mobile_survey_responses.mobile_id)
                 WHERE mobile_users.survey_id={survey_id}
                 ORDER BY RANDOM()
                 LIMIT 1;'''
        result = db.query(sql.format(survey_id=survey_id))
    try:
        return result.next()
    except StopIteration:
        return None
