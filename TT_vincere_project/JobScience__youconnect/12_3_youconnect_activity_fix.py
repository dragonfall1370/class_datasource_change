# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import time
import datetime
import re
# from edenscott._edenscott_dtypes import *
import os
import psycopg2
import common.vincere_common as vincere_common
import common.vincere_custom_migration as vincere_custom_migration
import common.vincere_standard_migration as vincere_standard_migration
import sqlalchemy
from functools import reduce
from common import vincere_job_application
import pandas as pd

# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('yc_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)
sqlite_path = cf['default'].get('sqlite_path')


def html_to_text(html):
    from bs4 import BeautifulSoup
    # url = "http://news.bbc.co.uk/2/hi/health/2284783.stm"
    # html = urllib.urlopen(url).read()
    soup = BeautifulSoup(html)

    # kill all script and style elements
    for script in soup(["script", "style"]):
        script.extract()  # rip it out

    # get text
    text = soup.get_text()

    # break into lines and remove leading and trailing space on each
    lines = (line.strip() for line in text.splitlines())
    # break multi-headlines into a line each
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    # drop blank lines
    text = '\n'.join(chunk for chunk in chunks if chunk)

    return text


# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% extract data
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()
# %%
cand = pd.read_sql("""
select id as candidate_id, external_id as CandidateId from candidate where external_id is not null
""", connection)
job = pd.read_sql("""
select id as position_id, external_id as JobId from position_description where external_id is not null
""", connection)

activity = pd.read_sql("""
select a.id, candidate_id, position_id, content, ua.first_name||' '||ua.last_name as created_name
from activity a
left join (select id, first_name, last_name from  user_account where id <> -10) ua on a.user_account_id = ua.id
where content like '%#Migration jobscience interview%'
""", connection)

activity_2 = pd.read_sql("""
select a.id, candidate_id, position_id, content, ua.first_name||' '||ua.last_name as created_name
from activity a
left join (select id, first_name, last_name from  user_account where id <> -10) ua on a.user_account_id = ua.id
""", connection)

inject_activity = pd.read_csv('You Connect - YC Interview migration review - youconnect_interviews [inject this one].csv')
inject_activity = inject_activity.merge(job, on='jobid')
inject_activity = inject_activity.merge(cand, on='candidateid')
activity_1 = activity.merge(inject_activity, on=['position_id','candidate_id'])
activity_1['title'] = '#Migration jobscience interview'
activity_1['content'] = activity_1[['title', 'created_name']].apply(lambda x: ' - '.join([e for e in x if e]), axis=1)
# activity_1['insert_timestamp'] = pd.to_datetime(activity_1['ts2__Start_Time__c'])
vincere_custom_migration.load_data_to_vincere(activity_1, dest_db, 'update', 'activity', ['content'], ['id'], mylog)
activity_2_not_update = activity.loc[~activity['id'].isin(activity_1['id'])]
