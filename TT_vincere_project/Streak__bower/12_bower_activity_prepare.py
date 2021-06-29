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
cf.read('bower_config.ini')
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

# %% candidate
candidate = pd.read_sql("""
select ID as candidate_externalid
     , Name
from Candidate
""", engine_sqlite)
candidate['matcher'] = candidate['Name'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
cand_comment = pd.read_sql("""
select *
from CandidateComments
""", engine_sqlite)
cand_comment['matcher'] = cand_comment['name'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
cand_meeting = pd.read_sql("""
select *
from CandidateNotes
""", engine_sqlite)
cand_meeting['matcher'] = cand_meeting['name'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
cand_comment = cand_comment.merge(candidate, on='matcher')
cand_meeting = cand_meeting.merge(candidate, on='matcher')

cand_comment['insert_timestamp'] = pd.to_datetime(cand_comment['timeCreated'])
cand_comment['content'] = cand_comment['comment']
cand_comment['candidate_external_id'] = cand_comment['candidate_externalid']
cand_comment['owner'] = ''
cand_meeting['insert_timestamp'] = pd.to_datetime(cand_meeting['startTime'])
cand_meeting['candidate_external_id'] = cand_meeting['candidate_externalid']

cand_meeting['text'] = cand_meeting['text'].apply(lambda x: html_to_text(x) if x else x)
cand_meeting['content'] = cand_meeting[['type', 'startTime', 'endTime', 'text']]\
    .apply(lambda x: '\n'.join([': '.join([i for i in e if i]) for e in zip(['Type', 'Start time', 'End time', 'Content'], x) if e[1]]), axis=1)
cand_meeting['owner'] = cand_meeting['creator']
cand_meeting = cand_meeting.drop_duplicates()
cand_comment = cand_comment.drop_duplicates()

# %% contact
contact = pd.read_sql("""
select ID as contact_externalid
     , Name
from Contacts
""", engine_sqlite)
contact['matcher'] = contact['Name'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
cont_comment = pd.read_sql("""
select *
from ContactComments
""", engine_sqlite)
cont_comment['matcher'] = cont_comment['name'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
cont_note = pd.read_sql("""
select *
from ContactNotes
""", engine_sqlite)
cont_note['matcher'] = cont_note['name'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
cont_comment = cont_comment.merge(contact, on='matcher')
cont_note = cont_note.merge(contact, on='matcher')

cont_comment['insert_timestamp'] = pd.to_datetime(cont_comment['timeCreated'])
cont_comment['content'] = cont_comment['comment']
cont_comment['contact_external_id'] = cont_comment['contact_externalid']
cont_comment['owner'] = ''
cont_note['insert_timestamp'] = pd.to_datetime(cont_note['startTime'])
cont_note['contact_external_id'] = cont_note['contact_externalid']

cont_note['text'] = cont_note['text'].apply(lambda x: html_to_text(x) if x else x)
cont_note['content'] = cont_note[['type', 'startTime', 'endTime', 'text']]\
    .apply(lambda x: '\n'.join([': '.join([i for i in e if i]) for e in zip(['Type', 'Start time', 'End time', 'Content'], x) if e[1]]), axis=1)
cont_note['owner'] = cont_note['creator']
cont_note = cont_note.drop_duplicates()
cont_comment = cont_comment.drop_duplicates()

assert False
# %% load db
from common import vincere_activity
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
re1 = vincere_activity.transform_activities_temp(cand_comment, conn_str_ddb, mylog)
re1 = re1.where(re1.notnull(), None)
re2 = vincere_activity.transform_activities_temp(cand_meeting, conn_str_ddb, mylog)
re2 = re2.where(re2.notnull(), None)
re3 = vincere_activity.transform_activities_temp(cont_note, conn_str_ddb, mylog)
re3 = re3.where(re3.notnull(), None)
re4 = vincere_activity.transform_activities_temp(cont_comment, conn_str_ddb, mylog)
re4 = re4.where(re4.notnull(), None)

dtype = {
    'company_id': sqlalchemy.types.INT,
    'contact_id': sqlalchemy.types.INT,
    'candidate_id': sqlalchemy.types.INT,
    'position_id': sqlalchemy.types.INT,
    'user_account_id': sqlalchemy.types.INT,
    'insert_timestamp': sqlalchemy.types.DateTime(),
    'content': sqlalchemy.types.NVARCHAR,
    'category': sqlalchemy.types.VARCHAR,
    'type': sqlalchemy.types.VARCHAR
}
re1.to_sql(con=engine_sqlite, name='vincere_activity_cont_cand', if_exists='replace', dtype=dtype, index=False)
re2.to_sql(con=engine_sqlite, name='vincere_activity_cont_cand', if_exists='append', dtype=dtype, index=False)
re3.to_sql(con=engine_sqlite, name='vincere_activity_cont_cand', if_exists='append', dtype=dtype, index=False)
re4.to_sql(con=engine_sqlite, name='vincere_activity_cont_cand', if_exists='append', dtype=dtype, index=False)