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
company = pd.read_sql("""
select "Company ID" as company_externalid
     , Name
from Company
""", engine_sqlite)
company['matcher'] = company['Name'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
comp_comment = pd.read_sql("""
select *
from CompanyComments
""", engine_sqlite)
comp_comment['matcher'] = comp_comment['name'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
comp_note = pd.read_sql("""
select *
from CompanyNotes
""", engine_sqlite)
comp_note['matcher'] = comp_note['name'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
comp_comment = comp_comment.merge(company, on='matcher')
comp_note = comp_note.merge(company, on='matcher')

comp_comment['insert_timestamp'] = pd.to_datetime(comp_comment['timeCreated'])
comp_comment['content'] = comp_comment['comment']
comp_comment['company_external_id'] = comp_comment['company_externalid']
comp_comment['owner'] = ''
comp_note['insert_timestamp'] = pd.to_datetime(comp_note['startTime'])
comp_note['company_external_id'] = comp_note['company_externalid']

comp_note['text'] = comp_note['text'].apply(lambda x: html_to_text(x) if x else x)
comp_note['content'] = comp_note[['type', 'startTime', 'endTime', 'text']]\
    .apply(lambda x: '\n'.join([': '.join([i for i in e if i]) for e in zip(['Type', 'Start time', 'End time', 'Content'], x) if e[1]]), axis=1)
comp_note['owner'] = comp_note['creator']
comp_note = comp_note.drop_duplicates()
comp_comment = comp_comment.drop_duplicates()

assert False
# %% load db
from common import vincere_activity
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
re1 = vincere_activity.transform_activities_temp(comp_comment, conn_str_ddb, mylog)
re1 = re1.where(re1.notnull(), None)
re2 = vincere_activity.transform_activities_temp(comp_note, conn_str_ddb, mylog)
re2 = re2.where(re2.notnull(), None)
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
re1.to_sql(con=engine_sqlite, name='vincere_activity_comp', if_exists='replace', dtype=dtype, index=False)
re2.to_sql(con=engine_sqlite, name='vincere_activity_comp', if_exists='append', dtype=dtype, index=False)