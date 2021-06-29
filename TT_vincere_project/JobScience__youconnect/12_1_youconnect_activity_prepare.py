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

# %%
candidate_interview = pd.read_sql("""
select
c.CreatedDate
, c.ts2__Job__c as job_externalid
, c.ts2__Candidate_Contact__c as candidate_externalid
, c.ts2__Account__c as company_externalid
, c.ts2__Cover_Letter__c
, c.ts2__Overall_Reject_Reason__c
, ts2__Status__c
, u.Email as owner
from ts2__Application__c c
left join User u on c.ts2__PrimaryRecruiter__c = u.Id
where c.IsDeleted=0
and ts2extams__Substatus__c = 'Interview YouConnect'
""", engine_sqlite)

candidate_interview['insert_timestamp'] = pd.to_datetime(candidate_interview['CreatedDate'])
candidate_interview['content'] = candidate_interview[['ts2__Cover_Letter__c', 'ts2__Overall_Reject_Reason__c', 'ts2__Status__c']]\
    .apply(lambda x: '\n'.join([': '.join([i for i in e if i]) for e in zip(['Cover letter', 'Reject Reason', 'Status'], x) if e[1]]), axis=1)
candidate_interview['content'] = '#Migration jobscience interview\n\n' + candidate_interview['content']
candidate_interview['company_external_id'] = candidate_interview['company_externalid']
candidate_interview['candidate_external_id'] = candidate_interview['candidate_externalid']
candidate_interview['position_external_id'] = candidate_interview['job_externalid']
assert False
# %%
from common import vincere_activity
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
re1 = vincere_activity.transform_tasks_temp(candidate_interview, conn_str_ddb, mylog)
re1 = re1.where(re1.notnull(), None)
re1['kpi_action'] = '24'
re1['action'] = '24'
re1['type'] = 'candidate'
#24
assert False
dtype = {
    'company_id': sqlalchemy.types.INT,
    'contact_id': sqlalchemy.types.INT,
    'candidate_id': sqlalchemy.types.INT,
    'position_id': sqlalchemy.types.INT,
    'user_account_id': sqlalchemy.types.INT,
    'insert_timestamp': sqlalchemy.types.DateTime(),
    'content': sqlalchemy.types.NVARCHAR,
    'category': sqlalchemy.types.VARCHAR,
    'type': sqlalchemy.types.VARCHAR,
    'kpi_action': sqlalchemy.types.VARCHAR,
    'action': sqlalchemy.types.VARCHAR

}
re1.to_sql(con=engine_sqlite, name='vincere_activity_candidate', if_exists='replace', dtype=dtype, index=False)
