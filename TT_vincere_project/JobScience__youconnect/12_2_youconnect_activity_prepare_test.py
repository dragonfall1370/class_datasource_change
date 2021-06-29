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
candidate_interview = pd.read_csv('You Connect - YC Interview migration review - youconnect_interviews [inject this one].csv')
assert False
candidate_interview = pd.read_sql("""
select
i.ts2__Start_Time__c as CreatedDate
, c.ts2__Job__c as job_externalid
, c.ts2__Candidate_Contact__c as candidate_externalid
, c.ts2__Account__c as company_externalid
, c.ts2__Cover_Letter__c
, c.ts2__Overall_Reject_Reason__c
, c.ts2__Status__c
, u.Email as owner
from ts2__Application__c c
left join ts2__Interview__c i on i.ts2__Candidate__c = c.ts2__Candidate_Contact__c and  i.ts2__Job__c = c.ts2__Job__c
left join User u on i.ts2__PrimaryRecruiter__c  = u.Id
where c.IsDeleted=0
and c.ts2extams__Substatus__c = 'Interview YouConnect'
and ts2__Start_Time__c is not null
""", engine_sqlite)

candidate_interview['insert_timestamp'] = pd.to_datetime(candidate_interview['ts2__Start_Time__c'])
candidate_interview['content'] = candidate_interview['content INCLUDING THE RIGHT OWNER']
# candidate_interview['company_external_id'] = candidate_interview['company_externalid']
candidate_interview['candidate_external_id'] = candidate_interview['candidateid']
candidate_interview['position_external_id'] = candidate_interview['jobid']
candidate_interview['owner'] = candidate_interview['Created by']
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
# assert False
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
re1.to_sql(con=engine_sqlite, name='vincere_activity_candidate_3', if_exists='replace', dtype=dtype, index=False)

activity_db = pd.read_sql("""
# select * from vincere_activity_candidate_3
# """, engine_sqlite)
#
# activity_prod = pd.read_sql("""
# select id, company_id, candidate_id, position_id  from activity where content like '%#Migration jobscience interview%'
# """, connection)
#
# activity_prod_1 = activity_prod.merge(activity_db, on=['company_id','candidate_id','position_id'])
# activity_prod_1['insert_timestamp'] = pd.to_datetime(activity_prod_1['insert_timestamp'])
# vincere_custom_migration.psycopg2_bulk_update_tracking(activity_prod_1, connection, ['insert_timestamp'], ['id'], 'activity', mylog)
# a = activity_prod_1[['company_id','candidate_id','position_id','content','insert_timestamp']]
# a.to_csv('youconnect_activities.csv',index=False)
