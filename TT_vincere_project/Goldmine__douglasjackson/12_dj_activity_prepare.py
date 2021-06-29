# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import re
# from edenscott._edenscott_dtypes import *
import os
import psycopg2
import common.vincere_common as vincere_common
import common.vincere_custom_migration as vincere_custom_migration
import common.vincere_standard_migration as vincere_standard_migration
import pandas as pd
import sqlalchemy
# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('dj_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
src_db = cf[cf['default'].get('src_db')]
sqlite_path = cf['default'].get('sqlite_path')
dest_db = cf[cf['default'].get('dest_db')]
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')
# engine_postgre_src = sqlalchemy.create_engine(conn_str_sdb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
# engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')

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
assert False
user = pd.read_csv('user.csv')
# %% history
history = pd.read_sql("""
select h.ACCOUNTNO, com.ID as company_externalid, REF, ACTVCODE, RESULTCODE,CONCAT_WS(' ',cast(substring(cast(CREATEON as varchar(max)),0,12) as date),CREATEAT) as date, concat('history_',rn,'.',EXT) as file_name,CREATEBY
from history_html h
--left join USERS u on u.USERNAME = h.CREATEBY
left join (select ACCOUNTNO, p.ID
from CONTACT1 c
left join company p on p.COMPANY = c.COMPANY
where p.ID is not null) com on h.ACCOUNTNO = com.ACCOUNTNO
""", engine_mssql)
history = history.drop_duplicates()
history['CREATEBY'] = history['CREATEBY'].apply(lambda x: x.strip() if x else x)
history = history.merge(user, left_on='CREATEBY', right_on='USERNAME', how='left')
history = history.where(history.notnull(), None)
history = history.loc[history['email'].notnull()]

history_content = pd.read_sql("""
select * from parse_history_html_0
union
select * from parse_history_html_1
union
select * from parse_history_html_2
union
select * from parse_history_html_3
union
select * from parse_history_html_4
union
select * from parse_history_html_5
union
select * from parse_history_html_6
union
select * from parse_history_html_7
union
select * from parse_history_html_8
union
select * from parse_history_html_9
union
select * from parse_history_html_10
union
select * from parse_history_html_11
union
select * from parse_history_html_12
union
select * from parse_history_html_13
union
select * from parse_history_html_14
union
select * from parse_history_html_15
union
select * from parse_history_html_16
union
select * from parse_history_html_17
union
select * from parse_history_html_18
union
select * from parse_history_html_19
union
select * from parse_history_html_20
union
select * from parse_history_html_21
union
select * from parse_history_html_22
union
select * from parse_history_html_23
union
select * from parse_history_html_24
union
select * from parse_history_html_25
union
select * from parse_history_html_26
union
select * from parse_history_html_27
union
select * from parse_history_html_28
union
select * from parse_history_html_29
union
select * from parse_history_html_30
union
select * from parse_history_html_31
union
select * from parse_history_html_32
union
select * from parse_history_html_33
union
select * from parse_history_html_34
union
select * from parse_history_html_35
union
select * from parse_history_html_36
union
select * from parse_history_html_37
union
select * from parse_history_html_38
union
select * from parse_history_html_39
union
select * from parse_history_html_40
union
select * from parse_history_html_41
union
select * from parse_history_html_42
union
select * from parse_history_html_43
union
select * from parse_history_html_44
union
select * from parse_history_html_45
union
select * from parse_history_html_46
union
select * from parse_history_html_47
""", engine_sqlite)
history_content = history_content.drop_duplicates()
history = history.merge(history_content, left_on='file_name', right_on='file', how='left')

# %% pending
pending = pd.read_sql("""
select h.ACCOUNTNO, com.ID as company_externalid, REF, ACTVCODE,CONCAT_WS(' ',cast(substring(cast(CREATEON as varchar(max)),0,12) as date),CREATEAT) as date, concat('pending_',rn,'.',EXT) as file_name,CREATEBY
from pending_html h
-- left join USERS u on u.USERNAME = h.CREATEBY
left join (select ACCOUNTNO, p.ID
from CONTACT1 c
left join company p on p.COMPANY = c.COMPANY
where p.ID is not null) com on h.ACCOUNTNO = com.ACCOUNTNO
""", engine_mssql)
pending = pending.drop_duplicates()
pending = pending.loc[pending['CREATEBY'].notnull()]
pending['CREATEBY'] = pending['CREATEBY'].apply(lambda x: x.strip() if x else x)
pending = pending.merge(user, left_on='CREATEBY', right_on='USERNAME', how='left')
pending = pending.where(pending.notnull(), None)
pending = pending.loc[pending['email'].notnull()]

pending_content = pd.read_sql("""
select * from parse_pending_html_0
""", engine_sqlite)
pending_content = pending_content.drop_duplicates()
pending = pending.merge(pending_content, left_on='file_name', right_on='file', how='left')

# %% note
note = pd.read_sql("""
select h.ACCOUNTNO, com.ID as company_externalid, USERID,CREATEDDATE, concat('note_',rn,'.',EXT) as file_name
from note_html h
--left join USERS u on u.USERNAME = h.USERID
left join (select ACCOUNTNO, p.ID
from CONTACT1 c
left join company p on p.COMPANY = c.COMPANY
where p.ID is not null) com on h.ACCOUNTNO = com.ACCOUNTNO
""", engine_mssql)
note = note.drop_duplicates()
note = note.loc[note['USERID'].notnull()]
note['USERID'] = note['USERID'].apply(lambda x: x.strip() if x else x)
note = note.merge(user, left_on='USERID', right_on='USERNAME', how='left')
note = note.where(note.notnull(), None)
note = note.loc[note['email'].notnull()]

notecontent = pd.read_sql("""
select * from parse_note_html_0
union
select * from parse_note_html_1
""", engine_sqlite)
notecontent = notecontent.drop_duplicates()
note = note.merge(notecontent, left_on='file_name', right_on='file', how='left')
assert False
# %%
history['insert_timestamp'] = pd.to_datetime(history['date'], format='%Y-%m-%d %H:%M:%S')
history['owner'] = history['email']
history['ACTVCODE'] = history['ACTVCODE'].apply(lambda x: str(x) if x else x)
history['REF'] = history['REF'].apply(lambda x: str(x) if x else x)
history['content'] = history['content'].apply(lambda x: str(x) if x else x)
history['content'] = history[['ACTVCODE','REF', 'content']].apply(lambda x: '\n'.join([': '.join([i for i in e if i]) for e in zip(['CODE','Reference',''], x) if e[1]]), axis=1)
history['content'] = '---HISTORY---\n'+history['content']
history['subject'] = None
tem1_history = history.copy()
tem2_history = history.copy()
tem1_history['contact_external_id'] = tem1_history['ACCOUNTNO']
tem1_history['company_external_id'] = tem1_history['company_externalid']
tem2_history['candidate_external_id'] = tem2_history['ACCOUNTNO']

pending['insert_timestamp'] = pd.to_datetime(pending['date'], format='%Y-%m-%d %H:%M:%S')
pending['owner'] = pending['email']
pending['ACTVCODE'] = pending['ACTVCODE'].apply(lambda x: str(x) if x else x)
pending['REF'] = pending['REF'].apply(lambda x: str(x) if x else x)
pending['subject'] = pending['REF']
pending['content'] = pending['content'].apply(lambda x: str(x) if x else x)
pending['content'] = pending[['ACTVCODE','REF', 'content']].apply(lambda x: '\n'.join([': '.join([i for i in e if i]) for e in zip(['CODE','Reference',''], x) if e[1]]), axis=1)
pending['content'] = '---PENDING---\n'+pending['content']
tem1_pending = pending.copy()
tem2_pending = pending.copy()
tem1_pending['contact_external_id'] = tem1_pending['ACCOUNTNO']
tem1_pending['company_external_id'] = tem1_pending['company_externalid']
tem2_pending['candidate_external_id'] = tem2_pending['ACCOUNTNO']

note['insert_timestamp'] = pd.to_datetime(note['CREATEDDATE'], format='%Y-%m-%d %H:%M:%S')
note['owner'] = note['email']
note['content'] = note['content'].apply(lambda x: str(x) if x else x)
note['content'] = '---NOTE---\n'+note['content']
note['subject'] = None
tem1_note = note.copy()
tem2_note = note.copy()
tem1_note['contact_external_id'] = tem1_note['ACCOUNTNO']
tem1_note['company_external_id'] = tem1_note['company_externalid']
tem2_note['candidate_external_id'] = tem2_note['ACCOUNTNO']

tem1_history = tem1_history.where(tem1_history.notnull(),None)
tem2_history = tem2_history.where(tem2_history.notnull(),None)
tem1_pending = tem1_pending.where(tem1_pending.notnull(),None)
tem2_pending = tem2_pending.where(tem2_pending.notnull(),None)
tem1_note = tem1_note.where(tem1_note.notnull(),None)
tem2_note = tem2_note.where(tem2_note.notnull(),None)

# %% load db
from common import vincere_activity
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
re1 = vincere_activity.transform_activities_temp(tem1_history, conn_str_ddb, mylog)
re1 = re1.where(re1.notnull(), None)

re2 = vincere_activity.transform_activities_temp(tem2_history, conn_str_ddb, mylog)
re2 = re2.where(re2.notnull(), None)

re3 = vincere_activity.transform_tasks_temp(tem1_pending, conn_str_ddb, mylog)
re3 = re3.where(re3.notnull(), None)

re4 = vincere_activity.transform_tasks_temp(tem2_pending, conn_str_ddb, mylog)
re4 = re4.where(re4.notnull(), None)

re5 = vincere_activity.transform_activities_temp(tem1_note, conn_str_ddb, mylog)
re5 = re5.where(re5.notnull(), None)

re6 = vincere_activity.transform_activities_temp(tem2_note, conn_str_ddb, mylog)
re6 = re6.where(re6.notnull(), None)

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
    'subject': sqlalchemy.types.VARCHAR
}
re1.to_sql(con=engine_sqlite, name='vincere_activity_fixed', if_exists='replace', dtype=dtype, index=False)
re2.to_sql(con=engine_sqlite, name='vincere_activity_fixed', if_exists='append', dtype=dtype, index=False)
re3.to_sql(con=engine_sqlite, name='vincere_activity_fixed', if_exists='append', dtype=dtype, index=False)
re4.to_sql(con=engine_sqlite, name='vincere_activity_fixed', if_exists='append', dtype=dtype, index=False)
re5.to_sql(con=engine_sqlite, name='vincere_activity_fixed', if_exists='append', dtype=dtype, index=False)
re6.to_sql(con=engine_sqlite, name='vincere_activity_fixed', if_exists='append', dtype=dtype, index=False)

# %% activity
activity = pd.read_sql("""
select id, company_id, contact_id, candidate_id, content, insert_timestamp from activity
""", engine_postgre_review)
activity['matcher'] = activity['content'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower() if x else x)
activity['insert_timestamp'] = activity['insert_timestamp'].astype(str)[0:19]
activity['company_id'] = activity['company_id'].astype(str)
activity['contact_id'] = activity['contact_id'].astype(str)
activity['candidate_id'] = activity['candidate_id'].astype(str)
act_fixed = pd.read_sql("""
select * from vincere_activity_fixed where user_account_id is not null
""", engine_sqlite)
act_fixed['matcher'] = act_fixed['content'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower() if x else x)
act_fixed['insert_timestamp'] = act_fixed['insert_timestamp'].astype(str)[0:19]
act_fixed['company_id'] = act_fixed['company_id'].astype(str)
act_fixed['contact_id'] = act_fixed['contact_id'].astype(str)
act_fixed['candidate_id'] = act_fixed['candidate_id'].astype(str)
tem = activity.merge(act_fixed, on=['company_id','contact_id','candidate_id','matcher','insert_timestamp'])
tem = tem.drop_duplicates()
tem2 = tem[['id','user_account_id']].drop_duplicates()
tem2['rn'] = tem2.groupby('id').cumcount()
tem2.loc[tem2['rn']>0]
tem2.loc[tem2['id']==135615]
tem3 = tem2.loc[tem2['rn']==0]
tem4 = tem2.loc[tem2['rn']>0]
vincere_custom_migration.psycopg2_bulk_update_tracking(tem3, connection, ['user_account_id', ], ['id', ], 'activity', mylog)
vincere_custom_migration.psycopg2_bulk_update_tracking(tem4, connection, ['user_account_id', ], ['id', ], 'activity', mylog)