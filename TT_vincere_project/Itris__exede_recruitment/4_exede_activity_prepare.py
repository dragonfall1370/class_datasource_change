# -*- coding: UTF-8 -*-
import configparser
import os
import pathlib

import pandas as pd
import psycopg2
import sqlalchemy

import common.logger_config as log
import common.vincere_custom_migration as vincere_custom_migration
from common import vincere_common

# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('exede_config.ini')
mylog = log.get_info_logger(cf['default'].get('log_file'))
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
sqlite_path = cf['default'].get('sqlite_path')
src_db = cf[cf['default'].get('src_db')]

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect to database
ddbconn = psycopg2.connect(host=dest_db.get('server'), user=dest_db.get('user'), password=dest_db.get('password'), database=dest_db.get('database'), port=dest_db.get('port'))
ddbconn.set_client_encoding('UTF8')
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')

# %% comment
comment = pd.read_sql("""
 select
 	   c.ApplicantId as candidate_external_id
       , c.CompanyId as company_external_id
       , c.ContactId as contact_external_id
       , c.JobId as position_external_id
       , c.CreatedDateTime as insert_timestamp
       , c.CommentTypeName as type
       , u.EmailAddress as owner
       , c.Text as content
       , d.DOC_PATH as attached_files
    from vw_Comment c
    left join Documents d on c.DocumentId = d.DOC_ID
    left join [tblvwUser] u on c.CreatedUserId=u.Id
    where c.Id > 281716
""", engine_mssql)

reminder = pd.read_sql("""
select 
d.DIARY_ID
, case datt.FORM_ID 
	when 1 then 'CANDIDATE'
	when 2 then 'COMPANY'
	when 3 then 'CONTACT'
	when 4 then 'JOB'
	end as entity_type
, datt.RECORD_ID
, dc.NAME as category_name
, d.CREATED_ON
, d.START_DATETIME
, d.END_DATETIME
, d.SUBJECT
, d.NOTES
, d.RECIPIENT
, u.EmailAddress as owner
from Diary d
join DiaryAttendees datt on d.DIARY_ID = datt.DIARY_ID
join DiaryCat dc on d.CATEGORY_ID = dc.CATEGORY_ID
left join [tblvwUser] u on d.EMP_ID=u.Id
where DELETED_DATE is null
and d.DIARY_ID > 320
;
""", engine_mssql)

# %% transform data

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

comment['attached_files'] = comment['attached_files'].map(lambda x: x.split('\\')[-1] if x else x)
comment['content'] = comment[['type', 'content', 'attached_files']]\
    .apply(lambda x: '\n\n'.join([': '.join(e) for e in zip(['Activity Type', 'Content', 'Attached File'], x) if e[1]]), axis=1)

reminder['insert_timestamp'] = reminder[['CREATED_ON', 'START_DATETIME']].apply(lambda x: x[0] if x[0] else x[1], axis=1)
reminder['candidate_external_id'] = reminder.apply(lambda x: x['RECORD_ID'] if x.entity_type == 'CANDIDATE' else None, axis=1)
reminder['contact_external_id'] = reminder.apply(lambda x: x['RECORD_ID'] if x.entity_type == 'CONTACT' else None, axis=1)
reminder['position_external_id'] = reminder.apply(lambda x: x['RECORD_ID'] if x.entity_type == 'JOB' else None, axis=1)
reminder['company_external_id'] = reminder.apply(lambda x: x['RECORD_ID'] if x.entity_type == 'COMPANY' else None, axis=1)
reminder['START_DATETIME'] = pd.to_datetime(reminder['START_DATETIME'], errors='coerce').dt.strftime('%d/%m/%Y %H:%M:%S')
reminder['END_DATETIME'] = pd.to_datetime(reminder['END_DATETIME'], errors='coerce').dt.strftime('%d/%m/%Y %H:%M:%S')
reminder['content'] = reminder[['category_name', 'START_DATETIME', 'END_DATETIME', 'SUBJECT', 'NOTES', 'RECIPIENT']] \
    .apply(lambda x: '\n\n'.join([': '.join(e) for e in zip(['Category', 'Start Date', 'End Date', 'Subject', 'Note', 'Recipient'], x) if e[1]]), axis=1)

reminder['entity_type'].unique()

# %% load to temp db

from common import vincere_activity
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
re1 = vincere_activity.transform_activities_temp(comment, conn_str_ddb, mylog)
re1 = re1.where(re1.notnull(), None)
re2 = vincere_activity.transform_activities_temp(reminder, conn_str_ddb, mylog)
re2 = re2.where(re2.notnull(), None)
dtype = {
    'company_id': sqlalchemy.types.INT,
    'contact_id': sqlalchemy.types.INT,
    'candidate_id': sqlalchemy.types.INT,
    'position_id': sqlalchemy.types.INT,
    'user_account_id': sqlalchemy.types.INT,
    'insert_timestamp': sqlalchemy.types.DateTime(),
    'content': sqlalchemy.types.NVARCHAR,
    'category': sqlalchemy.types.VARCHAR
}
re1.to_sql(con=engine_sqlite, name='vincere_activity', if_exists='replace', dtype=dtype, index=False)
re2.to_sql(con=engine_sqlite, name='vincere_activity', if_exists='append', dtype=dtype, index=False)

























