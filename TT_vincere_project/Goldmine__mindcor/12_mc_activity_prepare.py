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
cf.read('mc_config.ini')
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
# assert False
# %% pending
pending = pd.read_sql("""
with pend as(
select h.ACCOUNTNO, com.ID as company_externalid, u.EMC_ACC_EMAILS, REF, ACTVCODE,CONCAT_WS(' ',cast(substring(cast(CREATEON as varchar(max)),0,12) as date),CREATEAT) as date, concat('pending_',rn,'.',EXT) as file_name
from pending_html h
left join USERS u on u.USERNAME = h.CREATEBY
left join (select ACCOUNTNO, p.ID
from CONTACT1 c
left join company p on p.COMPANY = c.COMPANY
where p.ID is not null) com on h.ACCOUNTNO = com.ACCOUNTNO)
select * from pend where nullif(date,'') is not null and nullif(ACCOUNTNO,'') is not null
""", engine_mssql)
pending = pending.drop_duplicates()

pend_arr=[]
for i in range(32):
    print(i)
    sql = """select * from parse_pending_html_"""+str(i)
    pending_content = pd.read_sql(sql, engine_sqlite)
    pending_content = pending_content.drop_duplicates()
    pend_arr.append(pending_content)
pending_content = pd.concat(pend_arr)
pending_content = pending_content.drop_duplicates()
pending = pending.merge(pending_content, left_on='file_name', right_on='file', how='left')

# %% company
log = pd.read_sql("""
select m.ACCOUNTNO, u.EMC_ACC_EMAILS, CREATEON, MAILREF as REF, file_name,concat_ws(' ',convert(date,MAILDATE), MAILTIME) as date
from MAILBOX m
join (select LINKRECID, concat('email_',rn,'.',EXT) as file_name from activity_file where nullif(ACCOUNTNO,'') is not null and EXT is not null and nullif(LINKRECID,'') is not null) a on m.LINKRECID = a.LINKRECID
left join USERS u on u.USERNAME = m.USERID
-- where m.ACCOUNTNO = 'B4060250053)2E5A@Bar'

where nullif(m.ACCOUNTNO,'') is not null;
""", engine_mssql)
log = log.drop_duplicates()

emai_arr=[]
for i in range(1477):
    print(i)
    sql = """select file_name, emailContent from eml_parsed"""+str(i)
    email = pd.read_sql(sql, engine_sqlite)
    email = email.drop_duplicates()
    emai_arr.append(email)
email = pd.concat(emai_arr)
email = email.drop_duplicates()
ltem = log.merge(email,on='file_name')
ltem = ltem.loc[ltem['emailContent'].notnull()]
assert False
# %%
ltem.loc[(ltem['date'].str.contains('1999-11-30 Øñ0	Øñ0')), 'date'] = ltem['CREATEON']


ltem['date'] = ltem['date'].apply(lambda x: x.replace('am',':00'))
ltem['date'] = ltem['date'].apply(lambda x: x.replace('.',':'))
ltem['date'] = ltem['date'].apply(lambda x: x.replace('::',':'))
# ltem['date'] = ltem['date'].apply(lambda x: x.replace('14.53','14:53:00'))
# ltem['date'] = ltem['date'].apply(lambda x: x.replace('15.21','15:21:00'))
ltem.loc[ltem['date'].str.contains('16.45')]
ltem['insert_timestamp'] = pd.to_datetime(ltem['date'], format='%Y/%m/%d %H:%M:%S')
# ltem.loc[ltem['date'].str.contains('1999-11-30 Øñ0	Øñ0')]

ltem['owner'] = ltem['EMC_ACC_EMAILS']
ltem['content'] = ltem[['REF','emailContent']].apply(lambda x: '\n'.join([': '.join([i for i in e if i]) for e in zip(['REF',''], x) if e[1]]), axis=1)

tem1 = ltem.copy()
tem2 = ltem.copy()

tem1['contact_external_id'] = tem1['ACCOUNTNO']
# tem1['company_external_id'] = tem1['company_externalid']
tem2['candidate_external_id'] = tem2['ACCOUNTNO']


pending.loc[(pending['date'].str.contains('1999-11-30 ¨2š¨')), 'date'] = '1999-11-30'
pending.loc[(pending['date'].str.contains('1999-11-30 jKEY')), 'date'] = '1999-11-30'
pending.loc[(pending['date'].str.contains('1999-11-30 ²L')), 'date'] = '1999-11-30'
pending.loc[(pending['date'].str.contains('1999-11-30 ò&URC')), 'date'] = '1999-11-30'
# pending.loc[pending['date'].str.contains('1999-11-30')]

pending['insert_timestamp'] = pd.to_datetime(pending['date'], format='%Y-%m-%d %H:%M:%S')
pending['owner'] = pending['EMC_ACC_EMAILS']
pending['ACTVCODE'] = pending['ACTVCODE'].apply(lambda x: str(x) if x else x)
pending['REF'] = pending['REF'].apply(lambda x: str(x) if x else x)
pending['subject'] = pending['REF']
pending['content'] = pending['content'].apply(lambda x: str(x) if x else x)
pending['content'] = pending[['ACTVCODE','REF', 'content']].apply(lambda x: '\n'.join([': '.join([i for i in e if i]) for e in zip(['CODE','Reference',''], x) if e[1]]), axis=1)
pending['content'] = '---PENDING---\n'+pending['content']
tem1_pending = pending.copy()
tem2_pending = pending.copy()
tem1_pending['contact_external_id'] = tem1_pending['ACCOUNTNO']
# tem1_pending['company_external_id'] = tem1_pending['company_externalid']
tem2_pending['candidate_external_id'] = tem2_pending['ACCOUNTNO']


# %% load db
from common import vincere_activity
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
re1 = vincere_activity.transform_activities_temp(tem1, conn_str_ddb, mylog)
re1 = re1.where(re1.notnull(), None)
re2 = vincere_activity.transform_activities_temp(tem2, conn_str_ddb, mylog)
re2 = re2.where(re2.notnull(), None)

re3 = vincere_activity.transform_activities_temp(tem1_pending, conn_str_ddb, mylog)
re3 = re3.where(re3.notnull(), None)
re4 = vincere_activity.transform_activities_temp(tem2_pending, conn_str_ddb, mylog)
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
re1.to_sql(con=engine_sqlite, name='vincere_activity', if_exists='replace', dtype=dtype, index=False)
re2.to_sql(con=engine_sqlite, name='vincere_activity', if_exists='append', dtype=dtype, index=False)
re3.to_sql(con=engine_sqlite, name='vincere_activity', if_exists='append', dtype=dtype, index=False)
re4.to_sql(con=engine_sqlite, name='vincere_activity', if_exists='append', dtype=dtype, index=False)

# %% activity
# activity = pd.read_sql("""
# select id, company_id, contact_id, content, insert_timestamp, user_account_id from activity where company_id is not null and contact_id is not null
# """, engine_postgre_review)
# activity['matcher'] = activity['content'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
# re3['matcher'] = re3['content'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
# tem = activity.merge(re3, on=['company_id','contact_id','matcher','insert_timestamp'])
# tem2 = tem[['id','position_id']].drop_duplicates()
# vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, connection, ['position_id', ], ['id', ], 'activity', mylog)