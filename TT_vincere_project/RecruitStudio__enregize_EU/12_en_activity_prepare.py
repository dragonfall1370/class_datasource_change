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
cf.read('en_config.ini')
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
# %% candidate
company_log = pd.read_sql("""
select l.ItemId as company_external_id
     , nullif(l.Subject,'') as Subject
     , nullif(l.ShortUser,'') as ShortUser
     , l.LogDate
     , nullif(u.Email,'') as owner
     , nullif(ld.Text,'') as Text
from LogItems l
join Companies c on c.CompanyId = l.ItemId
left join Users u on l.UserId = u.UserId
left join LogData ld on l.LogDataId = ld.LogDataId
""", engine_mssql)
company_log = company_log.drop_duplicates()
company_log['company_external_id'] = 'EUK'+company_log['company_external_id']

contact_log1 = pd.read_sql("""
select * from contact_activitites_tmp
where logdate between '2006-09-20 11:01:50' and '2013-12-31 23:59:59'
""", engine_mssql)
contact_log2 = pd.read_sql("""
select * from contact_activitites_tmp
where logdate between '2014-01-01 00:00:00' and '2015-12-31 23:59:59'
""", engine_mssql)
contact_log3 = pd.read_sql("""
select * from contact_activitites_tmp
where logdate between '2016-01-01 00:00:00' and '2016-12-31 23:59:59'
""", engine_mssql)
contact_log4 = pd.read_sql("""
select * from contact_activitites_tmp
where logdate between '2017-01-01 00:00:00' and '2017-12-31 23:59:59'
""", engine_mssql)
contact_log5 = pd.read_sql("""
select * from contact_activitites_tmp
where logdate between '2018-01-01 00:00:00' and '2018-12-31 23:59:59'
""", engine_mssql)
contact_log6 = pd.read_sql("""
select * from contact_activitites_tmp
where logdate between '2019-01-01 00:00:00' and '2019-12-31 23:59:59'
""", engine_mssql)
contact_log7 = pd.read_sql("""
select * from contact_activitites_tmp
where logdate between '2020-01-01 00:00:00' and '2020-12-31 23:59:59'
""", engine_mssql)
contact_log = pd.concat([contact_log1,contact_log2,contact_log3,contact_log4,contact_log5,contact_log6,contact_log7])
contact_log = contact_log.drop_duplicates()
contact_log['contact_external_id'] = 'EUK'+contact_log['contact_external_id']
contact_log['company_external_id'] = 'EUK'+contact_log['company_external_id']

job_log = pd.read_sql("""
select l.ItemId as position_external_id
     , nullif(l.Subject,'') as Subject
     , nullif(l.ShortUser,'') as ShortUser
     , l.LogDate
     , nullif(u.Email,'') as owner
     , nullif(ld.Text,'') as Text
from LogItems l
join Vacancies v on v.JobNumber = l.ItemId
left join Users u on l.UserId = u.UserId
left join LogData ld on l.LogDataId = ld.LogDataId
""", engine_mssql)
job_log = job_log.drop_duplicates()
job_log['position_external_id'] = 'EUK'+job_log['position_external_id']

candidate_log1 = pd.read_sql("""
select *
from dbo.candidate_activitites_tmp
where candidate_activitites_tmp.logdate between '2006-09-14 00:00:00' and '2012-12-31 23:59:59'
""", engine_mssql)
candidate_log2 = pd.read_sql("""
select *
from dbo.candidate_activitites_tmp
where candidate_activitites_tmp.logdate between '2013-01-01 00:00:00' and '2014-12-31 23:59:59'
""", engine_mssql)
candidate_log3 = pd.read_sql("""
select *
from dbo.candidate_activitites_tmp
where candidate_activitites_tmp.logdate between '2015-01-01 00:00:00' and '2015-12-31 23:59:59'
""", engine_mssql)
candidate_log4 = pd.read_sql("""
select *
from dbo.candidate_activitites_tmp
where candidate_activitites_tmp.logdate between '2016-01-01 00:00:00' and '2016-12-31 23:59:59'
""", engine_mssql)
candidate_log5 = pd.read_sql("""
select *
from dbo.candidate_activitites_tmp
where candidate_activitites_tmp.logdate between '2017-01-01 00:00:00' and '2017-12-31 23:59:59'
""", engine_mssql)
candidate_log6 = pd.read_sql("""
select *
from dbo.candidate_activitites_tmp
where candidate_activitites_tmp.logdate between '2018-01-01 00:00:00' and '2018-12-31 23:59:59'
""", engine_mssql)
candidate_log7 = pd.read_sql("""
select *
from dbo.candidate_activitites_tmp
where candidate_activitites_tmp.logdate between '2019-01-01 00:00:00' and '2019-12-31 23:59:59'
""", engine_mssql)
candidate_log8 = pd.read_sql("""
select *
from dbo.candidate_activitites_tmp
where candidate_activitites_tmp.logdate between '2020-01-01 00:00:00' and '2020-12-31 23:59:59'
""", engine_mssql)
candidate_log = pd.concat([candidate_log1,candidate_log2,candidate_log3,candidate_log4,candidate_log5,candidate_log6,candidate_log7,candidate_log8])
candidate_log = candidate_log.drop_duplicates()
candidate_log['candidate_external_id'] = 'EUK'+candidate_log['candidate_external_id']
candidate_log['company_external_id'] = candidate_log['company_external_id'].apply(lambda x:'EUK'+x.strip() if x else x)
assert False
# %%
company_log['insert_timestamp'] = pd.to_datetime(company_log['LogDate'])
company_log['content'] = company_log[['ShortUser','Subject', 'Text']]\
    .apply(lambda x: '\n'.join([': '.join([i for i in e if i]) for e in zip(['Short user','Subject', 'Log'], x) if e[1]]), axis=1)

contact_log['insert_timestamp'] = pd.to_datetime(contact_log['LogDate'])
contact_log['content'] = contact_log[['ShortUser','Subject', 'Text']]\
    .apply(lambda x: '\n'.join([': '.join([i for i in e if i]) for e in zip(['Short user','Subject', 'Log'], x) if e[1]]), axis=1)

job_log['insert_timestamp'] = pd.to_datetime(job_log['LogDate'])
job_log['content'] = job_log[['ShortUser','Subject', 'Text']]\
    .apply(lambda x: '\n'.join([': '.join([i for i in e if i]) for e in zip(['Short user','Subject', 'Log'], x) if e[1]]), axis=1)

candidate_log['insert_timestamp'] = pd.to_datetime(candidate_log['LogDate'])
candidate_log['content'] = candidate_log[['ShortUser','Subject', 'Text']]\
    .apply(lambda x: '\n'.join([': '.join([i for i in e if i]) for e in zip(['Short user','Subject', 'Log'], x) if e[1]]), axis=1)

# %% load db
from common import vincere_activity
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
re1 = vincere_activity.transform_activities_temp(company_log, conn_str_ddb, mylog)
re1 = re1.where(re1.notnull(), None)
re2 = vincere_activity.transform_activities_temp(contact_log, conn_str_ddb, mylog)
re2 = re2.where(re2.notnull(), None)
re3 = vincere_activity.transform_activities_temp(job_log, conn_str_ddb, mylog)
re3 = re3.where(re3.notnull(), None)
re4 = vincere_activity.transform_activities_temp(candidate_log, conn_str_ddb, mylog)
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
