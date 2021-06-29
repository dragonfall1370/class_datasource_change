# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import time
import re
# from edenscott._edenscott_dtypes import *
import os
import psycopg2
import common.vincere_common as vincere_common
import common.vincere_custom_migration as vincere_custom_migration
import common.vincere_standard_migration as vincere_standard_migration
import sqlalchemy
import datetime
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
cf.read('yv_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)
# src_db = cf[cf['default'].get('src_db')]
sqlite_path = cf['default'].get('sqlite_path')
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
# conn_str_sdb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(src_db.get('user'), src_db.get('password'), src_db.get('server'), src_db.get('port'), src_db.get('database'))
# engine_postgre_src = sqlalchemy.create_engine(conn_str_sdb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)

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

# %% candidate
company_log = pd.read_sql("""
select c.idcompany as company_externalid
     , al.createdon
     , al.createdby
     , al.activitytype
     , al.subject
     , al.description
     , alct.value as log_type
     , u.useremail as owner
from activitylogentity ale
join company c on c.idcompany = ale.contextentityid
join activitylog al on al.idactivitylog = ale.idactivitylog
LEFT JOIN activitylogcontacttype alct ON alct.idactivitylogcontacttype = al.idactivitylogcontacttype
left join "user" u on u.fullname = al.createdby
WHERE ale.contextentitytype = 'Company'
""", engine_sqlite)
company_log = company_log.drop_duplicates()

contact_log = pd.read_sql("""
select cand.idperson as contact_externalid
     , ale.idcompany1 as company_externalid
     , al.createdon
     , al.createdby
     , al.activitytype
     , al.subject
     , al.description
     , t.startdate
     , t.completeddate
     , alct.value as log_type
     , al.progresstablename
     , cp.value as cand_progress
     , u.useremail as owner
from activitylogentity ale
join (select px.idperson
     , ROW_NUMBER() OVER(PARTITION BY c.idperson ORDER BY px.createdon DESC) rn
from candidate c
join (select personx.idperson, personx.createdon from personx where isdeleted = '0') px on c.idperson = px.idperson
) cand on cand.idperson = ale.idPerson
join activitylog al on al.idactivitylog = ale.idactivitylog
LEFT JOIN activitylogcontacttype alct ON alct.idactivitylogcontacttype = al.idactivitylogcontacttype
LEFT JOIN (select * from candidateprogress where isactive = '1') cp ON cp.idcandidateprogress = al.progressid
LEFT JOIN tasklog tl ON al.idactivitylog = tl.idactivitylog
LEFT JOIN task t ON tl.idtask = t.idtask
LEFT JOIN taskstatus ts ON t.idtaskstatus = ts.idtaskstatus
LEFT JOIN taskpriority tp ON t.idtaskpriority = tp.idtaskpriority
left join "user" u on u.fullname = al.createdby
where ActivityType <> 'AutoJournal'
and log_type in ('General Contact','Business Development','Client Contact')
""", engine_sqlite)
contact_log = contact_log.drop_duplicates()
contact_log.loc[contact_log['contact_externalid']=='dc81637d-463c-4b95-a2c4-a621e630215f']

job_log = pd.read_sql("""
select a.idassignment as job_externalid
     , al.createdon
     , al.createdby
     , al.activitytype
     , al.subject
     , al.description
     , t.startdate
     , t.completeddate
     , al.createdby
     , alct.value as log_type
     , al.progresstablename
     , cp.value as cand_progress
     , u.useremail as owner
from activitylogentity ale
join assignment a on a.idassignment = ale.contextentityid
join activitylog al on al.idactivitylog = ale.idactivitylog
LEFT JOIN activitylogcontacttype alct ON alct.idactivitylogcontacttype = al.idactivitylogcontacttype
LEFT JOIN (select * from candidateprogress where isactive = '1') cp ON cp.idcandidateprogress = al.progressid
LEFT JOIN tasklog tl ON al.idactivitylog = tl.idactivitylog
LEFT JOIN task t ON tl.idtask = t.idtask
LEFT JOIN taskstatus ts ON t.idtaskstatus = ts.idtaskstatus
LEFT JOIN taskpriority tp ON t.idtaskpriority = tp.idtaskpriority
left join "user" u on u.fullname = al.createdby
WHERE ale.contextentitytype IN ('Assignment', 'Flex')
and ActivityType <> 'AutoJournal'
""", engine_sqlite)
job_log = job_log.drop_duplicates()

# job_src = pd.read_sql("""
# select asrc.idAssignment as job_externalid, p.FullName, asp.Value, asrc.ContactedOn, asrc.ContactSubject, asrc.CreatedOn, u.useremail as owner
# from AssignmentSource asrc
# left join "user" u on u.fullname = asrc.createdby
# left join AssignmentSourceProgress asp on asp.idAssignmentSourceProgress = asrc.idAssignmentSourceProgress
# left join Person p on p.idPerson = asrc.idPerson
# """, engine_sqlite)
# job_src = job_src.drop_duplicates()

candidate_log = pd.read_sql("""
select cand.idperson as candidate_externalid
     , al.createdon
     , al.createdby
     , al.activitytype
     , al.subject
     , al.description
     , t.startdate
     , t.completeddate
     , alct.value as log_type
     , al.progresstablename
     , cp.value as cand_progress
     , u.useremail as owner
from activitylogentity ale
join (select px.idperson
     , ROW_NUMBER() OVER(PARTITION BY c.idperson ORDER BY px.createdon DESC) rn
from candidate c
join (select personx.idperson, personx.createdon from personx where isdeleted = '0') px on c.idperson = px.idperson
) cand on cand.idperson = ale.idPerson
join activitylog al on al.idactivitylog = ale.idactivitylog
LEFT JOIN activitylogcontacttype alct ON alct.idactivitylogcontacttype = al.idactivitylogcontacttype
LEFT JOIN (select * from candidateprogress where isactive = '1') cp ON cp.idcandidateprogress = al.progressid
LEFT JOIN tasklog tl ON al.idactivitylog = tl.idactivitylog
LEFT JOIN task t ON tl.idtask = t.idtask
LEFT JOIN taskstatus ts ON t.idtaskstatus = ts.idtaskstatus
LEFT JOIN taskpriority tp ON t.idtaskpriority = tp.idtaskpriority
left join "user" u on u.fullname = al.createdby
where ActivityType <> 'AutoJournal'
and log_type in ('General Contact','Assignment Candidate')
""", engine_sqlite)
candidate_log = candidate_log.drop_duplicates()
candidate_log.count()
# candidate_log.to_csv('candidate_log.csv',index=False)
assert False
# %%
company_log['Description'] = company_log['Description'].apply(lambda x: x.replace('\\x0d','\r') if x else x)
company_log['Description'] = company_log['Description'].apply(lambda x: x.replace('\\x0a','\n') if x else x)
company_log['insert_timestamp'] = pd.to_datetime(company_log['CreatedOn'])
company_log['content'] = company_log[['ActivityType','log_type', 'Subject', 'Description']]\
    .apply(lambda x: '\n'.join([': '.join([i for i in e if i]) for e in zip(['Type','Contact type', 'Subject', 'Description'], x) if e[1]]), axis=1)
company_log['company_external_id'] = company_log['company_externalid']

contact_log['Description'] = contact_log['Description'].apply(lambda x: x.replace('\\x0d','\r') if x else x)
contact_log['Description'] = contact_log['Description'].apply(lambda x: x.replace('\\x0a','\n') if x else x)
contact_log['progress'] = contact_log[['ProgressTableName', 'cand_progress']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
contact_log['insert_timestamp'] = pd.to_datetime(contact_log['CreatedOn'])
contact_log['content'] = contact_log[['ActivityType', 'log_type', 'progress', 'Subject', 'Description']]\
    .apply(lambda x: '\n'.join([': '.join([i for i in e if i]) for e in zip(['Type','Contact type', 'Progress', 'Subject', 'Description'], x) if e[1]]), axis=1)
contact_log['company_external_id'] = contact_log['company_externalid']
contact_log['contact_external_id'] = contact_log['contact_externalid']

job_log['Description'] = job_log['Description'].apply(lambda x: x.replace('\\x0d','\r') if x else x)
job_log['Description'] = job_log['Description'].apply(lambda x: x.replace('\\x0a','\n') if x else x)
job_log['progress'] = job_log[['ProgressTableName', 'cand_progress']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
job_log['insert_timestamp'] = pd.to_datetime(job_log['CreatedOn'])
job_log['content'] = job_log[['ActivityType', 'log_type', 'progress', 'Subject', 'Description']]\
    .apply(lambda x: '\n'.join([': '.join([i for i in e if i]) for e in zip(['Type','Contact type', 'Progress', 'Subject', 'Description'], x) if e[1]]), axis=1)
job_log['position_external_id'] = job_log['job_externalid']

candidate_log['Description'] = candidate_log['Description'].apply(lambda x: x.replace('\\x0d','\r') if x else x)
candidate_log['Description'] = candidate_log['Description'].apply(lambda x: x.replace('\\x0a','\n') if x else x)
candidate_log['progress'] = candidate_log[['ProgressTableName', 'cand_progress']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
candidate_log['insert_timestamp'] = pd.to_datetime(candidate_log['CreatedOn'])
candidate_log['content'] = candidate_log[['ActivityType', 'log_type', 'progress', 'Subject', 'Description']]\
    .apply(lambda x: '\n'.join([': '.join([i for i in e if i]) for e in zip(['Type','Contact type', 'Progress', 'Subject', 'Description'], x) if e[1]]), axis=1)
candidate_log['candidate_external_id'] = candidate_log['candidate_externalid']
assert False
# %% load db
from common import vincere_activity
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
re1 = vincere_activity.transform_activities_temp(company_log, conn_str_ddb, mylog)
re1 = re1.where(re1.notnull(), None)
re2 = vincere_activity.transform_activities_temp(contact_log, conn_str_ddb, mylog)
re2 = re2.where(re2.notnull(), None)
re4 = vincere_activity.transform_activities_temp(job_log, conn_str_ddb, mylog)
re4 = re4.where(re4.notnull(), None)
re5 = vincere_activity.transform_activities_temp(candidate_log, conn_str_ddb, mylog)
re5 = re5.where(re5.notnull(), None)

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
re4.to_sql(con=engine_sqlite, name='vincere_activity', if_exists='append', dtype=dtype, index=False)
re5.to_sql(con=engine_sqlite, name='vincere_activity', if_exists='append', dtype=dtype, index=False)
