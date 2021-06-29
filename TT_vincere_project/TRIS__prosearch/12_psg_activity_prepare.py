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
cf.read('psg_config.ini')
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
# %% company
company_log = pd.read_sql("""
select ClientID as company_external_id
     , ClientNoteDate
     , nullif(convert(varchar,ClientNoteSubject),'') as ClientNoteSubject
     , nullif(convert(nvarchar(max),ClientNoteDescription),'') as ClientNoteDescription
     , at.DisplayName as action_type
     , nullif(convert(varchar,EffectiveDate),'') as EffectiveDate
     , a.AMEmail as owner
from ClientNote cn
left join AM A on cn.AMID = A.AMID
left join tblActionType at on cn.ActionTypeID = at.ActionTypeID
""", engine_mssql)
company_log['company_external_id'] = company_log['company_external_id'].apply(lambda x: str(x) if x else x)
# tem1 = company_log.loc[company_log['owner'].str.contains('@')]
# tem2 = company_log.loc[~company_log['owner'].str.contains('@')]
# tem2['owner'] = tem2['owner'].apply(lambda x: x.lower().replace(' ',''))
# tem2['owner'] = tem2['owner']+'@email.com'
# company_log = pd.concat([tem1, tem2])

# %% contact
contact_log = pd.read_sql("""
select pn.PersonID as contact_external_id
     , ClientID as company_external_id
     , PersonNoteDate
     , nullif(convert(varchar,PersonNoteSubject),'') as PersonNoteSubject
     , nullif(convert(nvarchar(max),PersonNoteDescription),'') as PersonNoteDescription
     , at.DisplayName as action_type
     , nullif(convert(varchar,EffectiveDate),'') as EffectiveDate
     , a.AMEmail as owner
from PersonNote pn
join Contact c on c.PersonID = pn.PersonID
left join AM A on pn.AMID = A.AMID
left join tblActionType at on pn.ActionTypeID = at.ActionTypeID
""", engine_mssql)
contact_log = contact_log.where(contact_log.notnull(),None)
contact_log['company_external_id'] = contact_log['company_external_id'].apply(lambda x: str(x).split('.')[0] if x else x)
contact_log['contact_external_id'] = contact_log['contact_external_id'].apply(lambda x: str(x) if x else x)
# tem1 = contact_log.loc[contact_log['owner'].str.contains('@')]
# tem2 = contact_log.loc[~contact_log['owner'].str.contains('@')]
# tem2['owner'] = tem2['owner'].apply(lambda x: x.lower().replace(' ',''))
# tem2['owner'] = tem2['owner']+'@email.com'
# contact_log = pd.concat([tem1, tem2])

contact_mar = pd.read_sql("""
select tPMI.PersonID as contact_external_id
     , mi.*
     , A.AMEMail as owner
from tblPersonMarketingItem tPMI
join Contact c on c.PersonID = tPMI.PersonID
left join (select MarketingItemID
     , nullif(convert(nvarchar(max),MIName),'') as MIName
     , nullif(convert(nvarchar(max),MIComment),'') as MIComment
     , MIDate
     , tMIT.DisplayName as item_type
     , tMP.MPName from tblMarketingItem tma
left join tblMarketingItemType tMIT on tma.ItemTypeID = tMIT.MarketingItemTypeID
left join tblMarketingProject tMP on tma.MarketingProjectID = tMP.MarketingProjectID) mi on tPMI.MarketingItemID = mi.MarketingItemID
left join AM A on tPMI.AMID = A.AMID
""", engine_mssql)
contact_mar['contact_external_id'] = contact_mar['contact_external_id'].apply(lambda x: str(x) if x else x)
# tem1 = contact_mar.loc[contact_mar['owner'].str.contains('@')]
# tem2 = contact_mar.loc[~contact_mar['owner'].str.contains('@')]
# tem2['owner'] = tem2['owner'].apply(lambda x: x.lower().replace(' ',''))
# tem2['owner'] = tem2['owner']+'@email.com'
# contact_mar = pd.concat([tem1, tem2])

# %% job
job_log = pd.read_sql("""
select JobID as position_external_id
     , JobNoteDate
     , nullif(convert(varchar,JobNoteSubject),'') as JobNoteSubject
     , nullif(convert(nvarchar(max),JobNoteDescription),'') as JobNoteDescription
     , at.DisplayName as action_type
     , nullif(convert(varchar,EffectiveDate),'') as EffectiveDate
     , a.AMEmail as owner
from JobNote jn
left join AM A on jn.AMID = A.AMID
left join tblActionType at on jn.ActionTypeID = at.ActionTypeID
""", engine_mssql)
job_log['position_external_id'] = job_log['position_external_id'].apply(lambda x: str(x) if x else x)
# tem1 = job_log.loc[job_log['owner'].str.contains('@')]
# tem2 = job_log.loc[~job_log['owner'].str.contains('@')]
# tem2['owner'] = tem2['owner'].apply(lambda x: x.lower().replace(' ',''))
# tem2['owner'] = tem2['owner']+'@email.com'
# job_log = pd.concat([tem1, tem2])

# %% candidate
candidate_log = pd.read_sql("""
select pn.PersonID as candidate_external_id
     , PersonNoteDate
     , nullif(convert(varchar,PersonNoteSubject),'') as PersonNoteSubject
     , nullif(convert(nvarchar(max),PersonNoteDescription),'') as PersonNoteDescription
     , at.DisplayName as action_type
     , nullif(convert(varchar,EffectiveDate),'') as EffectiveDate
     , a.AMEmail as owner
from PersonNote pn
join Candidate c on c.PersonID = pn.PersonID
left join AM A on pn.AMID = A.AMID
left join tblActionType at on pn.ActionTypeID = at.ActionTypeID
""", engine_mssql)
candidate_log['candidate_external_id'] = candidate_log['candidate_external_id'].apply(lambda x: str(x) if x else x)
# tem1 = candidate_log.loc[candidate_log['owner'].str.contains('@')]
# tem2 = candidate_log.loc[~candidate_log['owner'].str.contains('@')]
# tem2['owner'] = tem2['owner'].apply(lambda x: x.lower().replace(' ',''))
# tem2['owner'] = tem2['owner']+'@email.com'
# candidate_log = pd.concat([tem1, tem2])

candidate_ref_check = pd.read_sql("""
select PersonID as candidate_external_id
     , nullif(convert(varchar,RefDate),'') as RefDate
     , nullif(convert(varchar,RefName),'') as RefName
     , nullif(convert(varchar,RefCompanyName),'') as RefCompanyName
     , nullif(convert(varchar,RefCompanyPhone),'') as RefCompanyPhone
     , nullif(convert(varchar,RefJobTitle),'') as RefJobTitle
     , nullif(convert(varchar,RefDuties),'') as RefDuties
     , nullif(convert(varchar,RefTechSkill),'') as RefTechSkill
     , nullif(convert(varchar,RefProbSolv),'') as RefProbSolv
     , nullif(convert(varchar,RefTeamWork),'') as RefTeamWork
     , nullif(convert(varchar,RefPressure),'') as RefPressure
     , nullif(convert(varchar,RefReliability),'') as RefReliability
     , nullif(convert(varchar,RefStrengths),'') as RefStrengths
     , nullif(convert(varchar,RefDevelopment),'') as RefDevelopment
     , nullif(convert(varchar,RefRehire),'') as RefRehire
     , nullif(convert(varchar,RefOther),'') as RefOther
     , nullif(convert(varchar,RefEmail),'') as RefEmail
     , rct.DisplayName as type
     , a.AMEmail as owner
from RefCheck rc
left join tblReferenceCheckType rct on rct.ID = rc.ReferenceCheckTypeID
left join AM A on rc.AMID = A.AMID
""", engine_mssql)
candidate_ref_check['candidate_external_id'] = candidate_ref_check['candidate_external_id'].apply(lambda x: str(x) if x else x)
# tem1 = candidate_ref_check.loc[candidate_ref_check['owner'].str.contains('@')]
# tem2 = candidate_ref_check.loc[~candidate_ref_check['owner'].str.contains('@')]
# tem2['owner'] = tem2['owner'].apply(lambda x: x.lower().replace(' ',''))
# tem2['owner'] = tem2['owner']+'@email.com'
# candidate_ref_check = pd.concat([tem1, tem2])

candidate_interview = pd.read_sql("""
select CandidateID as candidate_external_id
     , InternalInterviewDate
     , nullif(convert(nvarchar(max),InternalInterviewDescription),'') as InternalInterviewDescription 
     , a.AMEmail as owner
     , iit.DisplayName as type
from InternalInterview ii
left join AM A on ii.AMID = A.AMID
left join tblInternalInterviewType iit on ii.InternalInterviewTypeID = iit.ID
""", engine_mssql)
candidate_interview['candidate_external_id'] = candidate_interview['candidate_external_id'].apply(lambda x: str(x) if x else x)
# tem1 = candidate_interview.loc[candidate_interview['owner'].str.contains('@')]
# tem2 = candidate_interview.loc[~candidate_interview['owner'].str.contains('@')]
# tem2['owner'] = tem2['owner'].apply(lambda x: x.lower().replace(' ',''))
# tem2['owner'] = tem2['owner']+'@email.com'
# candidate_interview = pd.concat([tem1, tem2])

# %% manager task
manager_task = pd.read_sql("""
select CompanyID as company_external_id
     , ContactID as contact_external_id
     , CandidateID as candidate_external_id
     , JobID as position_external_id
     , Subject, CreatedDate
     , nullif(convert(nvarchar(max),mt.Description),'') as Description
     , mtt.Name as task_type
     , nullif(convert(varchar,mt.StartDate),'') as StartDate
     , nullif(convert(varchar,DueDate),'') as DueDate
     , nullif(convert(varchar,CompletedDate),'') as CompletedDate
     , nullif(convert(varchar,LevelOfCompleting),'') as LevelOfCompleting
     , Priority
     , Status
     , A1.AMName as manager
     , A2.AMEMail as owner
     , A3.AMName as completed
from ManagerTask mt
left join AM A1 on mt.ManagerID = A1.AMID
left join AM A2 on mt.CreatedByID = A2.AMID
left join AM A3 on mt.CompanyID = A3.AMID
left join ManagerTaskType mtt on mtt.ID = mt.ManagerTaskTypeID
""", engine_mssql)
manager_task = manager_task.where(manager_task.notnull(),None)
manager_task['company_external_id'] = manager_task['company_external_id'].apply(lambda x: str(x).split('.')[0] if x else x)
manager_task['contact_external_id'] = manager_task['contact_external_id'].apply(lambda x: str(x).split('.')[0] if x else x)
manager_task['candidate_external_id'] = manager_task['candidate_external_id'].apply(lambda x: str(x).split('.')[0] if x else x)
manager_task['position_external_id'] = manager_task['position_external_id'].apply(lambda x: str(x).split('.')[0] if x else x)
# tem1 = manager_task.loc[manager_task['owner'].str.contains('@')]
# tem2 = manager_task.loc[~manager_task['owner'].str.contains('@')]
# tem2['owner'] = tem2['owner'].apply(lambda x: x.lower().replace(' ',''))
# tem2['owner'] = tem2['owner']+'@email.com'
# manager_task = pd.concat([tem1, tem2])
assert False
# %%
company_log['insert_timestamp'] = pd.to_datetime(company_log['ClientNoteDate'])
company_log['content'] = company_log[['action_type','ClientNoteSubject', 'ClientNoteDescription','EffectiveDate']]\
    .apply(lambda x: '\n'.join([': '.join([i for i in e if i]) for e in zip(['Type','Subject', 'Description', 'Effective Date'], x) if e[1]]), axis=1)

contact_log['insert_timestamp'] = pd.to_datetime(contact_log['PersonNoteDate'])
contact_log['content'] = contact_log[['action_type','PersonNoteSubject', 'PersonNoteDescription','EffectiveDate']]\
    .apply(lambda x: '\n'.join([': '.join([i for i in e if i]) for e in zip(['Type','Subject', 'Description', 'Effective Date'], x) if e[1]]), axis=1)

# contact_mar['insert_timestamp'] = pd.to_datetime(contact_mar['MIDate'])
# contact_mar['content'] = contact_mar[['MPName','MIName', 'item_type','MIComment']]\
#     .apply(lambda x: '\n'.join([': '.join([i for i in e if i]) for e in zip(['Project','Name', 'Type', 'Comment'], x) if e[1]]), axis=1)

job_log['insert_timestamp'] = pd.to_datetime(job_log['JobNoteDate'])
job_log['content'] = job_log[['action_type','JobNoteSubject', 'JobNoteDescription','EffectiveDate']]\
    .apply(lambda x: '\n'.join([': '.join([i for i in e if i]) for e in zip(['Type','Subject', 'Description', 'Effective Date'], x) if e[1]]), axis=1)

candidate_log['insert_timestamp'] = pd.to_datetime(candidate_log['PersonNoteDate'])
candidate_log['content'] = candidate_log[['action_type','PersonNoteSubject', 'PersonNoteDescription','EffectiveDate']]\
    .apply(lambda x: '\n'.join([': '.join([i for i in e if i]) for e in zip(['Type','Subject', 'Description', 'Effective Date'], x) if e[1]]), axis=1)
candidate_log.to_csv('candidate_log.csv',index=False)

candidate_ref_check['insert_timestamp'] = pd.to_datetime(candidate_ref_check['RefDate'])
candidate_ref_check['content'] = candidate_ref_check[['type','RefName','RefCompanyName', 'RefCompanyPhone','RefJobTitle','RefDuties','RefTechSkill'
    , 'RefProbSolv','RefTeamWork','RefPressure','RefReliability'
    , 'RefStrengths','RefDevelopment','RefRehire','RefOther'
    , 'RefEmail']]\
    .apply(lambda x: '\n'.join([': '.join([i for i in e if i]) for e in zip(['Type','Ref Name','Ref Company Name', 'Ref Company Phone','Ref Job Title','Ref Duties','Ref Tech Skill'
    , 'Ref Problem Solve','Ref Team Work','Ref Pressure','Ref Reliability'
    , 'Ref Strengths','Ref Development','Ref Rehire','Ref Other'
    , 'Ref Email'], x) if e[1]]), axis=1)

candidate_interview['insert_timestamp'] = pd.to_datetime(candidate_interview['InternalInterviewDate'])
candidate_interview['content'] = candidate_interview[['type','InternalInterviewDescription']]\
    .apply(lambda x: '\n'.join([': '.join([i for i in e if i]) for e in zip(['Type','Description'], x) if e[1]]), axis=1)

manager_task['insert_timestamp'] = pd.to_datetime(manager_task['CreatedDate'])
manager_task['LevelOfCompleting'] = manager_task['LevelOfCompleting']+'%'
manager_task['content'] = manager_task[['Subject','Description', 'task_type','StartDate','DueDate','CompletedDate','LevelOfCompleting', 'manager', 'completed']]\
    .apply(lambda x: '\n'.join([': '.join([i for i in e if i]) for e in zip(['Subject','Description', 'Type', 'Start Date', 'Due Date', 'Completed Date', 'Level Of Completing', 'Manager', 'Completed'], x) if e[1]]), axis=1)
# %% load db
from common import vincere_activity
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
re1 = vincere_activity.transform_activities_temp(company_log, conn_str_ddb, mylog)
re1 = re1.where(re1.notnull(), None)
re2 = vincere_activity.transform_tasks_temp(contact_log, conn_str_ddb, mylog)
re2 = re2.where(re2.notnull(), None)
re3 = vincere_activity.transform_activities_temp(job_log, conn_str_ddb, mylog)
re3 = re3.where(re3.notnull(), None)
re4 = vincere_activity.transform_activities_temp(candidate_log, conn_str_ddb, mylog)
re4 = re4.where(re4.notnull(), None)
# re5 = vincere_activity.transform_activities_temp(contact_mar, conn_str_ddb, mylog)
# re5 = re5.where(re5.notnull(), None)
re6 = vincere_activity.transform_activities_temp(candidate_ref_check, conn_str_ddb, mylog)
re6 = re6.where(re6.notnull(), None)
re7 = vincere_activity.transform_activities_temp(candidate_interview, conn_str_ddb, mylog)
re7 = re7.where(re7.notnull(), None)
re8 = vincere_activity.transform_activities_temp(manager_task, conn_str_ddb, mylog)
re8 = re8.where(re8.notnull(), None)

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
# re5.to_sql(con=engine_sqlite, name='vincere_activity', if_exists='append', dtype=dtype, index=False)
re6.to_sql(con=engine_sqlite, name='vincere_activity', if_exists='append', dtype=dtype, index=False)
re7.to_sql(con=engine_sqlite, name='vincere_activity', if_exists='append', dtype=dtype, index=False)
re8.to_sql(con=engine_sqlite, name='vincere_activity', if_exists='append', dtype=dtype, index=False)

# re1.to_csv('re1.csv',index=False)
# re2.to_csv('re2.csv',index=False)
# re3.to_csv('re3.csv',index=False)
# re4.to_csv('re4.csv',index=False)
# re6.to_csv('re6.csv',index=False)
# re8.to_csv('re8.csv',index=False)
