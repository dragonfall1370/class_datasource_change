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
cf.read('ak_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)
src_db = cf[cf['default'].get('src_db')]
sqlite_path = cf['default'].get('sqlite_path')
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
conn_str_sdb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(src_db.get('user'), src_db.get('password'), src_db.get('server'), src_db.get('port'), src_db.get('database'))
engine_postgre_src = sqlalchemy.create_engine(conn_str_sdb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)

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
     , u.useremail as owner
from activitylogentity ale
join company c on c.idcompany = ale.contextentityid
join activitylog al on al.idactivitylog = ale.idactivitylog
left join "user" u on u.fullname = al.createdby
WHERE ale.contextentitytype = 'Company'
""", engine_postgre_src)

contact_log = pd.read_sql("""
select con.idperson as contact_externalid
     , ale.idcompany1 as company_externalid
     , al.createdon
     , al.createdby
     , al.activitytype
     , al.subject
     , al.description
     , alct.value
     , u.useremail as owner
from activitylogentity ale
join (select p.idperson, p.firstname, p.lastname, p.emailprivate, cont.idcompany, u.useremail
from personx p
join
(select cp.idperson, cp.idcompany,
       ROW_NUMBER() OVER(PARTITION BY cp.idperson
		ORDER BY cp.sortorder ASC, cp.employmentfrom DESC, cp.isdefaultrole desc, cp.isactiveemployment desc) rn
from company_person cp
join (select idcompany
from company) c on c.idcompany = cp.idcompany) cont on cont.idperson = p.idperson
left join "user" u on u.iduser = p.iduser
where cont.rn = 1
and p.isdeleted = '0') con on con.idperson = ale.contextentityid
join activitylog al on al.idactivitylog = ale.idactivitylog
LEFT JOIN activitylogcontacttype alct ON alct.idactivitylogcontacttype = al.idactivitylogcontacttype
left join "user" u on u.fullname = al.createdby
WHERE ale.contextentitytype = 'Person'
""", engine_postgre_src)

contact_project = pd.read_sql("""
select ctc.idperson as contact_externalid
	, ale.idcompany1 as company_externalid
	 , cc.createdon
     , c.campaigntitle
     , cc.contactedby
     , cc.contactsubject
     , c.campaigndescription
     , c.approximatenooftargets
     , c.campaignbudget
     , c.finalcost
     , c.createdby ,u.useremail as owner
     , c.modifiedby
     , c.modifiedon , c.campaignreference, cc.lastcontactedon, c.campaignno
from (select p.idperson, p.firstname, p.lastname, p.emailprivate, cont.idcompany, u.useremail
from personx p
join(select cp.idperson, cp.idcompany,
       ROW_NUMBER() OVER(PARTITION BY cp.idperson
		ORDER BY cp.sortorder ASC, cp.employmentfrom DESC, cp.isdefaultrole desc, cp.isactiveemployment desc) rn
from company_person cp
join (select idcompany
from company) c on c.idcompany = cp.idcompany) cont on cont.idperson = p.idperson
left join "user" u on u.iduser = p.iduser
where cont.rn = 1
and p.isdeleted = '0') ctc
JOIN (select * from campaigncontact where isexcluded = '0') cc on ctc.idperson = cc.idperson
JOIN campaign c on cc.idcampaign = c.idcampaign
join activitylogentity ale on ale.contextentityid = ctc.idperson
left join "user" u on u.fullname = c.createdby
WHERE 1=1
AND c.isdeleted = '0'
and ale.contextentitytype = 'Person'
""", engine_postgre_src)

job_log = pd.read_sql("""
select job.idassignment as job_externalid
     , al.createdon
     , al.createdby
     , al.activitytype
     , al.subject
     , al.description
     , t.startdate
     , t.completeddate
     , al.createdby
     , alct.value
     , al.progresstablename
     , cp.value
     , u.useremail as owner
from activitylogentity ale
join (select a.idassignment
     , a.assignmenttitle, contact.idperson, a.idcompany
     , u.useremail
from assignment a
left join (select * from
(select ac.idassignment
     , ac.idperson , comp_per.idcompany
     , ROW_NUMBER() OVER(PARTITION BY ac.idassignment ORDER BY ac.createdon DESC) rn
from assignmentcontact ac
join (select p.idperson, cont.idcompany
from personx p
join
(select cp.idperson, cp.idcompany,
       ROW_NUMBER() OVER(PARTITION BY cp.idperson
		ORDER BY cp.sortorder ASC, cp.employmentfrom DESC, cp.isdefaultrole desc, cp.isactiveemployment desc) rn
from company_person cp
join (select idcompany
from company) c on c.idcompany = cp.idcompany) cont on cont.idperson = p.idperson
where cont.rn = 1
and p.isdeleted = '0') comp_per on ac.idperson = comp_per.idperson) cont
where cont.rn = 1) contact on contact.idassignment = a.idassignment and contact.idcompany = a.idcompany
left join "user" u on u.iduser = a.iduser
where a.isdeleted = '0') job on job.idassignment = ale.contextentityid
join activitylog al on al.idactivitylog = ale.idactivitylog
LEFT JOIN activitylogcontacttype alct ON alct.idactivitylogcontacttype = al.idactivitylogcontacttype
LEFT JOIN (select * from candidateprogress where isactive = '1') cp ON cp.idcandidateprogress = al.progressid
LEFT JOIN tasklog tl ON al.idactivitylog = tl.idactivitylog
LEFT JOIN task t ON tl.idtask = t.idtask
LEFT JOIN taskstatus ts ON t.idtaskstatus = ts.idtaskstatus
LEFT JOIN taskpriority tp ON t.idtaskpriority = tp.idtaskpriority
left join "user" u on u.fullname = al.createdby
WHERE ale.contextentitytype IN ('Assignment', 'Flex')
""", engine_postgre_src)

candidate_log = pd.read_sql("""
select cand.idperson as candidate_externalid
     , al.createdon
     , al.createdby
     , al.activitytype
     , al.subject
     , al.description
     , t.startdate
     , t.completeddate
     , al.createdby
     , alct.value
     , al.progresstablename
     , cp.value
     , u.useremail as owner
from activitylogentity ale
join (select px.idperson
     , ROW_NUMBER() OVER(PARTITION BY c.idperson ORDER BY px.createdon DESC) rn
from candidate c
join (select personx.idperson, personx.createdon from personx where isdeleted = '0') px on c.idperson = px.idperson
) cand on cand.idperson = ale.contextentityid
join activitylog al on al.idactivitylog = ale.idactivitylog
LEFT JOIN activitylogcontacttype alct ON alct.idactivitylogcontacttype = al.idactivitylogcontacttype
LEFT JOIN (select * from candidateprogress where isactive = '1') cp ON cp.idcandidateprogress = al.progressid
LEFT JOIN tasklog tl ON al.idactivitylog = tl.idactivitylog
LEFT JOIN task t ON tl.idtask = t.idtask
LEFT JOIN taskstatus ts ON t.idtaskstatus = ts.idtaskstatus
LEFT JOIN taskpriority tp ON t.idtaskpriority = tp.idtaskpriority
left join "user" u on u.fullname = al.createdby
WHERE ale.contextentitytype = 'Person'
""", engine_postgre_src)

candidate_contract = pd.read_sql("""
select c.idcontract --activity_ext_id
	, c.idperson as candidate_externalid
	, f.idassignment as job_externalid
    , c.createdby
       , c.createdon
       , c.modifiedon
       , c.modifiedby
       , ct.value as type
       , cs.value as status
     , c.contractreference
     , c.contractjobtitle
     , c.contractstartdate
     , c.estimatedcontractenddate
     , c.contractextendedtodate
     , c.contracthoursperday
     , c.contractratenote
     , u.useremail as owner
from contract c
left join flex f on f.idflex = c.idflex
left join contracttype ct on ct.idcontracttype = c.idcontracttype
left join contractstatus cs on cs.idcontractstatus = c.idcontractstatus
left join "user" u on u.fullname = c.createdby
""", engine_postgre_src)

# %%
company_log['description'] = company_log['description'].apply(lambda x: x.replace('\\x0d\\x0a',', ') if x else x)
company_log['insert_timestamp'] = pd.to_datetime(company_log['createdon'])
company_log['content'] = company_log[['activitytype', 'subject', 'description']]\
    .apply(lambda x: '\n'.join([': '.join([i for i in e if i]) for e in zip(['Type', 'Subject', 'Description'], x) if e[1]]), axis=1)
company_log['company_external_id'] = company_log['company_externalid']

contact_log['description'] = contact_log['description'].apply(lambda x: x.replace('\\x0d\\x0a',', ') if x else x)
contact_log['insert_timestamp'] = pd.to_datetime(contact_log['createdon'])
contact_log['content'] = contact_log[['activitytype', 'subject', 'description']]\
    .apply(lambda x: '\n'.join([': '.join([i for i in e if i]) for e in zip(['Type', 'Subject', 'Description'], x) if e[1]]), axis=1)
contact_log['company_external_id'] = contact_log['company_externalid']
contact_log['contact_external_id'] = contact_log['contact_externalid']

contact_project['campaigndescription'] = contact_project['campaigndescription'].apply(lambda x: x.replace('\\x0d\\x0a',', ') if x else x)
contact_project['insert_timestamp'] = pd.to_datetime(contact_project['createdon'])
contact_project['content'] = contact_project[['campaigntitle', 'contactedby', 'contactsubject', 'campaigndescription'
    , 'approximatenooftargets', 'campaignbudget', 'finalcost', 'modifiedby', 'modifiedon', 'campaignreference', 'lastcontactedon', 'campaignno']]\
    .apply(lambda x: '\n'.join([': '.join([i for i in e if i]) for e in zip(['Title', 'Contacted by', 'Subject'
    , 'Description','Approximate','Budget', 'Final cost', 'Modified by', 'Modified date', 'Reference', 'Last Contact', 'Number'], x) if e[1]]), axis=1)
contact_project['company_external_id'] = contact_project['company_externalid']
contact_project['contact_external_id'] = contact_project['contact_externalid']

job_log['description'] = job_log['description'].apply(lambda x: x.replace('\\x0d\\x0a',', ') if x else x)
job_log['insert_timestamp'] = pd.to_datetime(job_log['createdon'])
job_log['content'] = job_log[['activitytype', 'subject', 'description']]\
    .apply(lambda x: '\n'.join([': '.join([i for i in e if i]) for e in zip(['Type', 'Subject', 'Description'], x) if e[1]]), axis=1)
job_log['position_external_id'] = job_log['job_externalid']

candidate_log['description'] = candidate_log['description'].apply(lambda x: x.replace('\\x0d\\x0a',', ') if x else x)
candidate_log['insert_timestamp'] = pd.to_datetime(candidate_log['createdon'])
candidate_log['content'] = candidate_log[['activitytype', 'subject', 'description']]\
    .apply(lambda x: '\n'.join([': '.join([i for i in e if i]) for e in zip(['Type', 'Subject', 'Description'], x) if e[1]]), axis=1)
candidate_log['candidate_external_id'] = candidate_log['candidate_externalid']

candidate_contract['insert_timestamp'] = pd.to_datetime(candidate_contract['createdon'])
candidate_contract['content'] = candidate_contract[['contractjobtitle', 'type', 'status', 'contractstartdate', 'estimatedcontractenddate', 'contracthoursperday', 'contractratenote', 'contractreference']]\
    .apply(lambda x: '\n'.join([': '.join([i for i in e if i]) for e in zip(['Title', 'Type', 'Status', 'Start date', 'End date', 'Hours per day', 'Note', 'Reference'], x) if e[1]]), axis=1)
candidate_contract['candidate_external_id'] = candidate_contract['candidate_externalid']
candidate_contract['position_external_id'] = candidate_contract['job_externalid']

assert False
# %% load db
from common import vincere_activity
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
re1 = vincere_activity.transform_activities_temp(company_log, conn_str_ddb, mylog)
re1 = re1.where(re1.notnull(), None)
re2 = vincere_activity.transform_activities_temp(contact_log, conn_str_ddb, mylog)
re2 = re2.where(re2.notnull(), None)
re3 = vincere_activity.transform_activities_temp(contact_project, conn_str_ddb, mylog)
re3 = re3.where(re3.notnull(), None)
re4 = vincere_activity.transform_activities_temp(job_log, conn_str_ddb, mylog)
re4 = re4.where(re4.notnull(), None)
re5 = vincere_activity.transform_activities_temp(candidate_log, conn_str_ddb, mylog)
re5 = re5.where(re5.notnull(), None)
re6 = vincere_activity.transform_activities_temp(candidate_contract, conn_str_ddb, mylog)
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
    'type': sqlalchemy.types.VARCHAR
}
re1.to_sql(con=engine_sqlite, name='vincere_activity', if_exists='replace', dtype=dtype, index=False)
re2.to_sql(con=engine_sqlite, name='vincere_activity', if_exists='append', dtype=dtype, index=False)
re3.to_sql(con=engine_sqlite, name='vincere_activity', if_exists='append', dtype=dtype, index=False)
re4.to_sql(con=engine_sqlite, name='vincere_activity', if_exists='append', dtype=dtype, index=False)
re5.to_sql(con=engine_sqlite, name='vincere_activity', if_exists='append', dtype=dtype, index=False)
re6.to_sql(con=engine_sqlite, name='vincere_activity', if_exists='append', dtype=dtype, index=False)