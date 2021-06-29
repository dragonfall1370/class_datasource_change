# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import time
import re
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
cf.read('exede_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
sqlite_path = cf['default'].get('sqlite_path')
src_db = cf[cf['default'].get('src_db')]
mylog = log.get_info_logger(log_file)

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
ddbconn = psycopg2.connect(host=dest_db.get('server'), user=dest_db.get('user'), password=dest_db.get('password'), database=dest_db.get('database'), port=dest_db.get('port'))
ddbconn.set_client_encoding('UTF8')
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')

# %%
jobapp = pd.read_sql("""

 -- prospect to shortlisted
  select
 	p.JOB_ID as job_externalid 
 	, p.APP_ID as candidate_externalid 
 	, 'shortlisted' as Stage
 	--SHORTLISTED
 	,	case
 			when p.STATUS_ID = 9 then 'Not Interested'
 			when p.STATUS_ID = 11 then 'Not Suitable'
 			when p.STATUS_ID = 15 then 'Chase'
 			when p.STATUS_ID = 13 then 'Called for Referrals'
 			when p.STATUS_ID = 10 then 'Competitor Contacted'
 			else NULL
 		end as SubStatus 
 	, p.DATE_ADDED as ActionedDate
 from Prospects p
 left join Applicants a on	p.APP_ID = a.APP_ID
 left join Requirements r on	r.JOB_ID = p.JOB_ID
 where p.PROSPECT_ID > 1330
 -------------------------------------------------------------------
 union 
 -------------------------------------------------------------------
 -- rejected/declined to shortlisted       
  select
 	c.JOB_ID as job_externalid 
 	, c.APP_ID as candidate_externalid 
 	, 'shortlisted > rejected' as Stage 
 	, case
 		when c.STATUS = 0 then 'Sent'
 		when c.STATUS = 5 then 'Declined'
 		when c.STATUS = 4 then 'Rejected'
 		when c.STATUS = -1 then 'Not Actioned'
 		else NULL
 	end as SubStatus 
 	, c.DATE_SENT as ActionedDate
 from CVsSent c
 left join Applicants a on	c.APP_ID = a.APP_ID
 left join Requirements r on	c.JOB_ID = r.JOB_ID
 where	c.STATUS in (4,	5)
 and c.CVSENT_ID > 'HQ00000645'
 -------------------------------------------------------------------
 union 
 -------------------------------------------------------------------
-- cv sent/awaiting interview to sent
 select
	c.JOB_ID as job_externalid 
	, c.APP_ID as candidate_externalid 
	, 'sent' as Stage 
	, case
	/* Status: Sent 0 | Awaiting Interview 3 | Interview 6 | Under Offer 1 | Placed 2 | Rejected 4 | Declined 5 | Not Actioned -1 */
		when c.STATUS = 0 then 'Sent'
		when c.STATUS = 5 then 'Declined'
		when c.STATUS = 4 then 'Rejected'
		when c.STATUS = -1 then 'Not Actioned'
		else concat('', c.STATUS)
	end as SubStatus 
	, c.DATE_SENT as ActionedDate
from CVsSent c
left join Applicants a on c.APP_ID = a.APP_ID
left join Requirements r on c.JOB_ID = r.JOB_ID
where c.STATUS not in (4,	5)
and c.CVSENT_ID>'HQ00000690'
-------------------------------------------------------------------
union 
-------------------------------------------------------------------
--first interview to first_interview
select 
	i.JOB_ID as job_externalid
	, c.APP_ID as candidate_externalid
	, '1ST_INTERVIEW' as Stage
	, 'Interview' as SubStatus
	, i.ARRANGE_ON as ActionedDate
from Interviews i
left join CVsSent c on c.CVSENT_ID = i.CVSENT_ID
left join Applicants a on c.APP_ID = a.APP_ID
left join Requirements r on i.JOB_ID = r.JOB_ID
where i.NUM<=1
and i.INTERVIEW_ID >191
-------------------------------------------------------------------
union 
-------------------------------------------------------------------
--first interview to first_interview rejected
select 
	i.JOB_ID as job_externalid
	, c.APP_ID as candidate_externalid
	, '1ST_INTERVIEW > rejected' as Stage
	, 'Interview' as SubStatus
	, i.ARRANGE_ON as ActionedDate
from Interviews i
left join CVsSent c on c.CVSENT_ID = i.CVSENT_ID
left join Applicants a on c.APP_ID = a.APP_ID
left join Requirements r on i.JOB_ID = r.JOB_ID
where i.NUM<=1 and c.STATUS in (4, 5)
-------------------------------------------------------------------
union 
-------------------------------------------------------------------
--second interview to second_interview
select 
	i.JOB_ID as job_externalid
	, c.APP_ID as candidate_externalid
	, '2ND_INTERVIEW' as Stage
	, 'Interview' as SubStatus
	, i.ARRANGE_ON as ActionedDate
from Interviews i
left join CVsSent c on c.CVSENT_ID = i.CVSENT_ID
left join Applicants a on c.APP_ID = a.APP_ID
left join Requirements r on i.JOB_ID = r.JOB_ID
where i.NUM>1
and i.INTERVIEW_ID > 183
-------------------------------------------------------------------
union 
-------------------------------------------------------------------
--second interview to second_interview
select 
	i.JOB_ID as job_externalid
	, c.APP_ID as candidate_externalid
	, '2ND_INTERVIEW > rejected' as Stage
	, 'Interview' as SubStatus
	, i.ARRANGE_ON as ActionedDate
from Interviews i
left join CVsSent c on c.CVSENT_ID = i.CVSENT_ID
left join Applicants a on c.APP_ID = a.APP_ID
left join Requirements r on i.JOB_ID = r.JOB_ID
where i.NUM>1 and c.STATUS in (4, 5)
-------------------------------------------------------------------
union 
-------------------------------------------------------------------
-- offer to offer
select 
        c.JOB_ID as job_externalid
        , c.APP_ID as candidate_externalid
        , 'offer' as Stage
        , 'JobOffers' as SubStatus
        , j.DATE_TIME as ActionedDate
        from JobOffers j
        left join CVsSent c on c.CVSENT_ID = j.CVSENT_ID
        left join Applicants a on c.APP_ID = a.APP_ID
        left join Requirements r on c.JOB_ID = r.JOB_ID
        where j.CVSENT_ID is not NULL and c.JOB_ID is not NULL
-------------------------------------------------------------------
union 
-------------------------------------------------------------------
-- under offer to offer
select 
        c.JOB_ID as job_externalid
        , c.APP_ID as candidate_externalid
        , 'offer' as Stage 
        , 'Under Offer' as SubStatus
        , c.DATE_SENT as ActionedDate
        from CVsSent c
        left join Applicants a on c.APP_ID = a.APP_ID
        left join Requirements r on c.JOB_ID = r.JOB_ID
        where c.STATUS in (1) --Under Offer
-------------------------------------------------------------------
union 
-------------------------------------------------------------------       
-- place to place
select 
        p.JOB_ID as job_externalid
        , p.APP_ID as candidate_externalid
        , 'place' as Stage --PLACED stage
        , 'Placed' as SubStatus
        , p.CREATED_ON as ActionedDate 
    from NewPlacements p--Placed stage
    left join Applicants a on p.APP_ID = a.APP_ID
    left join Requirements r on p.JOB_ID = r.JOB_ID
    where p.ID>'HQ00000051'
""", engine_mssql)

# %% mapping stage
# assert False
# tem1 = pd.DataFrame(jobapp['application_stage'].value_counts().keys(), columns=['application_stage'])
# tem1['matcher'] = tem1['application_stage'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
# tem2 = pd.read_csv('jobapp_stage_mapping.csv')
# tem2['matcher'] = tem2['application_stage'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
# tem3 = tem1.merge(tem2, on='matcher', suffixes=['', '_y'])
# tem4 = jobapp.merge(tem3, on='application_stage')
tem4 = jobapp[['job_externalid', 'candidate_externalid', 'Stage', 'ActionedDate']]\
    .rename(columns={'ActionedDate': 'CreatedDate', 'Stage': 'Vincere Stage', 'job_externalid': 'jobid', 'candidate_externalid': 'candidateid'})
# assert False
# %%
tem5 = tem4[['candidateid', 'jobid', 'CreatedDate', 'Vincere Stage']]
tem5['Vincere Stage'].value_counts()
tem5['application-stage'] = tem5['Vincere Stage']

from common import vincere_job_application
ja = vincere_job_application.JobApplication(ddbconn)
# ja.jobapp_map_only(tem5)
# tem5['stage'].unique()
# tem5.loc[tem5.stage==-1]

# assert False

tem5['application-positionExternalId'] = tem5['jobid']
tem5['application-candidateExternalId'] = tem5['candidateid']
tem5['application-actionedDate'] = tem5['CreatedDate']
jobapp_result = ja.process_jobapp_v2(tem5)

jobapp_result.loc[jobapp_result['application-positionExternalId'] == 'HQ00005154']
# ck = tem5.loc[tem5['application-positionExternalId'] == 'HQ00005154']
# ja.process_jobapp_v2(ck)
# jobapp.head()

# %% job application separate to: not placement and placement
df_jobapplication_placement = jobapp_result[jobapp_result['application-stage'].str.contains('PLACEMENT')]
df_jobapplication_placement['application-stage-orgi'] = df_jobapplication_placement['application-stage']
df_jobapplication_placement['application-stage'] = 'OFFERED'
df_jobapplication_other = jobapp_result[~jobapp_result['application-stage'].str.contains('PLACEMENT')]

df_jobapplication_placement['application-actionedDate'] = df_jobapplication_placement['application-actionedDate'].map(lambda x: str(x)[:10])
df_jobapplication_other['application-actionedDate'] = df_jobapplication_other['application-actionedDate'].map(lambda x: str(x)[:10])

df_jobapplication_placement.to_csv(os.path.join(standard_file_upload, '7_jobapplication_placement.csv'), index=False)
df_jobapplication_other.to_csv(os.path.join(standard_file_upload, '7_jobapplication_other.csv'), index=False)
