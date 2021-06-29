# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import datetime
import re
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
cf.read('theo_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
src_db = cf[cf['default'].get('src_db')]
dest_db = cf[cf['default'].get('dest_db')]
sqlite_path = cf['default'].get('sqlite_path')

mylog = log.get_info_logger(log_file)
# assert False
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)
csv_path = r""

# %% connect data
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')

from common import vincere_job
vjob = vincere_job.Job(engine_postgre.raw_connection())

# %% job
job = pd.read_sql("""
select 
job.jobPostingID as job_externalid
, cont.clientID as contact_externalid
, com.company_externalid
, job.title as job_title
, job.dateAdded as reg_date
, job.address
, job.city
, job.clientBillRate
, c.COUNTRY
, job.customFloat1
, job.customText1
, job.customText2
, job.customText3
, job.dateEnd
, job.description
, job.employmentType
, job.feeArrangement
, job.isOpen
, job.markupPercentage
, job.numOpenings
, job.payRate
, job.publicDescription
, job.customInt3
-- , job.reportToUserID
, concat(cont2.firstName, ' ', cont2.lastName) as reportToUserID
, job.salary
, job.salaryUnit
, job.skills
, job.[source]
, job.startDate
, job.state
, job.status
, job.title
, job.zip
from bullhorn1.BH_JobPosting job
join (
	select 
	com.clientCorporationID as company_externalid
	, com.name as company_name
	, com.dateAdded as reg_date
	from bullhorn1.BH_ClientCorporation com
	join bullhorn1.BH_Department de on com.departmentID = de.departmentID
	where com.status != 'Archive' 
	and de.name = 'Theo James Recruitment Limited'
) com on  job.clientcorporationid = com.company_externalid
left join (
	select userID, clientcorporationid, isdeleted, max(clientID) as clientID 
	from bullhorn1.BH_Client where isdeleted <> 1 and status <> 'Archive' 
	group by userID, clientcorporationid, isdeleted) cont 
	on (job.clientUserID = cont.userID and job.clientcorporationid = cont.clientcorporationid)
left join tmp_country c on job.countryID = c.CODE
left join bullhorn1.BH_UserContact cont2 on job.reportToUserID = cont2.userID
where job.isDeleted <> 1 and job.status != 'Archive'
""", engine_mssql)

job = job.where(job.notnull(), None)
job.job_externalid = job.job_externalid.astype(str)
job.contact_externalid = job.contact_externalid.map(lambda x: str(x) if x else x)
job.contact_externalid = job.contact_externalid.apply(lambda x: x.split('.')[0] if x else x)
job.company_externalid = job.company_externalid.map(lambda x: str(x) if x else x)
job.info()
assert False

# %% markup_percent
tem = job[['job_externalid', 'markupPercentage']].rename(columns={'markupPercentage': 'markup_percent'})
vjob.update_compensation_markup_percent(tem, mylog)

# %% headcount
tem = job[['job_externalid', 'numOpenings']].rename(columns={'numOpenings': 'head_count'})
vjob.update_head_count(tem, mylog)

# %% payrate
tem = job[['job_externalid', 'payRate']].rename(columns={'payRate': 'pay_rate'})
vjob.update_pay_rate(tem, mylog)

# %% public description
tem = job[['job_externalid', 'publicDescription']].rename(columns={'publicDescription': 'public_description'})
tem = tem.where(tem.notnull(), '')
tem = tem.loc[tem.public_description != '']
tem.loc[tem.public_description.isnull()]
cp9 = vjob.update_public_description(tem, mylog)

# %% contract rate
assert set(job.salaryUnit.unique()).issubset({'Per Month', 'Per Hour'})
cp7 = vjob.update_pay_interval_hourly(job.loc[job.salaryUnit == 'Per Hour'], mylog)
cp8 = vjob.update_pay_interval_monthly(job.loc[job.salaryUnit == 'Per Month'], mylog)

# %% quick fee forcast
job['use_quick_fee_forecast'] = 1
cp5 = vjob.update_use_quick_fee_forecast(job, mylog)
job['percentage_of_annual_salary'] = job.feeArrangement
cp6 = vjob.update_percentage_of_annual_salary(job, mylog)

# %% job type
tem = job[['job_externalid', 'employmentType']].dropna().rename(columns={'employmentType': 'job_type'})
tem.job_type.unique()
tem.loc[tem.job_type == 'Permanent', 'job_type'] = 'permanent'
tem.loc[tem.job_type == 'Temporary', 'job_type'] = 'contract'
tem.loc[tem.job_type == 'Contract', 'job_type'] = 'contract'
tem.loc[tem.job_type == 'Direct Hire', 'job_type'] = 'permanent'
tem.loc[tem.job_type == 'Fixed Term', 'job_type'] = 'contract'
tem['job_type'].value_counts()
vjob.update_job_type(tem, mylog)

# %% description
tem = job[['job_externalid', 'description']].dropna().rename(columns={'description': 'internal_description'})
cp4 = vjob.update_internal_description(tem, mylog)

# %% job salary from
tem = job[['job_externalid', 'salary']].dropna().rename(columns={'salary': 'salary_from'})
vjob.update_salary_from(tem, mylog)

# %% start date close date
tem = job[['job_externalid', 'startDate']].dropna().rename(columns={'startDate': 'start_date'})
vjob.update_start_date(tem, mylog)
tem = job[['job_externalid', 'dateEnd']].dropna().rename(columns={'dateEnd': 'close_date'})
vjob.update_close_date(tem, mylog)

# if isOpen == False, the job close date will be reupdate to 'yesterday'
tem = job[['job_externalid', 'isOpen']]
tem = tem.loc[tem.isOpen == 0]
tem['close_date'] = datetime.datetime.now() - datetime.timedelta(days=1)
vjob.update_close_date(tem, mylog)

# %% salary to
tem = job[['job_externalid', 'customFloat1']].dropna().rename(columns={'customFloat1': 'salary_to'})
cp3 = vjob.update_salary_to(tem, mylog)

# %% reg date
vjob.update_reg_date(job, mylog)

# %% owner
job_owner = pd.read_sql("""
select
j.jobPostingID as job_externalid
, cont.email
, cont.email2
, cont.email3
, cont2.email as second_owner_email 
, cont2.email2 as second_owner_email2
, cont2.email3 as second_owner_email3
from bullhorn1.BH_JobPosting j
left join bullhorn1.BH_UserContact cont on j.userID = cont.userID
left join bullhorn1.BH_UserContact cont2 on j.reportToUserID = cont2.userID
""", engine_mssql)
job_owner = job_owner.melt(id_vars=['job_externalid'], value_name='email').drop('variable', axis='columns').dropna().drop_duplicates()
job_owner = job_owner.loc[job_owner.email.str.strip() != '']
job_owner = job_owner.loc[job_owner.job_externalid.isin(job.job_externalid)]
job_owner.job_externalid = job_owner.job_externalid.astype(str)

cp = vjob.insert_owner(job_owner, mylog)

# %% note
prefixs = [
    'Job BH ID',
    'Address',
    'City',
    'County',
    'Postcode',
    'Country',
    'customInt3'
]
job['note'] = job[[
    'job_externalid',
    'address',
    'city',
    'state',
    'zip',
    'COUNTRY',
    'customInt3'
            ]].apply(lambda x: '\n'.join([': '.join([str(e1) for e1 in e]) for e in zip(prefixs, x) if e[1]]), axis='columns')


cp2 = vjob.update_note(job, mylog)




