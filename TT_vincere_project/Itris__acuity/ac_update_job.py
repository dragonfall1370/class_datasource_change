# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import re
import os
import psycopg2
import common.vincere_common as vincere_common
import common.vincere_custom_migration as vincere_custom_migration
import common.vincere_standard_migration as vincere_standard_migration
import pandas as pd
import sqlalchemy
import datetime
# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('ac_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
src_db = cf[cf['default'].get('src_db')]
dest_db = cf[cf['default'].get('dest_db')]

mylog = log.get_info_logger(log_file)

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)
csv_path = r""

# %% data connection
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)

engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')

from common import vincere_job
vjob = vincere_job.Job(engine_postgre.raw_connection())

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

# %% reg date

job = pd.read_sql("""
select 
j.Id as job_externalid
, com.company_externalid
, cont.CONTACT_ID as contact_externalid
, j.JobTitle as job_title
, j.CreatedDateTime as reg_date
, u.EmailAddress as job_owner
, case j.JobStatus
        when 0 then 'Vacant'
        when 1 then 'Filled by us'
        when 2 then 'Cancelled'
        when 3 then 'Filled elsewhere'
        when 4 then 'On hold'
        when 5 then 'Filled internally'
        else concat('', j.JobStatus)
        end as jobstatus
, case j.JobStatus
        when 0 then 'open'
        when 1 then 'open'
        when 2 then 'close'
        when 3 then 'close'
        when 4 then 'close'
        when 5 then 'close'
        else 'close'
        end as close_job
, case j.JobType
                 when 5 then 'Contract'
                 when 6 then 'Permanent'
                 when 7 then 'Temporary'
				 end as jobtype
, j.Location
, j.StartDate
, j.EndDate
, j.NumberOfPeopleRequired as headcount
, j.Payment
, j.PaymentTo
, j.Payment * j.Charge / 100 as profit
, j.Charge
, j.ChargeInterval
, j.CloseDate
from Job j
left join (
	select
		 c.COMPANY_ID as company_externalid
		from Companies c 
		where c.DELETED = 0
) com on j.CompanyId = com.company_externalid
left join (
		select 
		cont.CONTACT_ID
		, cont.COMPANY_ID
		from Contacts cont
		where cont.DELETED = 0		
) cont on (j.ContactId = cont.CONTACT_ID and j.CompanyId = cont.COMPANY_ID)
left join [User] u on j.CreatedUserId = u.Id;
""", engine_mssql)
assert False
# %% reg date
vjob.update_reg_date(job, mylog)

# %% open/close  start date end date
tem = job[['job_externalid', 'EndDate']].dropna()
tem['close_date'] = pd.to_datetime(tem['EndDate'])
cp1 = vjob.update_close_date(tem, mylog)

tem = job[['job_externalid', 'StartDate']].dropna()
tem['start_date'] = pd.to_datetime(tem['StartDate'])
vjob.update_start_date(tem, mylog)

# %% job type
tem = job[['job_externalid', 'jobtype']].replace({'Contract': 'contract', 'Permanent': 'permanent'})
tem['job_type'] = tem['jobtype']
tem['job_type'].unique()
vjob.update_job_type(tem, mylog)

# %% head count
tem = job[['job_externalid', 'headcount']].dropna().rename(columns={'headcount': 'head_count'})
vjob.update_head_count(tem, mylog)

# %% pay rate from / payrate to
# tem = job[['job_externalid', 'paymntfrom']].dropna().rename(columns={'paymntfrom': 'pay_rate_from'})
# vjob.update_pay_rate_from(tem, mylog)
# tem = job[['job_externalid', 'PaymentTo']].dropna().rename(columns={'PaymentTo': 'pay_rate_to'})
# vjob.update_pay_rate_to(tem, mylog)

# %% Actual salary
tem = job[['job_externalid', 'Payment']].dropna().rename(columns={'Payment': 'actual_salary'})
vjob.update_actual_salary(tem, mylog)

# %% salary from/to
frsala = job[['job_externalid', 'Payment']].dropna()
frsala['salary_from'] = frsala['Payment']
cp8 = vjob.update_salary_from(frsala, mylog)

tosala = job[['job_externalid', 'PaymentTo']].dropna()
tosala['salary_to'] = tosala['PaymentTo']
cp9 = vjob.update_salary_to(tosala, mylog)

# %% pay rate
tem = job[['job_externalid', 'Charge']].dropna().rename(columns={'Charge': 'pay_rate'})
cp = vjob.update_pay_rate(tem, mylog)

# %% quick fee forcast
job['use_quick_fee_forecast'] = 1
vjob.update_use_quick_fee_forecast(job, mylog)

# %% job description
job = pd.read_sql("""
    select JOB_ID as job_externalid, DESCRIPTION_HTML as public_description from JobDescriptions;
    """, engine_mssql)

vjob.update_public_description(job, mylog)

# %% calculate profit and percentage of annual salary
def _cal_profit_and_annual_salary_percentage(df):
    # for Permanent
    filter_contract = (df.ChargeInterval.isin([0, 1, 2, 3, 4, 5])) & (df.jobtype.isin(['Permanent']))
    percentage_charge = df.loc[filter_contract]
    percentage_charge['profit'] = percentage_charge.Charge * percentage_charge.Payment/100

    filter_contract = (df.ChargeInterval.isin([6, 7])) & (df.jobtype.isin(['Permanent']))
    fixedfee_charge = df.loc[filter_contract]
    fixedfee_charge['profit'] = fixedfee_charge.Charge

    # for contract and tem
    filter_contract = (df.ChargeInterval.isin([0, 1, 2, 3, 4, 5])) & (df.jobtype.isin(['Contract', 'Temporary']))
    percentage_charge_contract = df.loc[filter_contract]
    percentage_charge_contract['profit'] = percentage_charge_contract.Charge - percentage_charge_contract.Payment
    percentage_charge_contract.loc[percentage_charge_contract.profit < 0, 'profit'] = 0

    filter_contract = (df.ChargeInterval.isin([6, 7])) & (df.jobtype.isin(['Contract', 'Temporary'])) # annually and fixed fee
    fixedfee_charge_contract = df.loc[filter_contract]
    fixedfee_charge_contract['profit'] = fixedfee_charge_contract.Charge
    fixedfee_charge_contract.loc[fixedfee_charge_contract.profit < 0, 'profit'] = 0
    return pd.concat([percentage_charge, fixedfee_charge, percentage_charge_contract, fixedfee_charge_contract])

tem = job[['job_externalid', 'Charge', 'Payment', 'ChargeInterval', 'jobtype']]
tem = _cal_profit_and_annual_salary_percentage(tem)
vjob.update_profit(tem, mylog)

tem['percentage_of_annual_salary'] = tem.profit * 100/ tem.Payment
vjob.update_percentage_of_annual_salary(tem, mylog)

# %% note
note = pd.read_sql("""
select j.Id as job_externalid,
       j.CreatedDateTime,
       j.ModifiedDateTime,
       j.Duration,
       j.Location,
       c.TELEPHONE,
       comp.Telephone,
       CONCAT(c.FIRST_NAME, ' ', c.LAST_NAME, ' ', c.ADDRESS) invoices,
       c.ADDRESS,
       CONCAT(u1.FirstName, ' ', u1.LastName) created_by,
       CONCAT(u2.FirstName, ' ', u2.LastName) modified_by
from Job j
left join Contacts c on c.CONTACT_ID = j.ContactId
left join CompanyAddress comp on comp.CompanyId = j.CompanyId
left join [User] u1 on u1.Id = j.CreatedUserId
left join [User] u2 on u2.Id = j.ModifyUserId
""", engine_mssql)
assert False
note = note.drop_duplicates()
# note['invoices'] = note['invoices'].apply(lambda x: ', '.join([e for e in x if e]))\
#     .map(lambda x: html_to_text(x)).map(lambda x: x.replace('\n', ', ').replace(',,', ',').replace(', ,', ','))
note.CreatedDateTime = note.CreatedDateTime.astype(object).where(note.CreatedDateTime.notnull(), None)
note.CreatedDateTime = note.CreatedDateTime.map(lambda x: datetime.datetime.strftime(x, '%d-%b-%Y %H:%M') if x else x)
note.ModifiedDateTime = note.ModifiedDateTime.astype(object).where(note.ModifiedDateTime.notnull(), None)
note.ModifiedDateTime = note.ModifiedDateTime.map(lambda x: datetime.datetime.strftime(x, '%d-%b-%Y %H:%M') if x else x)
note['note'] = note[['job_externalid', 'CreatedDateTime', 'ModifiedDateTime', 'Duration', 'Location',
                     'TELEPHONE', 'Telephone', 'invoices', 'created_by', 'modified_by','ADDRESS']] \
    .apply(lambda x: '\n\n'.join([': '.join(e) for e in zip(['Itris Job ID', 'Created On', 'Modified On', 'Duration', 'Location',
                                                             'Contact Phone', 'Company Phone', 'Invoice Details', 'Created By', 'Modified By', 'Company Address'], x) if e[1] and str(e[1]).strip() != '']), axis=1)
note1 = note[['job_externalid', 'note']]
note1 = note1.dropna()
vjob.update_note(note1, mylog)
