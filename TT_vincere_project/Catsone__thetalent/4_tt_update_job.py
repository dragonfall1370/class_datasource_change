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
cf.read('tt_config.ini')
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
engine_sqlite = sqlalchemy.create_engine('sqlite:///thetalent.db', encoding='utf8')

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
select j.joborder_id as job_externalid
, c2.company_id as company_externalid
, c.contact_id as contact_externalid
, j.start_date
, j.status
, j.type
, j.is_hot
, j.openings
, j.notes
, j.date_created
, j.salary
, j.duration
, j.rate_max
, j.city
, j.state
, j.zip
, c3.name as country_name
from joborder j
left join contact c on c.company_id = j.company_id and c.contact_id = j.contact_id
left join company c2 on j.company_id = c2.company_id
left join country c3 on j.country_id = c3.country_id
""", engine_sqlite)

job_value = pd.read_sql("""
select j.joborder_id, efv.value_text, ef.field_name from extra_field_value efv
left join extra_field ef on ef.extra_field_id = efv.extra_field_id
join joborder j on efv.data_item_id = j.joborder_id
""", engine_sqlite)
job_value = pd.pivot_table(job_value, values='value_text',columns='field_name',aggfunc='first', index = 'joborder_id')
job_value['job_externalid'] = job_value.index
job_value.reset_index()
job = job.merge(job_value, on='job_externalid', how='left')
assert False


# %% job type
tem = job[['job_externalid', 'type']]
tem['type'].value_counts()
tem.loc[tem['type']=='C', 'type'] = 'permanent'
tem.loc[tem['type']=='H', 'type'] = 'contract'
tem.loc[tem['type']=='C2H', 'type'] = 'contract'
tem['job_type'] = tem['type']
tem['job_type'].unique()
vjob.update_job_type(tem, mylog)

# %% head count
tem = job[['job_externalid', 'openings']].dropna().rename(columns={'openings': 'head_count'})
tem['head_count'] = tem['head_count'].astype(int)
vjob.update_head_count(tem, mylog)

# %% pay rate from / payrate to
# tem = job[['job_externalid', 'Max Salary']].dropna().rename(columns={'Max Salary': 'pay_rate_from'})
# tem = tem.loc[tem.pay_rate_from != '40 per hour']
# tem['pay_rate_from'] = tem['pay_rate_from'].apply(lambda x: x.replace(',','').replace('$',''))
# tem['pay_rate_from'] = tem['pay_rate_from'].astype(float)
# tem['pay_rate_from'].value_counts()
# vjob.update_pay_rate_from(tem, mylog)
# tem = job[['job_externalid', 'Min Salary']].dropna().rename(columns={'Min Salary': 'pay_rate_to'})
# tem = tem.loc[tem.pay_rate_to != '35/hr']
# tem['pay_rate_to'] = tem['pay_rate_to'].apply(lambda x: x.replace(',','').replace('$',''))
# tem['pay_rate_to'] = tem['pay_rate_to'].astype(float)
# tem['pay_rate_to'].value_counts()
# vjob.update_pay_rate_to(tem, mylog)

# %% Actual salary
tem = job[['job_externalid', 'Max Salary']].dropna().rename(columns={'Max Salary': 'actual_salary'})
tem = tem.loc[tem.actual_salary != '40 per hour']
tem['actual_salary'] = tem['actual_salary'].apply(lambda x: x.replace(',','').replace('$',''))
tem['actual_salary'] = tem['actual_salary'].astype(float)
vjob.update_actual_salary(tem, mylog)

# %% salary from/to
tem = job[['job_externalid', 'Max Salary']].dropna().rename(columns={'Max Salary': 'salary_from'})
tem = tem.loc[tem.salary_from != '40 per hour']
tem['salary_from'] = tem['salary_from'].apply(lambda x: x.replace(',','').replace('$',''))
tem['salary_from'] = tem['salary_from'].astype(float)
cp8 = vjob.update_salary_from(tem, mylog)

tem = job[['job_externalid', 'Min Salary']].dropna().rename(columns={'Min Salary': 'salary_to'})
tem = tem.loc[tem.salary_to != '35/hr']
tem['salary_to'] = tem['salary_to'].apply(lambda x: x.replace(',','').replace('$',''))
tem['salary_to'] = tem['salary_to'].astype(float)
cp9 = vjob.update_salary_to(tem, mylog)

# %% pay rate
# tem = job[['job_externalid', 'Charge']].dropna().rename(columns={'Charge': 'pay_rate'})
# cp = vjob.update_pay_rate(tem, mylog)

# %% quick fee forcast
job['use_quick_fee_forecast'] = 1
vjob.update_use_quick_fee_forecast(job, mylog)
tem = job[['job_externalid', 'Fee Pct']].dropna().rename(columns={'Fee Pct': 'percentage_of_annual_salary'})
tem['percentage_of_annual_salary'] = tem['percentage_of_annual_salary'].apply(lambda x: x.replace('%',''))
tem['percentage_of_annual_salary'] = tem['percentage_of_annual_salary'].astype(float)
tem['percentage_of_annual_salary'].value_counts()
cp10 = vjob.update_percentage_of_annual_salary(tem, mylog)

# %% job description
job_des = pd.read_sql("""
    select j.joborder_id as job_externalid
, j.description as public_description
from joborder j;
    """, engine_sqlite)
job_des = job_des.dropna()
vjob.update_public_description(job_des, mylog)


# %% note
job['location'] = job[['city', 'state', 'zip', 'country_name']] \
    .apply(lambda x: ', '.join([e for e in x if e]), axis=1)
job.loc[job['is_hot']=='0', 'is_hot'] = 'No'
job.loc[job['is_hot']=='1', 'is_hot'] = 'Yes'
note = job[[
    'job_externalid',
    'location',
    'salary',
    'duration',
    'rate_max',
    'Previous ID',
    'Closed Reason',
    'Job Function',
    'Notes',
    'is_hot',
    'notes'
                ]]

prefixes = [
'CATS ID',
'Location',
'Salary',
'Duration',
'Maximum Rate',
'Previous ID',
'Closed Reason',
'Job Function',
'Notes',
'Hot',
'Notes',
]
note['notes'] = note['notes'].apply(lambda x: html_to_text(x) if x else x)
note = note.where(note.notnull(), None)
note['note'] = note.apply(lambda x: '\n '.join([': '.join(e) for e in zip(prefixes, x) if e[1]]), axis='columns')

vjob.update_note(note, mylog)

# %% reg date
job_date = job.loc[job['date_created']!='0000-00-00 00:00:00']
job_date['date_created'] = pd.to_datetime(job_date['date_created'])
job_date.rename(columns={'date_created': 'reg_date'}, inplace=True)
vjob.update_reg_date(job_date, mylog)

# %% open/close  start date end date
# tem = job[['job_externalid', 'EndDate']].dropna()
# tem['close_date'] = pd.to_datetime(tem['EndDate'])
# cp1 = vjob.update_close_date(tem, mylog)

tem = job[['job_externalid', 'start_date']].dropna()
tem = tem.loc[tem['start_date']!='0000-00-00 00:00:00']
tem['start_date'] = pd.to_datetime(tem['start_date'])
vjob.update_start_date(tem, mylog)
