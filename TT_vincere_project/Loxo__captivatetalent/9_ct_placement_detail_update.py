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
cf.read('ct_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
sqlite_path = cf['default'].get('sqlite_path')
dest_db = cf[cf['default'].get('dest_db')]
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()

from common import vincere_placement_detail
vplace = vincere_placement_detail.PlacementDetail(connection)

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
# %% permanent
placed_info = pd.read_sql("""
select * from jobs_placements
""", engine_sqlite)
placed_info['candidate_externalid'] = placed_info['placements_person'].astype(str)
placed_info['job_externalid'] = placed_info['job_id'].astype(str)
# placed_info['job_externalid'] = placed_info['RMSPLACEMENTID'].astype(str)
# placed_info['rn'] = placed_info.groupby('RMSPLACEMENTID').cumcount()
# placed_info = placed_info.loc[placed_info['rn']==0]

assert False
# %% offer date/place date
jobapp_created_date = placed_info[['job_externalid', 'candidate_externalid','placements_created']].dropna()
jobapp_created_date['offer_date'] = pd.to_datetime(jobapp_created_date['placements_created'])
jobapp_created_date['placed_date'] = pd.to_datetime(jobapp_created_date['placements_created'])
vplace.update_offerdate(jobapp_created_date, mylog)
vplace.update_placeddate(jobapp_created_date, mylog)

# %% start date/end date
stdate = placed_info[['job_externalid', 'candidate_externalid', 'placements_startDate']].dropna()
stdate['start_date'] = pd.to_datetime(stdate['placements_startDate'])
cp1 = vplace.update_startdate_only_for_placement_detail(stdate, mylog)

# %% salary
tem = placed_info[['job_externalid', 'candidate_externalid', 'placements_salary']].dropna().rename(columns={'placements_salary':'annual_salary'})
tem['annual_salary'] = tem['annual_salary'].astype(float)
cp6 = vplace.update_offer_annual_salary(tem, mylog)

# %% quick fee
#tem = placed_info[['job_externalid', 'candidate_externalid', 'placements_feeType']].dropna()
#tem = tem.loc[tem['placements_feeType'] == 'Percentage']
#vplace.update_use_quick_fee_forecast(tem, mylog)
vplace.update_use_quick_fee_forecast_for_permanent_job()

# %% quick fee
tem = placed_info[['job_externalid', 'candidate_externalid', 'placements_feeType','placements_fee']].dropna()

tem['percentage_of_annual_salary'] = tem['placements_fee']
tem['percentage_of_annual_salary'] = tem['percentage_of_annual_salary'].astype(float)
tem = tem.loc[tem['percentage_of_annual_salary']<101]
tem.percentage_of_annual_salary = tem.percentage_of_annual_salary.round(2)
cp5, cp6 = vplace.update_percentage_of_annual_salary(tem, mylog)

# %% quick fee
tem = placed_info[['job_externalid', 'candidate_externalid', 'placements_salary','placements_fee']].dropna()
tem['placements_fee'] = tem['placements_fee'].astype(float)
tem['placements_salary'] = tem['placements_salary'].astype(float)
tem = tem.loc[tem['placements_fee']>100]
tem['percentage_of_annual_salary'] = tem['placements_fee']/tem['placements_salary']
tem['percentage_of_annual_salary'] = tem['percentage_of_annual_salary']*100
tem.percentage_of_annual_salary = tem.percentage_of_annual_salary.round(2)
cp5, cp6 = vplace.update_percentage_of_annual_salary(tem, mylog)

# %% po number
# tem = placed_info[['job_externalid', 'candidate_externalid', 'PO NUMBER']].dropna()
# tem['client_purchase_order'] = tem['PO NUMBER']
# vplace.update_purchase_order_number(tem, connection, mylog)

# %% payrate
# tem = placed_info[['job_externalid', 'candidate_externalid', 'RATE PAY']].dropna()
# tem['pay_rate'] = tem['RATE PAY'].astype(float)
# cp8 = vplace.update_pay_rate(tem, mylog)

# %% charge rate
# tem = placed_info[['job_externalid', 'candidate_externalid', 'RATE BILL']].dropna()
# tem['charge_rate'] = tem['RATE BILL'].astype(float)
# cp1 = vplace.update_charge_rate(tem, mylog)

# %% currency country
# tem = placed_info[['job_externalid', 'candidate_externalid','RATE PAY CURRENCY']].dropna()
# tem['RATE PAY CURRENCY'].unique()
# tem.loc[(tem['RATE PAY CURRENCY'] == 'MYR    '), 'currency_type'] = 'myr'
# tem.loc[(tem['RATE PAY CURRENCY'] == 'USD    '), 'currency_type'] = 'usd'
# vplace.update_offer_currency_type(tem, mylog)

# %% interval
# tem = placed_info[['job_externalid','candidate_externalid','RATE PAY UNIT']].dropna() #pay_interval
# tem['RATE PAY UNIT'].unique()
# tem.loc[(tem['RATE PAY UNIT'] == 'Monthly'), 'pay_interval'] = 'monthly'
# tem.loc[(tem['RATE PAY UNIT'] == 'Hourly'), 'pay_interval'] = 'hourly'
# tem.loc[(tem['RATE PAY UNIT'] == 'Daily'), 'pay_interval'] = 'daily'
# tem.loc[(tem['RATE PAY UNIT'] == 'Per Diem'), 'pay_interval'] = 'daily'
# vplace.update_pay_interval(tem, mylog)

# %% placement note
note = placed_info[['candidate_externalid','job_externalid','placements_jobType','placements_notes','placements_fee','placements_feeType']]
note.loc[(note['placements_feeType'] == 'Flat'), 'fee'] = note['placements_fee']
note = note.where(note.notnull(),None)
note['fee'] = note['fee'].apply(lambda x: str(x) if x else x)
note['placements_notes'] = note['placements_notes'].apply(lambda x: html_to_text(x) if x else x)
note['note'] = note[[
    'placements_jobType','placements_notes','fee']] \
   .apply(lambda x: '\n'.join([': '.join([i for i in e]) for e in zip([
    'Placement Type', 'Notes','Flat fee'], x) if e[1]]), axis=1)
cp0 = vplace.update_internal_note(note, mylog)

# %% split
placed_split= pd.read_sql("""
select * from jobs_placements_split
""", engine_sqlite)
placed_split['candidate_externalid'] = placed_split['placements_person'].astype(str)
placed_split['job_externalid'] = placed_split['job_id'].astype(str)
placed_split.to_csv('placed_split.csv')

placed_split = pd.read_csv('placed_split.csv')
placed_split['candidate_externalid'] = placed_split['placements_person'].astype(str)
placed_split['job_externalid'] = placed_split['job_id'].astype(str)
tem = placed_split[['job_externalid', 'candidate_externalid', 'user','percentage']].dropna()
tem['user_email'] = tem['user']
tem['shared'] = tem['percentage']
tem['shared'] = tem['shared'].astype(float)
vplace.insert_profit_split_mode_percentage(tem, mylog)