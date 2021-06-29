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
cf.read('sh_config.ini')
log_file = cf['default'].get('log_file')
# data_folder = cf['default'].get('data_folder')
data_folder = '/Users/truongtung/Desktop'

data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
dest_db = cf[cf['default'].get('dest_db')]
src_db = cf[cf['default'].get('src_db')]
sqlite_path = cf['default'].get('sqlite_path')
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

from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)
# %% comp
onb = pd.read_sql("""
select CONCAT('',peo_no) as candidate_externalid, peo_forename, peo_surname
, nullif(peo_ni,'') as peo_ni
, nullif(peo_payroll_no,'') as peo_payroll_no
, nullif(peo_ltd_cli_name,'') as peo_ltd_cli_name
, nullif(peo_ltd_reg_no,'') as peo_ltd_reg_no
, nullif(peo_bank_name,'') as peo_bank_name
, nullif(peo_bank_acc_name,'') as peo_bank_acc_name
, nullif(peo_cnt_accountno,'') as peo_cnt_accountno
, nullif(peo_bank_sort_code,'') as peo_bank_sort_code
, nullif(peo_bank_society_no,'') as peo_bank_society_no
from people
where peo_flag =1
""", engine_mssql)
assert False
# %% payroll id
# tem = onb[['candidate_externalid', 'peo_payroll_no']].dropna()
# tem['value']=tem['peo_payroll_no']
# tem['type'] = 'payroll_id'
# vcand.insert_onboarding_value(tem, mylog)

# %% National Insurance number
# tem = onb[['candidate_externalid', 'peo_ni']].dropna()
# tem['value'] = tem['peo_ni']
# tem['type'] = 'national_insurance_number'
# vcand.insert_onboarding_value(tem, mylog)

# %% compnay_name
# tem = onb[['candidate_externalid', 'peo_ltd_cli_name']].dropna()
# tem['value'] = tem['peo_ltd_cli_name']
# tem['type'] = 'company_name'
# vcand.insert_onboarding_value(tem, mylog)

# %% compnay_number
# tem = onb[['candidate_externalid', 'peo_ltd_reg_no']].dropna()
# tem['value'] = tem['peo_ltd_reg_no']
# tem['type'] = 'company_number'
# vcand.insert_onboarding_value(tem, mylog)

# %% bank_name
# tem = onb[['candidate_externalid', 'peo_bank_name']].dropna()
# tem['value'] = tem['peo_bank_name']
# tem['type'] = 'bank_name'
# vcand.insert_onboarding_value(tem, mylog)

# %% sort code
tem = onb[['candidate_externalid', 'peo_bank_sort_code']].dropna()
tem['value'] = tem['peo_bank_sort_code']
tem['type'] = 'sort_code'
vcand.insert_onboarding_value(tem, mylog)

# %% bss number
# tem = onb[['candidate_externalid', 'peo_bank_society_no']].dropna()
# tem['value'] = tem['peo_bank_society_no']
# tem['type'] = 'bsb_number'
# vcand.insert_onboarding_value(tem, mylog)

# %% account_name
tem = onb[['candidate_externalid', 'peo_bank_acc_name']].dropna()
tem['value'] = tem['peo_bank_acc_name']
tem['type'] = 'account_name'
vcand.insert_onboarding_value(tem, mylog)

# %% account_number
tem = onb[['candidate_externalid', 'peo_cnt_accountno']].dropna()
tem['value'] = tem['peo_cnt_accountno']
tem['type'] = 'account_number'
vcand.insert_onboarding_value(tem, mylog)




