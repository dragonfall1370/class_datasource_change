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
cf.read('ca_config.ini')
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
select Candidate_Id, Candidate_Code, Sort_Code, Name, Bank_Account_Name, Bank_Account_No, Bank_Account_Ref, pm.Description, pay.*
from Candidate c
left join Person p on c.Person_Id = p.Person_Id
left join Bank_Branch bb on bb.Branch_Id = p.Branch_Id
left join Payment_Method pm on pm.Payment_Method_Id = p.Payment_Method_Id
left join (select PAYE_Id, NINO, Tax_Code, NI_Table from PAYE) pay on p.PAYE_Id = pay.PAYE_Id
where Candidate_Id not in (
39367
,2896
,37207
,22106
,25798
,35819
,50227
,48100
,46300
,47257)
""", engine_mssql)
onb['candidate_externalid'] = onb['Candidate_Id'].astype(str)
assert False
# %% National Insurance number
tem = onb[['candidate_externalid', 'NINO']].dropna()
tem['value'] = tem['NINO']
tem['type'] = 'national_insurance_number'
vcand.insert_onboarding_value(tem, mylog)

# %% payment method
tem = onb[['candidate_externalid', 'Description']].dropna()
tem['value'].unique()
tem.loc[tem['Description'] == 'Bacs/Temps', 'value'] = 0
tem.loc[tem['Description'] == 'Cheque/Temps', 'value'] = 1
tem['type'] = 'payment_method'
vcand.insert_onboarding_value(tem, mylog)

# %% sort code
tem = onb[['candidate_externalid', 'Sort_Code']].dropna()
tem['value'] = tem['Sort_Code']
tem['type'] = 'sort_code'
vcand.insert_onboarding_value(tem, mylog)

# %% bank_branch
tem = onb[['candidate_externalid', 'Name']].dropna()
tem['value'] = tem['Name']
tem['type'] = 'bank_branch'
vcand.insert_onboarding_value(tem, mylog)

# %% account_name
tem = onb[['candidate_externalid', 'Bank_Account_Name']].dropna()
tem['value'] = tem['Bank_Account_Name']
tem['type'] = 'account_name'
vcand.insert_onboarding_value(tem, mylog)

# %% account_number
tem = onb[['candidate_externalid', 'Bank_Account_No']].dropna()
tem['value'] = tem['Bank_Account_No']
tem['type'] = 'account_number'
vcand.insert_onboarding_value(tem, mylog)

# %% employee_id
tem = onb[['candidate_externalid', 'Bank_Account_Ref']].dropna()
tem['value'] = tem['Bank_Account_Ref']
tem['type'] = 'employee_id'
vcand.insert_onboarding_value(tem, mylog)

# %% tax_code
tem = onb[['candidate_externalid', 'Tax_Code']].dropna()
tem['Tax_Code'].unique()
# tem.loc[tem['Tax_Code'] == '1250L', 'value'] = 0
# # tem.loc[tem['Tax_Code'] == 'C', 'value'] = '1250L M1/W1'
# tem.loc[tem['Tax_Code'] == 'BR', 'value'] = 2
tem['type'] = 'tax_code'
tem['value'] = tem['Tax_Code']
vcand.insert_onboarding_value(tem, mylog)

# %% nation_insurance_letter
tem = onb[['candidate_externalid', 'NI_Table']].dropna()
tem['NI_Table'].unique()
tem.loc[tem['NI_Table'] == 'A', 'value'] = 0
tem.loc[tem['NI_Table'] == 'C', 'value'] = 2
tem.loc[tem['NI_Table'] == 'M', 'value'] = 5
tem['type'] = 'national_insurance_letter'
vcand.insert_onboarding_value(tem, mylog)
