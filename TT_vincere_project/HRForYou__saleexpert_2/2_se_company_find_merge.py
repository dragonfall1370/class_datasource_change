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
cf.read('saleexpert.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
db_source = cf[cf['default'].get('src_db')]
dest_db = cf[cf['default'].get('review_db')]
mylog = log.get_info_logger(log_file)

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
connection_str = "mysql+pymysql://{user}:{password}@{server}/{db}" \
    .format(user=db_source.get('user')
            , password=db_source.get('password')
            , server=db_source.get('server')
            , db=db_source.get('database'))
engine = sqlalchemy.create_engine(connection_str)

conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre.raw_connection()

from common import vincere_company
vcom = vincere_company.Company(connection)
assert False
# %%
sql = """
select id, name, external_id from company
"""
company = pd.read_sql(sql, engine_postgre)
company_2 = pd.read_csv(os.path.join(standard_file_upload, '2_company.csv'))
company_2['company-externalId'] = company_2['company-externalId'].astype(str)
company_match = company.merge(company_2, left_on=['name','external_id'], right_on=['company-name','company-externalId'], suffixes=['', '_y'], how='outer', indicator=True)
company_match_add = company_match.loc[company_match['_merge']=='right_only']
company_match.loc[company_match['_merge']=='left_only']
company_2.loc[company_2['company-externalId']=='2227']
company_to_add = company_match_add[['company-externalId','company-name','company-owners']]
company_to_add['company-name'] = company_to_add['company-name']+'_2'
company_to_add.to_csv(os.path.join(standard_file_upload, '2_company_filter.csv'), index=False)


contact = pd.read_csv(os.path.join(standard_file_upload, '4_contact.csv'))
contact['contact-companyId'] = contact['contact-companyId'].apply(lambda x: str(x).split('.')[0])
contact.to_csv(os.path.join(standard_file_upload, '4_contact.csv'), index=False)

job = pd.read_csv(os.path.join(standard_file_upload, '5_job.csv'))
job['position-contactId'] = job['position-contactId'].apply(lambda x: str(x).split('.')[0])
job.to_csv(os.path.join(standard_file_upload, '5_job.csv'), index=False)


sql = """
select id, email, external_id from candidate
"""
cand = pd.read_sql(sql, engine_postgre)
cand_2 = pd.read_csv(os.path.join(standard_file_upload, '6_candidate.csv'))
cand_2['candidate-externalId'] = cand_2['candidate-externalId'].astype(str)

cand_match = cand.merge(cand_2, left_on=['email','external_id'], right_on=['candidate-email','candidate-externalId'], suffixes=['', '_y'], how='outer', indicator=True)
company_match_add = cand_match.loc[cand_match['_merge']=='left_only']
company_to_add = company_match_add[['company-externalId','company-name','company-owners']]
company_to_add['company-name'] = company_to_add['company-name']+'_2'
company_to_add.to_csv(os.path.join(standard_file_upload, '2_company_filter.csv'), index=False)


company = pd.read_csv(r'C:\Users\tony\Desktop\skip_file (2).csv')
company['company-name'] = company['company-name'] +'_'+ company['company-externalId']
company.drop('Errors', axis=1, inplace=True)
company.to_csv(os.path.join(standard_file_upload, '2_company_more.csv'), index=False)

cand = pd.read_csv(r'C:\Users\tony\Desktop\skip_file (5).csv')
cand['candidate-email'] = '1_'+ cand['candidate-email']
cand.drop('Errors', axis=1, inplace=True)
cand.to_csv(os.path.join(standard_file_upload, '6_candidate_more.csv'), index=False)