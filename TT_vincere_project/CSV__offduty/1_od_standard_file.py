# -*- coding: UTF-8 -*-
# import sys
# sys.path.append('D:\Tony\Working\DMvincere')
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
cf.read('od_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
sqlite_path = cf['default'].get('sqlite_path')

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% data connections
# engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
assert False
# %% company
company = pd.read_csv(r'D:\Tony\project\vincere_project\CSV__offduty\Data\Off_Duty_Dataset_company.csv')
company = company.fillna('')
company['rn'] = company.groupby('Company Name').cumcount()
# company['rn'] = company['rn'] + 100
# company['Company Name'] = company['Company Name'] + '_' + company['rn'].astype(str)
# company.to_csv('company_2.csv',index=False)

contact = pd.read_csv(r'D:\Tony\project\vincere_project\CSV__offduty\Data\Off_Duty_Dataset_contact.csv')
contact = contact.fillna('')
contact = contact.merge(company, on=['Company Name','Company Telephone Number','Company Address Prefecture','Company Address','company address 1','Company City ','Company Address 2','Company Address District','Company Postal Code','Parent Company','Company Brief/Summary','company web address'], how='left')
contact.to_csv('contact.csv',index=False)
#
# assert False
# # %% transpose
# company = vincere_standard_migration.process_vincere_comp(company, mylog)
# contact, default_company = vincere_standard_migration.process_vincere_contact_2(contact)
# # candidate = candidate.where(candidate.notnull(),None)
# candidate = vincere_standard_migration.process_vincere_cand(candidate, mylog)
#
# # %% to csv files
# candidate.to_csv(os.path.join(standard_file_upload, '6_candidate.csv'), index=False)
# contact.to_csv(os.path.join(standard_file_upload, '4_contact.csv'), index=False)
# company.to_csv(os.path.join(standard_file_upload, '2_company.csv'), index=False)