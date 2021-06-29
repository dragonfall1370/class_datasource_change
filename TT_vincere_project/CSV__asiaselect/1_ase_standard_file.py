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
cf.read('ase_config.ini')
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
company = pd.read_csv(r'D:\Tony\project\vincere_project\CSV__asiaselect\Data\company.csv')
contact = pd.read_csv(r'D:\Tony\project\vincere_project\CSV__asiaselect\Data\contact.csv')
contact = contact.merge(company[['Company ID','Client']],on='Client')
contact['First_name'] = contact['POC'].apply(lambda x: x.split(' ')[0])
contact['Last_name'] = contact['POC'].apply(lambda x: x.split(' ')[-1])
contact['Middle_name'] = contact.apply(lambda x: x['POC'].replace(str(x['First_name']), ''), axis=1)
contact['Middle_name'] = contact.apply(lambda x: x['Middle_name'].replace(str(x['Last_name']), ''), axis=1)
contact.to_csv('4_contact.csv', index=False)


candidate = pd.read_csv(r'D:\Tony\project\vincere_project\CSV__asiaselect\Data\candidate.csv',encoding='ISO-8859-1')
candidate = candidate.drop(columns=['Candidate ID']).drop_duplicates()
candidate.to_csv('5_candidate.csv', index=False)


candidate_2 = pd.read_csv('candidate_2.csv',encoding='ISO-8859-1')
candidate_2['rn'] = candidate_2.groupby('Primary Email').cumcount()
candidate_2['rn'] = candidate_2['rn']+2
candidate_2['rn'] = candidate_2['rn'].astype(str)
candidate_2.to_csv('5_candidate_2.csv', index=False)

candidate_2['Primary Email'] = candidate_2['rn'] +'_'+ candidate_2['Primary Email']
candidate_2.loc[candidate_2['Primary Email'].str.contains('mcbalderite@gmail.com')]
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