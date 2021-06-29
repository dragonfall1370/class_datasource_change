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
cf.read('wm_config.ini')
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

# %% company
company = pd.read_csv(r'D:\Tony\project\vincere_project\CSV__wematch\Data\company.csv')
company2 = pd.read_csv(r'D:\Tony\project\vincere_project\CSV__wematch\Data\company_2.csv')
contact = pd.read_csv(r'D:\Tony\project\vincere_project\CSV__wematch\Data\contact.csv')
company = company.where(company.notnull(),None)
contact = contact.where(contact.notnull(),None)
company = company.fillna('')
contact = contact.fillna('')
candidate = pd.read_csv(r'D:\Tony\project\vincere_project\CSV__wematch\Data\candidate.csv')
candidate = candidate.where(candidate.notnull(),None)
assert False
# %% company
# company_1 = company
# company_1['Location Name'] = company_1['Company Address']

company['rn'] = company.groupby('Company Name').cumcount()
company['rn'] = company['rn'].astype(str)
company['Company Name_1'] = company['Company Name']+'_'+company['rn']
tem.to_csv(r'D:\Tony\project\vincere_project\CSV__wematch\Data\company_2.csv',index=False)

# %% contact
contact_1 = contact.merge(company2, on=[
    'Company Name'])
contact_1.loc[contact_1['Company ID'].isnull()]
contact_1.to_csv('contact_import.csv',index=False)

# %% candidate
candidate['rn'] = candidate.groupby('Primary Email').cumcount()
candidate.loc[(candidate['Primary Email'].isnull()), 'Primary Email'] = candidate['Candidate ID']+'@noemail.com'
candidate.loc[candidate['rn']>0]
candidate.loc[candidate['Primary Email']=='na']
candidate.loc[candidate['Work Email']=='na']
candidate['Work Email'].unique()
candidate.loc[candidate['Last Name'].isnull()]
candidate.loc[(candidate['rn']!='1'), 'Primary Email'] = candidate['rn']+'_'+candidate['Primary Email']
candidate.to_csv('candidate_import.csv',index=False)