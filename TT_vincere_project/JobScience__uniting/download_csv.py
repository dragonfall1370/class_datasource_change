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
# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('un_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
# csv_path = r'D:\Uniting\20190910_Uniting_Ambition_RevDump_CSV'
csv_path = r'D:\Tony\project\Uniting_DB\WE_00DD0000000nfloMAA_1'

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# comp = pd.read_csv(os.path.join(standard_file_upload, 'comp_skip_file.csv'))
# comp = comp.drop('Errors', axis=1)
# comp['company-name'] = comp['company-name']+'_'+comp['company-externalId']
# comp.to_csv(os.path.join(standard_file_upload, '1_company_20191120.csv'), index=False)

# cont_ori = pd.read_csv(os.path.join(standard_file_upload, '4_contact.csv'))
# cont_skip = pd.read_csv(os.path.join(standard_file_upload, 'contact_skip.csv'))
# cont_add_2 = pd.read_csv(os.path.join(standard_file_upload, '2_contacts_20191120.csv'))
#
# c = cont_ori.loc[~cont_ori['contact-externalId'].isin(cont_skip['contact-externalId'])]
# cont_not_add = cont_add_2.loc[~cont_add_2['contact-externalId'].isin(c['contact-externalId'])]
#
# cont_not_add['contact-email'] = cont_not_add['contact-externalId']+'_'+cont_not_add['contact-email']
# cont_not_add.to_csv(os.path.join(standard_file_upload, '4_contact_20191120_2_1.csv'), index=False)

job = pd.read_csv(os.path.join(standard_file_upload, '5_job_20192011.csv'))
job = job.drop('Errors', axis=1)
job.to_csv(os.path.join(standard_file_upload, '5_job_20192011.csv'), index=False)
