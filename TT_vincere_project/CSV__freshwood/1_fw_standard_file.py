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
cf.read('fw_config.ini')
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

# %% company
# company = pd.read_csv(r'D:\Tony\project\vincere_project\CSV__freshwood\Data\Vincere CSV Template - Freshwood Group - Company.csv')
# company = company.drop_duplicates()
# company = company.fillna('')
# company['company_id'] = company.index()
# contact = pd.read_csv(r'D:\Tony\project\vincere_project\CSV__freshwood\Data\Vincere CSV Template - Freshwood Group - Contact.csv')
# contact = contact.drop_duplicates()
# contact = contact.fillna('')
# company.to_csv('company.csv',index=False)
# contact.to_csv('contact.csv',index=False)
# contact = contact.merge(company[['Company Name','Company ID']], on='Company Name',how='left')
# contact = contact.drop_duplicates()
# contact['rn'] = contact.groupby('Contact ID').cumcount()
# candidate = pd.read_csv('D:\Tony\File\Departer\candidate import departer.csv', encoding = 'unicode_escape')

assert False
# %% transpose
company = pd.read_csv('company.csv')
contact = pd.read_csv('contact.csv')
company = company.fillna('')
contact = contact.fillna('')
contact = contact.merge(company[['Company Name','Company ID','Company Website','Company Brief / Summary']], on=['Company Name','Company Website','Company Brief / Summary'],how='left')
contact.to_csv(os.path.join(standard_file_upload, '4_contact.csv'), index=False)
company.to_csv(os.path.join(standard_file_upload, '2_company.csv'), index=False)
# %% transpose
company.rename(columns={
    'Company ID': 'company-externalId',
    'Company Name': 'company-name',
}, inplace=True)
company = vincere_standard_migration.process_vincere_comp(company, mylog)

contact.rename(columns={
    'Contact ID': 'contact-externalId',
    'Company ID': 'contact-companyId',
    'Contact Last Name': 'contact-lastName',
    'Contact First Name': 'contact-firstName',
    'Contact Middle Name': 'contact-middleName',
     'Primary Email': 'contact-email',
}, inplace=True)
contact, default_company = vincere_standard_migration.process_vincere_contact_2(contact)

# %% to csv files
contact.to_csv(os.path.join(standard_file_upload, '4_contact.csv'), index=False)
company.to_csv(os.path.join(standard_file_upload, '2_company.csv'), index=False)