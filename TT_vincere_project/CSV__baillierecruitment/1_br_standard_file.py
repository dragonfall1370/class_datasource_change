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
cf.read('br_config.ini')
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

# %% clean data
# engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
# com_cont = pd.read_csv(r'D:\Tony\project\vincere_project\CSV__baillierecruitment\Data\company_contact.csv')
# com_cont.loc[(com_cont['Company Address'] == '0'), 'Company Address'] = None
# com_cont.loc[(com_cont['Company Address 1'] == '0'), 'Company Address 1'] = None
# com_cont.loc[(com_cont['Company Address District / Suburb'] == '0'), 'Company Address District / Suburb'] = None
# com_cont.loc[(com_cont['Company Address Town / City'] == '0'), 'Company Address Town / City'] = None
# com_cont.loc[(com_cont['Company ZIP (Postal) Code'] == '0'), 'Company ZIP (Postal) Code'] = None
# com_cont.loc[(com_cont['Company Phone'] == '0'), 'Company Phone'] = None
# com_cont.loc[(com_cont['Company Website'] == '0'), 'Company Website'] = None
# com_cont.loc[(com_cont['Company Brief / Summary'] == '0'), 'Company Brief / Summary'] = None
# com_cont.loc[(com_cont['Contact First Name'] == '0'), 'Contact First Name'] = None
# com_cont.loc[(com_cont['Contact Last Name'] == '0'), 'Contact Last Name'] = None
# com_cont.loc[(com_cont['Job Title'] == '0'), 'Job Title'] = None
# com_cont.loc[(com_cont['Primary phone'] == '0'), 'Primary phone'] = None
# com_cont.loc[(com_cont['Mobile'] == '0'), 'Mobile'] = None
# com_cont.loc[(com_cont['Primary Email'] == '0'), 'Primary Email'] = None
# com_cont.loc[(com_cont['Contact Brief / Summary'] == '0'), 'Contact Brief / Summary'] = None
# com_cont.loc[(com_cont['Skills (Comma Delimited)'] == '0'), 'Skills (Comma Delimited)'] = None
# com_cont.to_csv('com_cont.csv',index=False)
# %% company
company = pd.read_csv(r'D:\Tony\project\vincere_project\CSV__baillierecruitment\Data\company_import.csv')
contact = pd.read_csv(r'com_cont.csv')
company = company.where(company.notnull(),None)
contact = contact.where(contact.notnull(),None)
company = company.fillna('')
contact = contact.fillna('')


# company_1 = company
# company_1['Location Name'] = company_1['Company Address']
company['rn'] = company.groupby('Company Name').cumcount()
company['rn'] = company['rn'].astype(str)
company['Company Name_1'] = company['Company Name']+'_'+company['rn']
company.to_csv('company_import.csv',index=False)


contact_1 = contact.merge(company, on=['Company Name','Company Address', 'Company ZIP (Postal) Code', 'Company Phone', 'Company Brief / Summary','Company Address 1','Company Address District / Suburb', 'Company Address Town / City','Company Website'])
contact_1.to_csv('contact_import.csv',index=False)