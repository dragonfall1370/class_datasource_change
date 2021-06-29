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
cf.read('bo_config.ini')
log_file = cf['default'].get('log_file')
# data_folder = cf['default'].get('data_folder')
data_folder = '/Users/truongtung/Desktop'

data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
dest_db = cf[cf['default'].get('dest_db')]
sqlite_path = cf['default'].get('sqlite_path')
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()

from common import vincere_company
vcom = vincere_company.Company(connection)
from common import vincere_contact
vcont = vincere_contact.Contact(connection)
# %% info
contact = pd.read_sql("""
select * from contact
""", engine_sqlite)
contact['contact_externalid'] = contact['Contact External ID']
assert False
# %% job title
jobtitle = contact[['contact_externalid', 'Job Title']].dropna().drop_duplicates()
jobtitle['job_title'] = jobtitle['Job Title']
vcont.update_job_title2(jobtitle, dest_db, mylog)

# %% primary phone
tem = contact[['contact_externalid', 'Primary phone']].dropna().drop_duplicates()
tem['primary_phone'] = tem['Primary phone']
vcont.update_primary_phone(tem, mylog)

# %% social
tem = contact[['contact_externalid', 'linkedinURL']].dropna().drop_duplicates()
tem['linkedin'] = tem['linkedinURL']
vcont.update_linkedin(tem, mylog)

# %% skype
tem = contact[['contact_externalid', 'Skype']].dropna().drop_duplicates()
tem['skype'] = tem['Skype']
vcont.update_skype(tem, mylog)

# %% middle name
tem = contact[['contact_externalid', 'Middle name']].dropna().drop_duplicates()
tem['middle_name'] = tem['Middle name']
vcont.update_middle_name(tem, mylog)

# %% note
tem = contact[['contact_externalid', 'Note','Comment']].drop_duplicates()
tem['note'] = tem[['Note','Comment']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
tem = tem.loc[tem['note'] != '']
cp7 = vcont.update_note_2(tem, dest_db, mylog)

# %% create distribution list
data = [['Healthcare', 'karl@bodhiresourcing.com']]
cont_group = pd.DataFrame(data, columns=['name', 'owner'])
cp = vcont.create_distribution_list(cont_group, mylog)

# %% add contact to distribution list
cont_group_cont = contact[['contact_externalid']]
cont_group_cont['group_name'] = 'Healthcare'
cp1 = vcont.add_contact_distribution_list(cont_group_cont, mylog)

# %% set work location
company_location = pd.read_sql('select * from company_location;', vcont.ddbconn)
contact_p = pd.read_sql('select company_id, id as contact_id, external_id as contact_externalid from contact', vcont.ddbconn)
contact_p = contact_p.loc[contact_p['contact_externalid'].isin(contact['contact_externalid'])]
contact_location = pd.read_sql('select * from contact_location', vcont.ddbconn)
# find companies have 1 location
pd.DataFrame(company_location.groupby('company_id').count().query("id==1"))
company_location['rn'] = company_location.groupby('company_id').cumcount()
company_location = company_location.query("rn == 0")

# companies_has_one_loc = company_location.groupby('company_id').count().query("id==1")
companies_has_one_loc = company_location  # get the first location
companies_has_one_loc.reset_index(level=0, inplace=True)

# find contacts belong to companies have one location
contact_of_companies_have_one_loc = contact_p.merge(companies_has_one_loc[['company_id']], on='company_id')

# find contacts had not been assigned location by default
contact_of_companies_have_one_loc_but_not_assigned_loc = contact_of_companies_have_one_loc.query("contact_id not in @contact_location.contact_id")

# assign default location for contacts have no default location
company_location = company_location[['id', 'company_id']]
company_location.rename(columns={'id': 'company_location_id'}, inplace=True)
contact_of_companies_have_one_loc_but_not_assigned_loc = contact_of_companies_have_one_loc_but_not_assigned_loc.merge(company_location, on='company_id')
contact_of_companies_have_one_loc_but_not_assigned_loc['insert_timestamp'] = datetime.datetime.now()
vincere_custom_migration.psycopg2_bulk_insert_tracking(contact_of_companies_have_one_loc_but_not_assigned_loc, vcont.ddbconn, ['contact_id', 'insert_timestamp', 'company_location_id'],'contact_location', mylog)