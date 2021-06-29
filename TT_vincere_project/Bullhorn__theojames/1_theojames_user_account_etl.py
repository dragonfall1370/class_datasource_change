# -*- coding: UTF-8 -*-
import configparser
import os
import pathlib

import pandas as pd
import psycopg2
import sqlalchemy

import common.logger_config as log
import common.vincere_custom_migration as vincere_custom_migration
from common import vincere_common

# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('theo_config.ini')
mylog = log.get_info_logger(cf['default'].get('log_file'))
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
sqlite_path = cf['default'].get('sqlite_path')
src_db = cf[cf['default'].get('src_db')]

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect to database
ddbconn = psycopg2.connect(host=dest_db.get('server'), user=dest_db.get('user'), password=dest_db.get('password'), database=dest_db.get('database'), port=dest_db.get('port'))
ddbconn.set_client_encoding('UTF8')
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')

# %% extract data
duser_account = pd.read_sql("""
select * from user_account;
""", ddbconn)

duser_account.columns = ['vincere_{}'.format(x) for x in duser_account.columns]

suser_account = pd.read_sql("""
select distinct 
  email as FirstName 
  , '' as LastName 
  , email 
from (
select email from cand_owner
union 
select email from cont_owner
union
select email from job_owner
) t
""", engine_sqlite)

# assert False

# %% transform data
checkuser = suser_account.merge(duser_account, left_on='email', right_on='vincere_email', how='outer', indicator=True, suffixes=('_x', '_y'))
checkuser = checkuser.loc[checkuser['_merge'].isin(['both', 'left_only'])]
checkuser['vincere_approver_flag'] = checkuser['vincere_approver_flag'].apply(lambda x: 0 if str(x) == 'nan' else x)
checkuser['vincere_email_connected'] = checkuser['vincere_email_connected'].apply(lambda x: 0 if str(x) == 'nan' else x)
checkuser['vincere_view_mode'] = checkuser['vincere_view_mode'].apply(lambda x: 0 if str(x) == 'nan' else x)
checkuser['vincere_synced_to_cognito'] = checkuser['vincere_synced_to_cognito'].apply(lambda x: 0 if str(x) == 'nan' else x)
checkuser['vincere_pin_comment'] = checkuser['vincere_pin_comment'].apply(lambda x: 0 if str(x) == 'nan' else x)
checkuser['vincere_exchange_version'] = checkuser['vincere_exchange_version'].apply(lambda x: 0 if str(x) == 'nan' else x)
checkuser['vincere_researcher'] = checkuser['vincere_researcher'].apply(lambda x: 0 if str(x) == 'nan' else x)
checkuser['vincere_consultant'] = checkuser['vincere_consultant'].apply(lambda x: 0 if str(x) == 'nan' else x)
checkuser['vincere_manager'] = checkuser['vincere_manager'].apply(lambda x: 0 if str(x) == 'nan' else x)
checkuser['vincere_director'] = checkuser['vincere_director'].apply(lambda x: 0 if str(x) == 'nan' else x)
checkuser['vincere_length_email_password'] = checkuser['vincere_length_email_password'].apply(lambda x: 0 if str(x) == 'nan' else x)
checkuser['vincere_length_calendar_password'] = checkuser['vincere_length_calendar_password'].apply(lambda x: 0 if str(x) == 'nan' else x)
checkuser['vincere_system_admin'] = checkuser['vincere_system_admin'].apply(lambda x: 0 if str(x) == 'nan' else x)
checkuser['vincere_business_partner'] = checkuser['vincere_business_partner'].apply(lambda x: 0 if str(x) == 'nan' else x)
checkuser['vincere_matrix_manager'] = checkuser['vincere_matrix_manager'].apply(lambda x: 0 if str(x) == 'nan' else x)
checkuser['vincere_interviewer'] = checkuser['vincere_interviewer'].apply(lambda x: 0 if str(x) == 'nan' else x)
checkuser['vincere_hr'] = checkuser['vincere_hr'].apply(lambda x: 0 if str(x) == 'nan' else x)
checkuser['vincere_line_manager'] = checkuser['vincere_line_manager'].apply(lambda x: 0 if str(x) == 'nan' else x)
checkuser['vincere_internal_recruiter'] = checkuser['vincere_internal_recruiter'].apply(lambda x: 0 if str(x) == 'nan' else x)
checkuser['vincere_super_user'] = checkuser['vincere_super_user'].apply(lambda x: 0 if str(x) == 'nan' else x)
checkuser['vincere_locked_user'] = checkuser['vincere_locked_user'].apply(lambda x: 1 if str(x) == 'nan' else x)
checkuser['vincere_requisition_signatory'] = checkuser['vincere_requisition_signatory'].apply(lambda x: 1 if str(x) == 'nan' else x)
checkuser['vincere_display_intro'] = checkuser['vincere_display_intro'].apply(lambda x: 1 if str(x) == 'nan' else x)
checkuser['vincere_candidate_quick_view_tab'] = checkuser['vincere_candidate_quick_view_tab'].apply(lambda x: 1 if str(x) == 'nan' else x)
checkuser['vincere_new_search'] = checkuser['vincere_new_search'].apply(lambda x: 1 if str(x) == 'nan' else x)
checkuser['vincere_display_training'] = checkuser['vincere_display_training'].apply(lambda x: 1 if str(x) == 'nan' else x)
checkuser['vincere_default_document_view'] = checkuser['vincere_default_document_view'].apply(lambda x: 1 if str(x) == 'nan' else x)
checkuser['vincere_default_candidate_view'] = checkuser['vincere_default_candidate_view'].apply(lambda x: 1 if str(x) == 'nan' else x)
checkuser['vincere_authentication'] = checkuser['vincere_authentication'].apply(lambda x: 1 if str(x) == 'nan' else x)
checkuser['vincere_ssl_tls'] = checkuser['vincere_ssl_tls'].apply(lambda x: 1 if str(x) == 'nan' else x)
checkuser['vincere_internal_staff'] = checkuser['vincere_internal_staff'].apply(lambda x: 1 if str(x) == 'nan' else x)
checkuser['vincere_job_dashboard_default_view'] = checkuser['vincere_job_dashboard_default_view'].apply(lambda x: 2 if str(x) == 'nan' else x)
checkuser['vincere_email_api'] = checkuser['vincere_email_api'].apply(lambda x: 'STANDARD' if str(x) == 'nan' else x)
checkuser['vincere_default_renewal_stage'] = checkuser['vincere_default_renewal_stage'].apply(lambda x: 'RENEWAL' if str(x) == 'nan' else x)
checkuser['vincere_mail_status'] = checkuser['vincere_mail_status'].apply(lambda x: 'ENABLED' if str(x) == 'nan' else x)
checkuser['vincere_distance_unit'] = checkuser['vincere_distance_unit'].apply(lambda x: 'km' if str(x) == 'nan' else x)
checkuser['vincere_position_of_box'] = checkuser['vincere_position_of_box'].apply(lambda x: '1,2,3,4,5,6' if str(x) == 'nan' else x)
checkuser['vincere_insert_timestamp'] = checkuser['vincere_insert_timestamp'].apply(lambda x: vincere_common.my_insert_timestamp if str(x) == 'NaT' else x)
checkuser['vincere_outgoing_authentication'].fillna(0, inplace=True)
checkuser['vincere_outgoing_encrypt_type'].fillna(0, inplace=True)

# values come from the client side
checkuser['vincere_user_location'].fillna('United States', inplace=True)
checkuser['vincere_timezone'].fillna('Canada/Pacific', inplace=True)
checkuser['vincere_email'] = checkuser.apply(lambda x: x['email'] if str(x['vincere_email']) == 'nan' else x['vincere_email'], axis=1)
checkuser['vincere_first_name'] = checkuser.apply(lambda x: x['FirstName'] if str(x['vincere_first_name']) == 'nan' else x['vincere_first_name'], axis=1)
checkuser['vincere_first_name'].fillna('', inplace=True)
checkuser['vincere_last_name'] = checkuser.apply(lambda x: x['LastName'] if str(x['vincere_last_name']) == 'nan' else x['vincere_last_name'], axis=1)
checkuser['vincere_last_name'].fillna('', inplace=True)
checkuser['vincere_name'] = checkuser.apply(lambda x: x['email'], axis=1)

checkuser = checkuser.filter(regex='^vincere_')
checkuser.columns = [x.replace('vincere_', '') for x in checkuser.columns]
checkuser_update = checkuser.loc[checkuser.id.notnull()]
checkuser_update['id'] = checkuser_update['id'].astype(int)
checkuser_update.drop(checkuser_update.columns[checkuser_update.isnull().sum() == checkuser_update.shape[0]], inplace=True, axis=1)
checkuser_insert = checkuser.loc[checkuser.id.isnull()].drop('id', axis=1)

# %% load data to vincere
if len(checkuser_update) > 0:
    vincere_custom_migration.psycopg2_bulk_update_tracking(checkuser_update, ddbconn, [x for x in checkuser_update.columns.drop('id')], ['id'], 'user_account', mylog)

# %% process dup email, none email, none lastname
checkuser_insert.loc[checkuser_insert['email'].isnull(), 'email'] = 'no_email@no_email.co'
checkuser_insert['rn'] = checkuser_insert.groupby('email').cumcount()
checkuser_insert.loc[checkuser_insert['rn']>0, 'email'] = checkuser_insert.loc[checkuser_insert['rn']>0][['email', 'rn']].apply(lambda x: '{}_{}'.format(x[1], x[0]), axis=1)
checkuser_insert = checkuser_insert.drop('rn', axis=1)
checkuser_insert.loc[checkuser_insert['last_name'].isnull(), 'last_name'] = checkuser_insert.loc[checkuser_insert['last_name'].isnull(), 'email']
if len(checkuser_insert):
    vincere_custom_migration.psycopg2_bulk_insert_tracking(checkuser_insert, ddbconn, checkuser_insert.columns, 'user_account', mylog)
