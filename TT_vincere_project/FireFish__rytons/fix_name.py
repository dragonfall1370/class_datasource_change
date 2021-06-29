# -*- coding: UTF-8 -*-
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
cf.read('rt_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)
sqlite_path = cf['default'].get('sqlite_path')
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% dest db
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
# %% dest db
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
ddbconn = engine_postgre_review.raw_connection()
assert False
# %% backup db
conn_str_ddb_bkup = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format('postgres', '123456', 'dmpfra.vinceredev.com', '5432', 'rytons_backup_restored')
engine_postgre_bkup = sqlalchemy.create_engine(conn_str_ddb_bkup, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
ddbconn_bkup = engine_postgre_bkup.raw_connection()
# assert False

comp_prod = pd.read_sql("""select id, name from company where external_id is null""", engine_postgre_review)
comp_prod['name'] = comp_prod['name'].apply(lambda x: x+'_NEW')
vincere_custom_migration.psycopg2_bulk_update_tracking(comp_prod, ddbconn, ['name'], ['id'], 'company', mylog)

cont_prod = pd.read_sql("""select id, last_name from contact where external_id is null""", engine_postgre_review)
cont_prod['last_name'] = cont_prod['last_name'].apply(lambda x: x+'_NEW')
vincere_custom_migration.psycopg2_bulk_update_tracking(cont_prod, ddbconn, ['last_name'], ['id'], 'contact', mylog)

cand_prod = pd.read_sql("""select id, last_name from candidate where external_id is null""", engine_postgre_review)
cand_prod['last_name'] = cand_prod['last_name'].apply(lambda x: x+'_NEW')
vincere_custom_migration.psycopg2_bulk_update_tracking(cand_prod, ddbconn, ['last_name'], ['id'], 'candidate', mylog)

cand_prod1 = pd.read_sql("""select external_id, id,first_name, last_name from candidate where external_id is not null""", engine_postgre_review)
cand_prod1['rn'] = cand_prod1.groupby('external_id').cumcount()
cand_prod1.loc[cand_prod1['rn'] != 0]

# %% backup db
cand_prod = pd.read_sql("""select
cl.id as common_location_id,
c.id as candidate_id,
c.external_id as candidate_external_id,
c.first_name,
c.middle_name,
c.last_name,
cl.address as candidate_address,
cl.state as candidate_state,
cl.city as candidate_city,
cl.country_code as candidate_country_code,
cl.post_code as candidate_post_code, ce.current_employer, comp.*
from candidate c
join common_location cl on c.current_location_id=cl.id
join candidate_extension ce on c.id=ce.candidate_id
left join (
select
cl.id as company_location_id
, c.id as Vincere_Company_ID
, c.name as Company_Name
, cl.address as Company_Address
, cl.state as Company_State
, cl.city as Company_City
, cl.post_code as Company_Postal_ZIP_Code
, cl.country_code as Company_Country_Code
from company c
join company_location cl on c.id = cl.company_id) comp on comp.Company_Name = ce.current_employer
""", engine_postgre_review)
cand_id = pd.read_csv('C:\\Users\\tony\\Downloads\\Rytons_candidate_locations_20200511 - Rytons_candidate_locations.csv')
cand_prod = cand_prod.loc[cand_prod['candidate_id'].isin(cand_id['candidate_id'])]
cand_prod.to_csv('Rytons_canidate_company_location_mapping_2.csv', index=False)