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
# company_location = pd.read_csv('company_location.csv')
# company_location = company_location.drop_duplicates()
# candidate_location = pd.read_csv('candidate_location.csv')
# candidate_location = candidate_location.drop_duplicates()
# candidate_location['id'] = candidate_location['id'].astype(int)
# company_location['id'] = company_location['id'].astype(int)
# assert False
# vincere_custom_migration.psycopg2_bulk_update_tracking(candidate_location, ddbconn, ['post_code', ], ['id', ], 'common_location', mylog)
# vincere_custom_migration.psycopg2_bulk_update_tracking(candidate_location, ddbconn, ['country_code', ], ['id', ], 'common_location', mylog)
# vincere_custom_migration.psycopg2_bulk_update_tracking(company_location, ddbconn, ['post_code', ], ['id', ], 'company_location', mylog)
# vincere_custom_migration.psycopg2_bulk_update_tracking(company_location, ddbconn, ['country_code', ], ['id', ], 'company_location', mylog)


# cand_location = pd.read_sql("""
# select
# cl.id,
# c.id as candidate_id,
# c.external_id as candidate_external_id,
# c.first_name,
# c.middle_name,
# c.last_name,
# nullif(cl.location_name,'') as location_name,
# nullif(cl.address,'') as address,
# nullif(cl.district,'') as district,
# nullif(cl.state,'') as state,
# nullif(cl.city,'') as city,
# nullif(cl.country,'') as country,
# nullif(cl.country_code,'') as country_code,
# nullif(cl.post_code,'') as post_code
# from candidate c
# join common_location cl on c.current_location_id=cl.id
# where nullif(cl.location_name, '') is null
# """, engine_postgre_review)
# cand_location['location_name'] = cand_location[['address', 'district', 'city', 'state', 'post_code', 'country_code']] \
#    .apply(lambda x: ', '.join([e for e in x if e]), axis=1)
# cand_location['address'] = cand_location.location_name
# vincere_custom_migration.psycopg2_bulk_update_tracking(cand_location, ddbconn, ['location_name','address', ], ['id', ], 'common_location', mylog)
#
# comp_location = pd.read_sql("""
# select
# cl.id
# , c.id as Vincere_Company_ID
# , c.name as Company_Name
# , nullif(cl.location_name,'') as location_name
# , nullif(cl.address,'') as address
# , nullif(cl.state,'') as state
# , nullif(cl.city,'') as city
# , nullif(cl.post_code,'') as post_code
# , nullif(cl.country_code,'') as country_code
# from company c
# join company_location cl on c.id = cl.company_id
# where nullif(cl.location_name, '') is null
# """, engine_postgre_review)
# comp_location['location_name'] = comp_location[['address', 'city', 'state', 'post_code', 'country_code']] \
#    .apply(lambda x: ', '.join([e for e in x if e]), axis=1)
# comp_location['address'] = comp_location.location_name
# vincere_custom_migration.psycopg2_bulk_update_tracking(comp_location, ddbconn, ['location_name','address', ], ['id', ], 'company_location', mylog)


cand_location = pd.read_sql("""
select cl.id as common_location_id,
c.id as candidate_id,
c.external_id as candidate_externalid,
c.first_name,
c.last_name, c.note as brief,
nullif(cl.location_name,'') as location_name,
nullif(cl.address,'') as address,
nullif(cl.state,'') as state,
nullif(cl.city,'') as city,
nullif(cl.country_code,'') as country_code
from candidate c
join common_location cl on c.current_location_id=cl.id
where nullif(cl.location_name, '') is null
and c.note like '%Rytons ID%'
""", engine_postgre_review.raw_connection())

cand_location_ori = pd.read_sql("""
select p.ID as candidate_externalid
     , rl."Level 1" as location
     , rl."Level 2" as sub_location
from Person p
join Candidate c on p.ID = c.ID
left join Person o on c.OwnerPersonID = o.ID
left join rytons_mapping_location as rl on (lower(rl."Secondary ID")) = (lower(c.LocationWSIID))
""", engine_sqlite)

cand_location1 = cand_location.merge(cand_location_ori, on='candidate_externalid')
cand_location1['id'] = cand_location1['common_location_id']
cand_location1['city'] = cand_location1['sub_location']
cand_location1 = cand_location1.loc[cand_location1['location'] != 'International']
cand_location1 = cand_location1.loc[cand_location1['location'] != 'Middle East']
cand_location1['country_code'] = 'GB'
cand_location1['location_name'] = cand_location1[['city', 'country_code']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
cand_location1['address'] = cand_location1['location_name']
vincere_custom_migration.psycopg2_bulk_update_tracking(cand_location1, ddbconn, ['location_name','address','city','country_code' ], ['id', ], 'common_location', mylog)