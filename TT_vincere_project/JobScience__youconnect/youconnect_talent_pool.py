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
import datetime
# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('yc_config.ini')
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

# %% connect db
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre.raw_connection()

from common import vincere_candidate
vcand = vincere_candidate.Candidate(engine_postgre.raw_connection())
assert False
# %% create talent pool
data = [['No Touch Candidates', ' ']]
cand_group = pd.DataFrame(data, columns=['name', 'owner'])
cp = vcand.create_talent_pool(cand_group, mylog)

# %% add candidate to talent pool
cand_group_cand = pd.read_sql("""
select
      c.Id as candidate_externalid,
      c.No_Touch__c
from Contact c
join RecordType r on (c.RecordTypeId || 'AA2') = r.Id
left join User u on c.OwnerId = u.Id
left join "User" m on c.LastModifiedById = m.Id
where c.IsDeleted = 0
and r.Name = 'Candidate'
""", engine_sqlite)
# assert False
cand_group_cand = cand_group_cand.loc[cand_group_cand['No_Touch__c'] == '1']
cand_group_cand = cand_group_cand.drop_duplicates()
cand_group_cand.candidate_externalid = cand_group_cand.candidate_externalid.astype(str)
cand_group_cand['group_name'] = 'No Touch Candidates'
cp1 = vcand.add_candidate_talent_pool(cand_group_cand, mylog)
