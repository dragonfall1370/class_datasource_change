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

# conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
# engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
# ddbconn = engine_postgre_review.raw_connection()
#
# cand = pd.read_csv(os.path.join(standard_file_upload, 'skip_file (4).csv'))
# tem1 = pd.DataFrame(cand['application-candidateExternalId'].value_counts().keys(), columns=['cand_externalid'])
# candidate = pd.read_sql("""
# select p.ID
#      , p.FirstName
#      , p.Surname
#      , p.MiddleName
#      , c.PersonalEMail
#      , o.WorkEMail as owner ,c.IsArchived, p.IsArchived
# from Person p
# join Candidate c on p.ID = c.ID
# left join Person o on c.OwnerPersonID = o.ID
# where  c.IsArchived = 0
# """, engine_sqlite)
# tem1 = tem1.astype(str)
# candidate = candidate.merge(tem1, left_on='ID', right_on='cand_externalid')
# tem1.loc[~tem1['cand_externalid'].isin(candidate['ID'])]

candidate = pd.read_sql("""
 select p.ID
      , p.FirstName
      , p.Surname
      , p.MiddleName
      , c.PersonalEMail
      , o.WorkEMail as owner ,c.IsArchived, p.IsArchived
 from Person p
 join Candidate c on p.ID = c.ID
 left join Person o on c.OwnerPersonID = o.ID
 where  c.IsArchived = 0 and p.ID = 19296
 """, engine_sqlite)

assert False
# %% transpose
candidate.rename(columns={
    'ID': 'candidate-externalId',
    'FirstName': 'candidate-firstName',
    'MiddleName': 'candidate-middleName',
    'Surname': 'candidate-lastName',
    'PersonalEMail': 'candidate-email',
    'owner': 'candidate-owners',
}, inplace=True)
candidate = vincere_standard_migration.process_vincere_cand(candidate, mylog)

# %% to csv files
candidate.to_csv(os.path.join(standard_file_upload, '6.2_candidate.csv'), index=False)