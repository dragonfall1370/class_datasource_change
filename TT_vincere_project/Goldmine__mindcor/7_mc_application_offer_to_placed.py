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
cf.read('mc_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
src_db = cf[cf['default'].get('src_db')]
sqlite_path = cf['default'].get('sqlite_path')
dest_db = cf[cf['default'].get('dest_db')]
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
ddbconn = psycopg2.connect(host=dest_db.get('server'), user=dest_db.get('user'), password=dest_db.get('password'), database=dest_db.get('database'), port=dest_db.get('port'))
ddbconn.set_client_encoding('UTF8')

# load placement
placements = pd.read_csv(os.path.join(standard_file_upload, '7_jobapplication_placement.csv'))
placements['application-positionExternalId'] = placements['application-positionExternalId'].astype(str)
placements['application-candidateExternalId'] = placements['application-candidateExternalId'].astype(str)
placements['application-positionExternalId'] = placements['application-positionExternalId'].str.strip()
placements['application-candidateExternalId'] = placements['application-candidateExternalId'].map(lambda x: str(x).strip())

jobs = pd.read_sql('select id as position_description_id, external_id from position_description', ddbconn)
cands = pd.read_sql('select id as candidate_id, external_id from candidate', ddbconn)

placements = placements.merge(jobs, left_on='application-positionExternalId', right_on='external_id')
placements = placements.merge(cands, left_on='application-candidateExternalId', right_on='external_id')

vincere_custom_migration.move_jobapp_from_offer_to_place(placements, ddbconn)

