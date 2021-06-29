# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import psycopg2
import re
import pymssql
import warnings
from common import vincere_common
import os
import datetime
import common.vincere_standard_migration as vincere_standard_migration
import common.vincere_custom_migration as vincere_custom_migration
import pandas as pd
# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('un_config.ini')
mylog = log.get_info_logger(cf['default'].get('log_file'))
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
review_db = cf[cf['default'].get('dest_db')]

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)


ddbconn = psycopg2.connect(host=review_db.get('server'), user=review_db.get('user'), password=review_db.get('password'), database=review_db.get('database'), port=review_db.get('port'))
ddbconn.set_client_encoding('UTF8')

jobapp_new = pd.read_csv(os.path.join(standard_file_upload, 'jobapplication_placement_new.csv'))
cand_new_skip = pd.read_csv(os.path.join(standard_file_upload, 'skip_file_place.csv'))
cand_new_add = pd.read_csv(os.path.join(standard_file_upload, 'jobapplication_placement_new_2.csv'))

cand_new_skip['matcher'] = cand_new_skip['application-positionExternalId']+cand_new_skip['application-candidateExternalId']
cand_new_add['matcher'] = cand_new_add['application-positionExternalId']+cand_new_add['application-candidateExternalId']
jobap = pd.concat([jobapp_new, cand_new_add])
jobap = jobap.drop_duplicates()
jobap['matcher'] = jobap['application-positionExternalId']+jobap['application-candidateExternalId']
placements_1 = jobap.loc[~jobap['matcher'].isin(cand_new_skip['matcher'])]
placements = pd.concat([placements_1, cand_new_add])
placements = placements.drop_duplicates()
assert False

# load placement
# placements = pd.read_csv(os.path.join(standard_file_upload, '7_jobapplication_placement_0.csv'))
placements['application-positionExternalId'] = placements['application-positionExternalId'].astype(str)
placements['application-candidateExternalId'] = placements['application-candidateExternalId'].astype(str)
placements['application-positionExternalId'] = placements['application-positionExternalId'].str.strip()
placements['application-candidateExternalId'] = placements['application-candidateExternalId'].map(lambda x: str(x).strip())

jobs = pd.read_sql('select id as position_description_id, external_id from position_description', ddbconn)
cands = pd.read_sql('select id as candidate_id, external_id from candidate', ddbconn)

placements = placements.merge(jobs, left_on='application-positionExternalId', right_on='external_id')
placements = placements.merge(cands, left_on='application-candidateExternalId', right_on='external_id')

vincere_custom_migration.move_jobapp_from_offer_to_place(placements, ddbconn)

