# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import re
from edenscott._edenscott_dtypes import *
import os
import psycopg2
import common.vincere_common as vincere_common
import common.vincere_custom_migration as vincere_custom_migration
import common.vincere_standard_migration as vincere_standard_migration
import pandas as pd
# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)
#
# loading configuration
cf = configparser.RawConfigParser()
cf.read('_edenscott_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
fr = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)
#
# create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

if __name__ == '__main__':
    ddbconn = psycopg2.connect(host=fr.get('server'), user=fr.get('user'), password=fr.get('password'), database=fr.get('database'), port=fr.get('port'))
    ddbconn.set_client_encoding('UTF8')

    vincere_custom_migration.run_after_standard_upload('gbp', ddbconn)

    # load placement
    placements = pd.read_csv(os.path.join(standard_file_upload, 'edenscott_job_app_placement_0_8378.csv'))
    placements['application-positionExternalId'] = placements['application-positionExternalId'].str.strip()
    placements['application-candidateExternalId'] = placements['application-candidateExternalId'].map(lambda x: str(x).strip())
if True:
    jobs = pd.read_sql('select id as position_description_id, external_id from position_description', ddbconn)
    cands = pd.read_sql('select id as candidate_id, external_id from candidate', ddbconn)

    placements = placements.merge(jobs, left_on='application-positionExternalId', right_on='external_id')
    placements = placements.merge(cands, left_on='application-candidateExternalId', right_on='external_id')

    vincere_custom_migration.move_jobapp_from_offer_to_place(placements, ddbconn)