# -*- coding: UTF-8 -*-
import pandas as pd
import psycopg2
from vincere import vincere_custom_migration
from common import connection_string

fr = connection_string.production_rlc_p35432
ddbconn = psycopg2.connect(host=fr.get('server'), user=fr.get('user'), password=fr.get('password'), database=fr.get('database'), port=fr.get('port'))
ddbconn.set_client_encoding('UTF8')

vincere_custom_migration.run_after_standard_upload('thb', ddbconn)

# load placement
placements = pd.read_csv(r'standard_upload_files/rlc_job_application_placement.csv')
jobs = pd.read_sql('select id as position_description_id, external_id from position_description', ddbconn)
cands = pd.read_sql('select id as candidate_id, external_id from candidate', ddbconn)

placements = placements.merge(jobs, left_on='application-positionExternalId', right_on='external_id')
placements = placements.merge(cands, left_on='application-candidateExternalId', right_on='external_id')

vincere_custom_migration.move_jobapp_from_offer_to_place(placements, ddbconn)