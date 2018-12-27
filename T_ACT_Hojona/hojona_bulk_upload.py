# -*- coding: UTF-8 -*-

import pymssql
import psycopg2
import logger.logger
import connection_string
from hojona_util import hojona_bulk_upload_process
import vincere_custom_migration

mylog = logger.logger.get_logger('hojona.log')

fr_mentalhealth = connection_string.client_hojona_mentalhealth
fr_socialcare = connection_string.client_hojona_socialcare
to_mentalhealth = connection_string.production_hojona_mentalhealth
to_socialworkers = connection_string.production_hojona_socialworkers

sdbconn_men = pymssql.connect(server=fr_mentalhealth.get('server'), user=fr_mentalhealth.get('user'), password=fr_mentalhealth.get('password'), database=fr_mentalhealth.get('database'), as_dict=True)
ddbconn_men = psycopg2.connect(host=to_mentalhealth.get('server'), user=to_mentalhealth.get('user'), password=to_mentalhealth.get('password'), database=to_mentalhealth.get('database'), port=to_mentalhealth.get('port'))

sdbconn_soc = pymssql.connect(server=fr_socialcare.get('server'), user=fr_socialcare.get('user'), password=fr_socialcare.get('password'), database=fr_socialcare.get('database'), as_dict=True)
ddbconn_soc = psycopg2.connect(host=to_socialworkers.get('server'), user=to_socialworkers.get('user'), password=to_socialworkers.get('password'), database=to_socialworkers.get('database'), port=to_socialworkers.get('port'))

# df1 = hojona_bulk_upload_process.comp_attachment_bulk_upload(sdbconn_men, ddbconn_men)
# df2 = hojona_bulk_upload_process.cont_attachment_bulk_upload(sdbconn_men, ddbconn_men)
# df3 = hojona_bulk_upload_process.cand_attachment_bulk_upload(sdbconn_men, ddbconn_men)

df1 = hojona_bulk_upload_process.comp_attachment_bulk_upload(sdbconn_soc, ddbconn_soc)
df2 = hojona_bulk_upload_process.cont_attachment_bulk_upload(sdbconn_soc, ddbconn_soc)
df3 = hojona_bulk_upload_process.cand_attachment_bulk_upload(sdbconn_soc, ddbconn_soc)