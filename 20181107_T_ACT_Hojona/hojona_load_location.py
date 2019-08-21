# -*- coding: UTF-8 -*-
import vincere_custom_migration
import pandas as pd
import logger.logger
import connection_string
import pymssql
import psycopg2


mylog = logger.logger.get_logger('hojona.log')

fr_mentalhealth = connection_string.client_hojona_mentalhealth
fr_socialcare = connection_string.client_hojona_socialcare
to_mentalhealth = connection_string.production_hojona_mentalhealth
to_socialworkers = connection_string.production_hojona_socialworkers
sdbconn_men = pymssql.connect(server=fr_mentalhealth.get('server'), user=fr_mentalhealth.get('user'), password=fr_mentalhealth.get('password'), database=fr_mentalhealth.get('database'), as_dict=True)
ddbconn_men = psycopg2.connect(host=to_mentalhealth.get('server'), user=to_mentalhealth.get('user'), password=to_mentalhealth.get('password'), database=to_mentalhealth.get('database'), port=to_mentalhealth.get('port'))
sdbconn_soc = pymssql.connect(server=fr_socialcare.get('server'), user=fr_socialcare.get('user'), password=fr_socialcare.get('password'), database=fr_socialcare.get('database'), as_dict=True)
ddbconn_soc = psycopg2.connect(host=to_socialworkers.get('server'), user=to_socialworkers.get('user'), password=to_socialworkers.get('password'), database=to_socialworkers.get('database'), port=to_socialworkers.get('port'))


df = pd.read_sql(""" select id, address, post_code from common_location where id in (select current_location_id from candidate) """, ddbconn_men)
vincere_custom_migration.load_location_by_postcost(df, 'id', 'address', 'post_code', 'output_standard_files/men_cand_location.csv', mylog)

df = pd.read_sql(""" select id, address, post_code from common_location where id in (select current_location_id from candidate) """, ddbconn_soc)
vincere_custom_migration.load_location_by_postcost(df, 'id', 'address', 'post_code', 'output_standard_files/soc_cand_location.csv', mylog)

