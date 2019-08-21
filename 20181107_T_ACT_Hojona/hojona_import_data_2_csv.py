# -*- coding: UTF-8 -*-
import common.connection_string as cs
import logger.logger as log
import pymssql
import psycopg2
import vincere_common

logger = log.get_logger("hojona.log")
fr_mentalhealth = cs.client_hojona_mentalhealth
fr_socialcare = cs.client_hojona_socialcare
to_mentalhealth = cs.production_hojona_mentalhealth
to_socialworkers = cs.production_hojona_socialworkers

sdbconn_men = pymssql.connect(server=fr_mentalhealth.get('server'), user=fr_mentalhealth.get('user'), password=fr_mentalhealth.get('password'), database=fr_mentalhealth.get('database'), as_dict=True)
sdbconn_soc = pymssql.connect(server=fr_socialcare.get('server'), user=fr_socialcare.get('user'), password=fr_socialcare.get('password'), database=fr_socialcare.get('database'), as_dict=True)
ddbconn_men = psycopg2.connect(host=to_mentalhealth.get('server'), user=to_mentalhealth.get('user'), password=to_mentalhealth.get('password'), database=to_mentalhealth.get('database'), port=to_mentalhealth.get('port'))
ddbconn_soc = psycopg2.connect(host=to_socialworkers.get('server'), user=to_socialworkers.get('user'), password=to_socialworkers.get('password'), database=to_socialworkers.get('database'), port=to_socialworkers.get('port'))

sql_script = open('data_script/men_contact.sql').read()
sql = sql_script + " OFFSET %d ROWS FETCH NEXT %d ROWS ONLY "
vincere_common.read_sql_to_csv1(sql, sdbconn_men, 'data_file/contact_mental_health.csv', offset=0, chunk_size=10000)
sql_script = open('data_script/soc_contact.sql').read()
sql = sql_script + " OFFSET %d ROWS FETCH NEXT %d ROWS ONLY "
vincere_common.read_sql_to_csv1(sql, sdbconn_soc, 'data_file/contact_socialcare.csv', offset=0, chunk_size=10000)

sql_script = open('data_script/men_candidate.sql').read()
sql = sql_script + " OFFSET %d ROWS FETCH NEXT %d ROWS ONLY "
vincere_common.read_sql_to_csv1(sql, sdbconn_men, 'data_file/candidate_mental_health.csv', offset=0, chunk_size=10000)
sql_script = open('data_script/soc_candidate.sql').read()
sql = sql_script + " OFFSET %d ROWS FETCH NEXT %d ROWS ONLY "
vincere_common.read_sql_to_csv1(sql, sdbconn_soc, 'data_file/candidate_socialcare.csv', offset=0, chunk_size=10000)

sql_script = open('data_script/men_company.sql').read()
sql = sql_script + " OFFSET %d ROWS FETCH NEXT %d ROWS ONLY "
vincere_common.read_sql_to_csv1(sql, sdbconn_men, 'data_file/company_mental_health.csv', offset=0, chunk_size=10000)
sql_script = open('data_script/soc_company.sql').read()
sql = sql_script + " OFFSET %d ROWS FETCH NEXT %d ROWS ONLY "
vincere_common.read_sql_to_csv1(sql, sdbconn_soc, 'data_file/company_socialcare.csv', offset=0, chunk_size=10000)
