# -*- coding: UTF-8 -*-

import psycopg2
import vincere.vincere_custom_migration as vc
import common.connection_string as cs
import logger.logger as log
import pymssql
import connection_string

logger = log.get_logger("private_site.log")

to_mentalhealth = connection_string.production_hojona_mentalhealth
to_socialworkers = connection_string.production_hojona_socialworkers

ddbconn_men = psycopg2.connect(host=to_mentalhealth.get('server'), user=to_mentalhealth.get('user'), password=to_mentalhealth.get('password'), database=to_mentalhealth.get('database'), port=to_mentalhealth.get('port'))
ddbconn_men.set_client_encoding('UTF8')

ddbconn_soc = psycopg2.connect(host=to_socialworkers.get('server'), user=to_socialworkers.get('user'), password=to_socialworkers.get('password'), database=to_socialworkers.get('database'), port=to_socialworkers.get('port'))
ddbconn_soc.set_client_encoding('UTF8')

logger.info('position_candidate is being deleted')
vc.clean_job_application(ddbconn_soc)
logger.info('position_candidate is deleted')
logger.info('candidate is being deleted')
vc.clean_candidate(ddbconn_soc)
logger.info('candidate is deleted')
logger.info('job is being deleted')
vc.clean_job(ddbconn_soc)
logger.info('job is deleted')
logger.info('contact is being deleted')
vc.clean_contact(ddbconn_soc)
logger.info('contact is deleted')
logger.info('company is being deleted')
vc.clean_company(ddbconn_soc)
logger.info('company is deleted')
logger.info('recent_record is being deleted')
vc.clean_recent_record(ddbconn_soc)
logger.info('recent_record is deleted')
logger.info('bulk_upload is being deleted')
vc.clean_bulk_upload(ddbconn_soc)
logger.info('bulk_upload is deleted')

