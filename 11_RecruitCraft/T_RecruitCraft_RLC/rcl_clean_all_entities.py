# -*- coding: UTF-8 -*-

import psycopg2
import vincere.vincere_custom_migration as vc
import common.connection_string as cs
import logger.logger as log

logger = log.get_logger("private_site.log")
fr = cs.production_rlc_p35432
ddbconn = psycopg2.connect(host=fr.get('server'), user=fr.get('user'), password=fr.get('password'), database=fr.get('database'), port=fr.get('port'))
ddbconn.set_client_encoding('UTF8')

logger.info('position_candidate is being deleted')
vc.clean_job_application(ddbconn)
logger.info('position_candidate is deleted')

logger.info('candidate is being deleted')
vc.clean_candidate(ddbconn)
logger.info('candidate is deleted')

logger.info('job is being deleted')
vc.clean_job(ddbconn)
logger.info('job is deleted')

logger.info('contact is being deleted')
vc.clean_contact(ddbconn)
logger.info('contact is deleted')

logger.info('company is being deleted')
vc.clean_company(ddbconn)
logger.info('company is deleted')

logger.info('recent_record is being deleted')
vc.clean_recent_record(ddbconn)
logger.info('recent_record is deleted')

logger.info('bulk_upload is being deleted')
vc.clean_bulk_upload(ddbconn)
logger.info('bulk_upload is deleted')

logger.info('unsupper_users are being deleted')
vc.clean_unsupper_users(ddbconn)
logger.info('unsupper_users are deleted')

logger.info('candidate_gdpr_compliance are being deleted')
vc.clean_candidate_gdpr_compliance(ddbconn)
logger.info('candidate_gdpr_compliance are deleted')
