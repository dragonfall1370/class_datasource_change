# -*- coding: UTF-8 -*-
import pymssql
import re
import common.connection_string as cs
import logger.logger as log
import pandas as pd
# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 2000)
pd.set_option('display.width', 1000)


logger = log.get_logger("rlc.log")
fr = cs.client_rlc_prd

sdbconn = pymssql.connect(server=fr.get('server'), user=fr.get('user'), password=fr.get('password'), database=fr.get('database'), as_dict=True)
#
# load sql script from file
logger.info('load sql script from file')
try:
    #
    # company
    company_sql_file = open('script/rlc___company.sql')
    company_sql_script = company_sql_file.read()
    #
    # contact
    contact_sql_file = open('script/rlc___contact.sql')
    contact_sql_script = contact_sql_file.read()
    #
    # candidate
    candidate_sql_file = open('script/rlc___candidate.sql')
    candidate_sql_script = candidate_sql_file.read()
    #
    # job
    job_sql_file = open('script/rlc___job.sql')
    job_sql_script = job_sql_file.read()
    #
    # job application
    jobapplication_sql_file = open('script/rlc___job_application.sql')
    jobapplication_sql_script = jobapplication_sql_file.read()

except KeyError as ke:
    logger.error('KeyError: %s' % ke)
except FileNotFoundError as fnfe:
    logger.error('FileNotFoundError: %s' % fnfe)

output_uploaded_files = 'standard_upload_files/'
#
# using pandas to load data from data connection then write them to csv file   
logger.info('company loading...')
df_company = pd.read_sql(company_sql_script, sdbconn)
#
# clean for ,, in company-locationName: (replace a character in column of dataframe)
# df_company[df_company['company-locationName'].str.contains(",,")]
df_company['company-name'] = df_company['company-name'].str.replace(",,", ",")
df_company['company-locationName'] = df_company['company-locationName'].str.replace(",,", ",")
df_company['company-locationAddress'] = df_company.apply(lambda x: re.sub(r'\r\n', ', ', x['company-locationAddress']), axis=1)
df_company['company-locationAddress'] = df_company['company-locationAddress'].str.replace(",,", ",")
df_company.to_csv(output_uploaded_files + r'rlc_company.csv', index=False, header=True, sep=',')
#
# ------------------------------------------------
logger.info('contact loading...')
df_contact = pd.read_sql(contact_sql_script, sdbconn)
df_contact.to_csv(output_uploaded_files + r'rlc_contact.csv', index=False, header=True, sep=',')
#
# job loading
logger.info('job loading...')
df_job = pd.read_sql(job_sql_script, sdbconn)
# TODO: copy to method processing position-publicDescription
df_job['position-publicDescription'] = df_job['position-publicDescription'].apply(lambda x: re.sub(r'\r\n', '</p><p>', x))
df_job['position-publicDescription'] = df_job['position-publicDescription'].apply(lambda x: re.sub(r'\n<br>', '</p><p>', x))
df_job['position-publicDescription'] = df_job['position-publicDescription'].apply(lambda x: '<p>%s</p>' % x)
df_job.to_csv(output_uploaded_files + r'rlc_job.csv', index=False, header=True, sep=',')

#
# job application
df_jobapplication = pd.read_sql(jobapplication_sql_script, sdbconn)
df_jobapplication.to_csv(output_uploaded_files + r'rlc_job_application.csv', index=False, header=True, sep=',')
# job application separate to: not placement and placement
df_jobapplication_placement = df_jobapplication[df_jobapplication['application-stage'].str.contains('PLACEMENT')]
df_jobapplication_placement['application-stage-orgi'] = df_jobapplication_placement['application-stage']
df_jobapplication_placement['application-stage'] = 'OFFERED'
df_jobapplication_placement.to_csv(output_uploaded_files + r'rlc_job_application_placement.csv', index=False)

df_jobapplication_other = df_jobapplication[~df_jobapplication['application-stage'].str.contains('PLACEMENT')]
df_jobapplication_other.to_csv(output_uploaded_files + r'rlc_job_application_other.csv', index=False)

logger.info('candidate loading...')
df_candidate = pd.read_sql(candidate_sql_script, sdbconn)
#
# check valid email candidate
df_candidate['validate-candidate-owners'] = df_candidate['candidate-owners'].map(lambda x: 'Yes' if (True) and (
        len(re.findall(r"\w\S*@\w*.\S*\w{1,10}", x if (x is not None and x != '') else '')) >= 1) else "No")
#
# assert that if cadidate owners have email, that must be valid: invalid emails of candidates are all none values
assert len(df_candidate[
               (df_candidate['validate-candidate-owners'] == 'No') & (df_candidate['candidate-owners'].notnull()) & (
                   df_candidate['candidate-owners'].notna())]) == 0

df_candidate[(df_candidate['validate-candidate-owners'] == 'No') & (df_candidate['candidate-owners'].isnull())]
df_candidate.to_csv(output_uploaded_files + r'rlc_candidate.csv', index=False, header=True, sep=',')


