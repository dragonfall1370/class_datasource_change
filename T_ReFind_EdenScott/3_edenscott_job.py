
"""
/* 
 * Requirement specs: Job import:
 * https://hrboss.atlassian.net/wiki/spaces/SB/pages/19071420/Requirement+specs+Job+import
 */
--'position-contactId'--1
--'position-title' --2  -must be unique
--'position-headcount'--3 --number-default value = 1
--'position-owners'--4
--'position-type'--5 PERMANENT, INTERIM_PROJECT_CONSULTING, TEMPORARY, CONTRACT. TEMPORARY_TO_PERMANENT--default PERMANENT
--'position-employmentType'--6 --FULL_TIME, PART_TIME, CASUAL --default FULL_TIME
--'position-comment'--7
--'position-currency'--8---http://www.currency-iso.org/en/home/tables/table-a1.html
--'position-actualSalary'--9
--'position-payRate'--10
--'position-contractLength'--11
--'position-publicDescription'--12
--'position-Description'--13
--'position-internalDescription'--14
--'position-externalId' --15
--'position-startDate'--16 yyyy-mm-dd
--'position-endDate'--17 yyyy-mm-dd
--'position-note' --18
--'position-document'--19

=========================================================================
ID                  External Id                           VAnum                                                           position-externalId
Type                Type                                  VAtype (P: Permanent, C:Contract, T: Temporary)                 position-type
Type                Type                                  VAtype (P: Permanent, C:Contract, T: Temporary)                 position-employmentType
Job Title           Title                                 VAjob                                                           position-title
Consultant          Owners                                VAdateBy                                                        position-owners (useroption.email)
Consultant 2        Owners                                VAcon2                                                          position-owners (useroption.email)
Salary              Actual Salary                         VAsal (VAvalue=VAsal*VAper/100)                                 position-actualSalary
Required            Headcount                             VArequired                                                      position-headcount
Client              External Contact Id                   VAcont                                                          position-contactId
Benefits            Internal Job Description              VAbenefits                                                      position-internalDescription
ID                  Job Notes with label "Refind Job Ref:"VAnum                                                           position-note
Source              Note                                  VAsource                                                        position-note
Notes               Note                                  VAcomm                                                          position-note
Company             External Company Id                   VAconum                                                         
Attachments         Files                                                                                                 
=========================================================================
                                                          VAchargecurr                                                    position-currency
                                                                                                                          position-comment
                                                                                                                          
Vacancy Created                                           VAdate                                                          position-startDate
                                                          VAcldat                                                         position-endDate
Status                                                    VAstatus (Live...)
                                                          VAactive (True/False)

"""


# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import re
import datetime
from dateutil.relativedelta import relativedelta
from edenscott._edenscott_dtypes import *
import os
import common.vincere_common as vincere_common
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
fr = cf[cf['default'].get('src_db')]
mylog = log.get_info_logger(log_file)
#
# create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

if __name__ == '__main__':
    df_job = pd.read_csv(os.path.join(data_input, 'job.csv'), dtype=dtype_job, parse_dates=dtype_job_date_parse)
    df_job_st = df_job[['VAnum', 'VAtype', 'VAjob', 'VAdateBy', 'VAcon2', 'VAsal', 'VArequired', 'VAcont', 'VAbenefits', 'VAsource', 'VAcomm', 'VAconum', 'VAchargecurr', 'VAdate', 'VAcldat', 'VAstatus', 'VAactive',]]

    df_user_options = pd.read_csv(os.path.join(data_input, 'user_options.csv'))
    df_user_options_st = df_user_options[['UOemail', 'UOloginname', ]]
    df_user_options_st = df_user_options_st[df_user_options_st['UOemail'] != ' ']
    df_job_st = df_job_st.merge(df_user_options_st, left_on='VAdateBy', right_on='UOloginname', how='left')

    #
    # fill owner info by COdateby@edenscott.com (fake email)) fil na by values of another column values
    df_job_st['UOemail'].fillna(df_job_st['VAdateBy'] + "@edenscott.com", inplace=True)
    df_job_st['UOemail'] = df_job_st['UOemail'].map(lambda x: re.search(r'[a-zA-Z0-9_][^\\\r\n\t\f\v ]*@[a-zA-Z0-9.-]*\w{1,}', x).group(0))
    # for x in df_job_st['UOemail']:
    #     a = re.search(r'[a-zA-Z0-9_][^\\\r\n\t\f\v ]*@[a-zA-Z0-9.-]*\w{1,}', x);
    #     if a == None:
    #         print("check %s" % x)


    df_job_st['position-type'] = [vincere_common.set_position_type(x) for x in df_job_st['VAtype']] # create new column
    df_job_st['position-employmentType'] = [vincere_common.set_position_employment_type(x) for x in df_job_st['VAtype']] # create new column
    df_job_st['position-currency'] = [vincere_common.map_currency_code(x, 'gbp').upper() for x in df_job_st['VAchargecurr']] # create new column

    df_job_st['position-note'] = df_job_st.apply(
        lambda x: re.sub(r"\n\s{2,}", "",
                    re.sub(r".*:\s{3,}\n", "",
                    re.sub(r".*:\s(nan\n|nan$)", '', '\n'.join([
                '%s %s' % ('Refind Job Ref:', x['VAnum']),
                '%s %s' % ('Source:', x['VAsource']),
                '%s %s' % ('Notes:', x['VAcomm']),
                ])
            )
        )
                    ).replace('\r', '')
        , axis=1)


    df_job_st.rename(columns={
        'VAnum'          :'position-externalId',
        'VAjob'          :'position-title',
        'UOemail'        :'position-owners',
        'VAsal'          :'position-actualSalary',
        'VArequired'     :'position-headcount',
        'VAcont'         :'position-contactId',
        'VAconum'        :'position-companyId',
        'VAbenefits'     :'position-internalDescription',
        'VAdate'         :'position-startDate',
        'VAcldat'        :'position-endDate',
        }, inplace=True)

    # active job
    df_job_st['position-endDate'] = df_job_st.apply(lambda x: datetime.datetime.now()+relativedelta(years=+1) if ((str(x['position-endDate'])=='NaT') & (x['VAactive'])) else x['position-endDate'], axis=1)
    df_job_st['position-startDate'] = df_job_st['position-startDate'].dt.date
    df_job_st['position-endDate'] = df_job_st['position-endDate'].dt.date

    df_job_st['position-actualSalary'].fillna(0, inplace=True)
    df_job_st['position-actualSalary'] = df_job_st['position-actualSalary'].astype(np.int64)
    #
    # process prosition title duplicated
    df_job_st['position-title'] = df_job_st['position-title'].str.strip()
    # df_job_st['position-checkdup'] = [str(x['position-title']).lower() + str(x['position-startDate']) for index, x in df_job_st.iterrows()]
    # df_job_st['position-title-rn'] = df_job_st.groupby(df_job_st['position-checkdup']).cumcount()+1
    # df_job_st['position-title'] = ["%s_%i" % (x['position-title'], x['position-title-rn']) if x['position-title-rn']>1 else x['position-title'] for index, x in df_job_st.iterrows()]
    df_job_st['position-checkdup'] = [str(x['position-title']).lower() for index, x in df_job_st.iterrows()]
    df_job_st['position-title-rn'] = df_job_st.groupby(df_job_st['position-checkdup']).cumcount() + 1
    df_job_st['position-title'] = ["%s_%s" % (x['position-title'], x['position-externalId']) if x['position-title-rn'] > 1 else x['position-title'] for index, x in df_job_st.iterrows()]

    ''' write the output to file '''
    df = df_job_st.filter(regex='position')
    df.to_csv(os.path.join(standard_file_upload, 'edenscott_job.csv'), index=False, header=True, sep=",")


