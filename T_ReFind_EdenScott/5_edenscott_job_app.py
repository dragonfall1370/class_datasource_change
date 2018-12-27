
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
    print("current working environment: %s" % os.getcwd())

    import warnings
    t0 = datetime.datetime.now()
    df_job_app = pd.read_csv(os.path.join(data_input, 'job_app.csv'))
    mylog.info("Read job_app.csv: %s" % (datetime.datetime.now() - t0))

    df_job_app_st = df_job_app.loc[df_job_app['Status'].notnull()]
    df_job_app_st.rename(columns={
        'Vacancy': 'application-positionExternalId',
        'Candidate': 'application-candidateExternalId',
        'Status': 'application-stage',
        }, inplace=True)

    #ignore warning:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df_job_app_st = vincere_standard_migration.process_vincere_jobapp(df_job_app_st)

    # job application separate to: not placement and placement
    df_jobapplication_placement = df_job_app_st[df_job_app_st['application-stage'].str.contains('PLACEMENT')]
    df_jobapplication_placement['application-stage-orgi'] = df_jobapplication_placement['application-stage']
    df_jobapplication_placement['application-stage'] = 'OFFERED'
    df_jobapplication_other = df_job_app_st[~df_job_app_st['application-stage'].str.contains('PLACEMENT')]

    # df_jobapplication_placement.to_csv(output_uploaded_files + r'rlc_job_application_placement.csv', index=False)
    # df_jobapplication_other.to_csv(output_uploaded_files + r'rlc_job_application_other.csv', index=False)

    # list_of_dfs = vincere_common.df_split_to_listofdfs(df_job_app_st)
    # for _, frame in enumerate(list_of_dfs):
    #     frame.to_csv(os.path.join(standard_file_upload, 'edenscott_job_app_{0}_{1}.csv'.format(_, len(frame))), index=False, header=True, sep=",")
    list_of_dfs = vincere_common.df_split_to_listofdfs(df_jobapplication_placement)
    for _, frame in enumerate(list_of_dfs):
        frame.to_csv(os.path.join(standard_file_upload, 'edenscott_job_app_placement_{0}_{1}.csv'.format(_, len(frame))), index=False, header=True, sep=",")
    list_of_dfs = vincere_common.df_split_to_listofdfs(df_jobapplication_other)
    for _, frame in enumerate(list_of_dfs):
        frame.to_csv(os.path.join(standard_file_upload, 'edenscott_job_app_other_{0}_{1}.csv'.format(_, len(frame))), index=False, header=True, sep=",")