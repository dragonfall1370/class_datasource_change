# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import re
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

    df_edenscott_company = pd.read_csv(os.path.join(standard_file_upload, 'edenscott_company.csv'))
    df_edenscott_contact = pd.read_csv(os.path.join(standard_file_upload, 'edenscott_contact.csv'))
    df_edenscott_job = pd.read_csv(os.path.join(standard_file_upload, 'edenscott_job.csv'))

    df_test = pd.merge(
        left=df_edenscott_company,
        right=df_edenscott_contact,
        left_on='company-externalId',
        right_on='contact-companyId',
        how='outer', indicator=True
        )

    # company no contact
    df_test[df_test._merge=='left_only']
    # contact no company
    df_test[df_test._merge=='right_only']

    df_company_no_contact = df_test[(df_test['company-externalId'].notnull()) & (df_test['contact-externalId'].isnull())]
    df_contact_no_company = df_test[(df_test['contact-externalId'].notnull()) & (df_test['company-externalId'].isnull())]
    assert len(df_contact_no_company)==0, "Some contacts have no corresponding company. A dummy company should be created."

    df_test = pd.merge(
        left=df_edenscott_job,
        right=df_edenscott_contact,
        left_on='position-contactId',
        right_on='contact-externalId',
        how='outer', indicator=True
        )
    df_test._merge.unique()
    # job no contact
    df_test[df_test._merge == 'left_only']

    # jobs dont have any corresponding contacts: 139
    len(df_test[(df_test['position-externalId'].notnull()) & (df_test['contact-externalId'].isnull())])
    df_job_no_contact = df_test[(df_test['position-externalId'].notnull()) & (df_test['contact-externalId'].isnull())]
    df_contact_dummies = df_job_no_contact[['position-contactId', 'position-companyId', ]].drop_duplicates()
    df_contact_dummies['position-contactId'] = df_contact_dummies['position-contactId'].astype(np.int64)
    df_contact_dummies['position-companyId'] = df_contact_dummies['position-companyId'].astype(np.int64)
    df_contact_dummies['contact-companyId'] = df_contact_dummies['position-companyId']
    df_contact_dummies['contact-externalId'] = df_contact_dummies['position-contactId']
    df_contact_dummies['contact-lastName'] = ['[DUMMY]' + str(x) for x in df_contact_dummies['position-contactId']]
    df_contact_dummies['contact-firstName'] = ['[DUMMY]' + str(x) for x in df_contact_dummies['position-contactId']]
    df_contact_dummies['contact-jobTitle'] = ['[DUMMY]' + str(x) for x in df_contact_dummies['position-contactId']]
    df_contact_dummies['contact-email'] = ['dummy_email' + str(x) + '@noemail.com' for x in df_contact_dummies['position-contactId']]
    df_contact_dummies = df_contact_dummies.filter(regex='^contact')
    #
    # check contact_dummies again company:
    df_test = pd.merge(
        left=df_edenscott_company,
        right=df_contact_dummies,
        left_on='company-externalId',
        right_on='contact-companyId',
        how='outer', indicator=True
    )
    # contact no company
    df_company_dummies = df_test[df_test._merge == 'right_only'][['contact-companyId', ]].drop_duplicates()
    df_company_dummies['contact-companyId'] = df_company_dummies['contact-companyId'].astype(np.int64)
    df_company_dummies['company-externalId'] = df_company_dummies['contact-companyId']
    df_company_dummies['company-name'] = df_company_dummies['contact-companyId'].map(lambda x: '[DUMMY]' + str(x))
    df_company_dummies['company-locationAddress'] = df_company_dummies['contact-companyId'].map(lambda x: '[DUMMY]' + str(x))
    df_company_dummies = df_company_dummies.filter(regex='^company')


    df_contact_dummies.to_csv(os.path.join(standard_file_upload, 'edenscott_contact_dummies.csv'), index=False, header=True, sep=",")
    df_company_dummies.to_csv(os.path.join(standard_file_upload, 'edenscott_company_dummies.csv'), index=False, header=True, sep=",")




