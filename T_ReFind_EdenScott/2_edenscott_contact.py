
                                                      
"""
/*
Contact import requirement specs: 
https://hrboss.atlassian.net/wiki/spaces/SB/pages/19071424/Requirement+specs+Contact+import
  
--'contact-companyId'
--'contact-firstName'
--'contact-lastName'
--'contact-firstNameKana'--4
--'contact-lastNameKana'--5
--'contact-middleName'--6
--'contact-owners'--Reference to Internal staff. Use Email address to identify users. If the user cannot be found, the record will be skipped. Multiple emails are separated by Commas.
--'contact-jobTitle'--8
--'contact-Note'--10
--'contact-document'--11 filenames
--'contact-phone'--12
--'contact-email'
--'contact-linkedin'--14
--'contact-skype'--15
--'contact-externalId'--16
*/
 
/*************************************** important note ***************************************/
/*
-- History              History > Actions       => Vincere's Activities Comments => on production site
-- Documents => Vincere's       FILES
-- Comments => Vincere's Activities Comments
-- Mail => Vincere's Activities Comments
-- Vacancies => Vincere's jobs
*/
/*************************************** important note ***************************************/

Surname                       Last Name                     CTsname                      contact-lastName
Forenames                     First Name                    CTfnames                     contact-firstName
Position                      Job Title                     CTposition                   contact-jobTitle
Mobile                        Mobile                        CTmtel                       contact-phone
Email 1                       Email                         CTemail                      contact-email
LinkedIn                      Linkedin                      CTlinkedin                   contact-linkedin
Email 2                       Note                          CTemail2                     contact-Note
Email 3                       Note                          CTemail3                     contact-Note
Twitter                       Note                          CTtwitter                    contact-Note
Source                        Note                          CTsource                     contact-Note
GDPR - Send Updates           Note                          CTGDPRupdates                contact-Note
GDPR - Hold Data              Note                          CTGDPRholdData               contact-Note
GDPR - Send Spec CVs          Note                          CTGDPRspecCV                 contact-Note
GDPR - Export Count           Note                          CTGDPRexported               contact-Note
GDPR - First Export           Note                          CTGDPRfirstExport            contact-Note
GDPR - Restricted             Note                          CTGDPRrestricted             contact-Note
GDPR - Date Obtained          Note                          CTGDPRobtained               contact-Note
Office Address                Note                          comps.COadd                  contact-Note
Personal comments             Note                          CTcomments                   contact-Note
By                            Owners                        CTdateby                     contact-owners
Consultant                    Owners                        CTcons                       contact-owners
Company                       Company Id                    CTconum                      contact-companyId
Contact Id                                                  CTnum                        contact-externalId
Direct Line                   Phone                         CTwtel                       
Attachments                   Files                                                      
                                                            CTactive                     contact-Note
                                                                                                                            
                                                                                                                            
                                                                                                                            
Follow this template:
·················
GDPR
·················
Send Updates: dfsf
Hold Data: sfsdf 

"""

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
    df_contact = pd.read_csv(os.path.join(data_input, 'contact.csv'), dtype=dtype_contact, parse_dates=dtype_contact_parse)
    df_contact_st = df_contact[['CTsname', 'CTfnames', 'CTposition', 'CTmtel', 'CTwtel'
                              , 'CTemail', 'CTemail2', 'CTemail3', 'CTlinkedin', 'CTtwitter'
                              , 'CTsource', 'CTGDPRupdates', 'CTGDPRholdData', 'CTGDPRspecCV'
                              , 'CTGDPRexported', 'CTGDPRfirstExport', 'CTGDPRrestricted', 'CTGDPRobtained'
                              , 'CTconum', 'CTcomments', 'CTcons', 'CTdateby', 'CTnum', 'CTactive']]


    df_user_options = pd.read_csv(os.path.join(data_input, 'user_options.csv'))
    df_user_options_st = df_user_options[['UOemail', 'UOloginname', ]]
    df_company = pd.read_csv(os.path.join(data_input, 'company.csv'), dtype=dtype_company, parse_dates=parse_dates_company)
    df_company_st = df_company[['COnum', 'COadd', ]] # select columns
    # FORMAT address
    df_company_st['COadd'] = df_company_st['COadd'].fillna('')
    df_company_st['COadd'] = df_company_st['COadd'].apply(lambda x: x.replace('\n', ','))
    df_company_st['COadd'] = df_company_st['COadd'].apply(lambda x: x.replace('\r', ','))
    df_company_st['COadd'] = df_company_st['COadd'].apply(lambda x: re.sub(r"\,{2,}|,\s$|\,\s*\,{1,}", ',', x))
    df_company_st['COadd'] = df_company_st['COadd'].apply(lambda x: re.sub(r"\,\s{1,}$", '', x))

    # df_contact_st['CTcons'] = df_contact_st.apply(lambda x: x['CTcons'] if x['CTcons'] is not None else x['CTdateby'], axis=1)
    df_contact_st = df_contact_st.merge(df_user_options_st, left_on='CTcons', right_on='UOloginname', how='left')
    df_contact_st = df_contact_st.merge(df_company_st, left_on='CTconum', right_on='COnum', how='left')

    #
    # fill owner info by COdateby@edenscott.com (fake email)) fil na by values of another column values
    df_contact_st['UOemail'].fillna(df_contact_st['CTcons'] + "@edenscott.com", inplace=True)

    df_contact_st.rename(columns={
        'CTsname'       :'contact-lastName',
        'CTfnames'      :'contact-firstName',
        'CTposition'    :'contact-jobTitle',
        'CTmtel'        :'contact-phone',
        'CTemail'       :'contact-email',
        'CTlinkedin'    :'contact-linkedin',
        'UOemail'       :'contact-owners',
        'CTconum'       :'contact-companyId',
        'CTnum'         :'contact-externalId',
        }, inplace=True)

    # contact-lastName must be not empty
    df_contact_st['contact-lastName'].fillna('', inplace=True)
    df_contact_st[df_contact_st['contact-lastName'].str.strip()=='']
    df_contact_st[df_contact_st['contact-lastName'].str.strip()=='[No Lastname]']
    # df_contact_st[df_contact_st['contact-lastName'].str.strip()=='']['contact-lastName']="No surname"
    df_contact_st.loc[df_contact_st['contact-lastName'].str.strip()=='', 'contact-lastName']="[No Lastname]"

    # contact-email
    df_contact_st['contact-email'].fillna('', inplace=True) # fill '' for missing emails
    df_contact_st['contact-email'] = df_contact_st.apply(lambda x: ','.join(
                                                                            set(re.findall(vincere_common.regex_pattern_email, x['contact-email'])) # set does not allow duplicates
                                                                            ), axis=1) # extract valid email, remove dup email of the same contact, join them by ','
    df_contact_st['contact-email'] = df_contact_st.apply(lambda x: ('No_email_' + str(x['contact-externalId']) + '@email.com') if len(x['contact-email'])==0 else x['contact-email'], axis=1) # set fake email for missing values

    df_contact_st_email = vincere_common.splitDataFrameList(df_contact_st[['contact-externalId', 'contact-email']], 'contact-email', ',')
    df_contact_st_email['email_cumcount'] = df_contact_st_email.groupby(df_contact_st_email['contact-email'].str.lower()).cumcount()+1 # group by email case insensitive
    df_contact_st_email['contact-email'] = df_contact_st_email.apply(lambda x: x['contact-email'] if x['email_cumcount']==1 else "%s_%s"%(x['email_cumcount'], x['contact-email']),axis=1) # add prefix for dup contact email
    temp_series = df_contact_st_email.groupby('contact-externalId')['contact-email'].apply(lambda x: ', '.join(x)) # group by groupby_colname, join all the contents by ','
    df_contact_st_email = pd.DataFrame(temp_series).reset_index() #

    df_contact_st.drop('contact-email', axis=1, inplace=True)
    df_contact_st = pd.merge(
        left=df_contact_st,
        right=df_contact_st_email,
        left_on='contact-externalId',
        right_on='contact-externalId',
        how='left'
        )

    # contact-Note
    df_contact_st['contact-Note'] = df_contact_st.apply(
        lambda x: re.sub(r"\s{1,}\n", "",
                    re.sub(r".*:\s{3,}\n", "",
                    re.sub(r".*:\s(nan\n|nan$)", '', '\n'.join([
                '%s %s' % ('Contact External Id:', x['contact-externalId']),
                '%s %s' % ('Contact Status:', x['CTactive']),
                '%s %s' % ('Email2:', x['CTemail2']),
                '%s %s' % ('Email3:', x['CTemail3']),
                '%s %s' % ('Twitter:', x['CTtwitter']),
                '%s %s' % ('Source:', x['CTsource']),
                '·······················',
                'GDPR',
                '·······················',
                '%s %s' % ('Send Updates:', ('YES' if x['CTGDPRupdates'] else 'NO')),
                '%s %s' % ('Hold Data:', ('YES' if x['CTGDPRholdData'] else 'NO')),
                '%s %s' % ('Send Spec CVs:', ('YES' if x['CTGDPRspecCV'] else 'NO')),
                '%s %s' % ('Export Count:', ('YES' if x['CTGDPRexported'] else 'NO')),
                '%s %s' % ('First Export:', ('YES' if x['CTGDPRfirstExport'] else 'NO')),
                '%s %s' % ('Restricted:', ('YES' if x['CTGDPRrestricted'] else 'NO')),
                '%s %s' % ('Date Obtained:', x['CTGDPRobtained']),
                '·······················',

                '%s %s' % ('Office Address:', x['COadd']),
                '%s %s' % ('Personal comments:', x['CTcomments']),

                ])
            )
        ) )
        , axis=1)

    ''' write the output to file '''
    df = df_contact_st.filter(regex='contact')
    df.to_csv(os.path.join(standard_file_upload, 'edenscott_contact.csv'), index=False, header=True, sep=",")