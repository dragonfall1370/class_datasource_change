

"""
/*
 * ref
 * https://hrboss.atlassian.net/wiki/spaces/SB/pages/18284908/Requirement+specs+Candidate+import
 * 
'candidate-title'--4 -MR, MRS, MS, MISS, DR
'candidate-Lastname' --6
'candidate-firstName
'candidate-firstNameKana'--7
'candidate-lastNameKana'--8
'candidate-middleName'--9
'candidate-dob'--10 --yyyy-mm-dd
'candidate-citizenship'--11 --country code check --http://www.nationsonline.org/oneworld/country_code_list.htm
'candidate-owner'
'candidate-email'--12--unique email address
'candidate-workEmail'--13
'candidate-phone'--14 --Personal Phone, Primary Phone
'candidate-mobile'--15
'candidate-homePhone'--16
'candidate-workPhone'--17
'candidate-address'--18
'candidate-city' --19
'candidate-country'--29
'candidate-zipcode'--30
'candidate-Sate'--31
'candidate-currency'--32--currency code check http://www.currency-iso.org/en/home/tables/table-a1.html
'candidate-currentSalary'--33
'candidate-desiredSalary'--34
'candidate-contractRate'--35
'candidate-contractInterval'--36--HOURS, DAYS, WEEKS, MONTHS, YEARS
'candidate-education'--37
'candidate-educationLevel'--38 --DEGREE, DIPLOMA, DOCTORATE, HIGH_SCHOOL_GRADUATE, ITE_TECH_TEACHING_CERT, INCOMPLETE_SEC_EDUCATION,MASTER,NOA, NO_FORMAL_EDUCATION, POST_GRAD_DIPLOMA, PRIMARY, PROFESSIONAL_QUALIFICATION
'candidate-schoolName' --39
'candidate-graduationDate'--40 --yyyy-mm-dd
'candidate-degreename' --41
'candidate-grade' --42
'candidate-gpa'--43
'candidate-numberOfEmployers'--44
'candidate-workHistory'--45
'candidate-company1' --46
'candidate-company2' --46
'candidate-company3' --46
'candidate-jobTitle1'
'candidate-jobTitle2'
'candidate-jobTitle3'
'candidate-jobType' --PERMANENT, INTERIM_PROJECT_CONSULTING, TEMPORARY, CONTRACT. TEMPORARY_TO_PERMANENT--default PERMANENT
'candidate-startDate1'
'candidate-startDate2'
'candidate-startDate3'
'candidate-endDate1'
'candidate-endDate2'
'candidate-endDate3'
'candidate-skills'
'candidate-keyword'
'candidate-note'
'candidate-photo'
'candidate-resume'
'candidate-gender'--MALE, FEMALE, BLANK-> Please select
'candidate-employmentType'--FULL_TIME, PART_TIME, CASUAL, LABOUR_HIRE
'candidate-comments'
'candidate-linkedin'
'candidate-externalId'
*/
=========================================================================
CAnum                     External ID AND Candidate                     CAnum                      candidate-externalId
Forenames                 First Name                                    CAfnames                   candidate-firstName
Surname                   Lastname                                      CAsname                    candidate-Lastname
Mobile                    Mobile / Phone                                CAmtel                     candidate-phone
Home Tel                  Home Phone                                    CAhtel                     candidate-homePhone
Work Line / Work Tel      Phone 2                                       CAwtel                     candidate-workPhone
Postcode                  Zip Code                                      CApc                       candidate-zipcode
Consultant                Owners                                        CAcons                     candidate-owner (useroption.email)
Nationality               Citizenship                                   CAnat                      candidate-citizenship
Gender                    Gender                                        CAsex                      candidate-gender
Salary                    Current Salary                                CAslry                     candidate-currentSalary
Rate                      Current Salary                                CArate                     candidate-currentSalary ???
Employer                  Company1                                      CAemp + CAempadd           candidate-company1
Title                     Title                                         CAtit                      candidate-title
Address                   Address                                       CAadd                      candidate-address
Email 1                   Email                                         CAemail                    candidate-email
Position                  Job title                                     CAcpos                     candidate-jobTitle1
Position                  Title                                         CAcpos                     
LinkedIn                  Linkedln                                      CAlinkedin                 candidate-linkedin
Perm                      Job Types                                     CAperm                     candidate-jobType
Temp/Cont                 Job Types                                     CAtemp                     candidate-jobType
CAnum                     Notes with label "Refind Candidate Ref:"      CAnum                      candidate-note
Salutation                Note                                          CAsalu                     candidate-note
Email 2                   Note                                          CAemail2                   candidate-note
Email 3                   Note                                          CAemail3                   candidate-note
Website                   Note                                          CAweb                      candidate-note
Facebook                  Note                                          CAfacebook                 candidate-note
Twitter                   Note                                          CAtwitter                  candidate-note
Location                  Note                                          CAloc                      candidate-note
Availability              Note                                          CAava                      candidate-note
Comments                  Note                                          CAcomments                 candidate-note
Source                    Note                                          CAad                       candidate-note
Notice                    Note                                          CAnotice                   candidate-note
Warning                   Note                                          TBC                         
Attachments               Resume                                                                    
=========================================================================
                                                                        CAdob                      candidate-dob
Is Working                                                              CAworking                  
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
    t0 = datetime.datetime.now()
    df_candidate = pd.read_csv(os.path.join(data_input, 'candidate.csv'), dtype=dtype_cand, parse_dates=dtype_cand_date_parse)
    mylog.info("Read candidate.csv: %s" % (datetime.datetime.now() - t0))
    df_user_options = pd.read_csv(os.path.join(data_input, 'user_options.csv'))
    df_user_options_st = df_user_options[['UOemail', 'UOloginname', ]]
    df_user_options_st = df_user_options_st[df_user_options_st['UOemail'] != ' ']

    mylog.info('Candidate note is processing...')
    t0 = datetime.datetime.now()
    df_candidate_note_prefix = ['Refind Candidate Ref:', 'Salutation:', 'Email2:', 'Email3:', 'Website:', 'Facebook:',
                                'Twitter:', 'Location:', 'Availability:', 'Comments:', 'Source:', 'Notice:', ]
    df_candidate_note = [
        pd.DataFrame({'CAnum': df_candidate['CAnum'], 'ExternalId': df_candidate['CAnum']}),
        df_candidate[['CAnum', 'CAsalu']],
        df_candidate[['CAnum', 'CAemail2']],
        df_candidate[['CAnum', 'CAemail3']],
        df_candidate[['CAnum', 'CAweb']],
        df_candidate[['CAnum', 'CAfacebook']],
        df_candidate[['CAnum', 'CAtwitter']],
        df_candidate[['CAnum', 'CAloc']],
        df_candidate[['CAnum', 'CAava']],
        df_candidate[['CAnum', 'CAcomments']],
        df_candidate[['CAnum', 'CAad']],
        df_candidate[['CAnum', 'CAnotice']],
    ]
    df_candidate_note = vincere_common.clean_duplicate_dropna_stripwhitespace_join_v1(df_candidate_note, df_candidate_note_prefix, 'CAnum', 'candidate-note')
    mylog.info("Candidate note completed in: %s" % (datetime.datetime.now() - t0))

    df_candidate_st = df_candidate[
        ['CAnum', 'CAfnames', 'CAsname', 'CAmtel', 'CAhtel', 'CAwtel', 'CApc', 'CAcons', 'CAnat', 'CAsex', 'CAslry',
         'CAemp', 'CAempadd', 'CAtit', 'CAadd', 'CAemail', 'CAcpos', 'CAlinkedin', 'CAperm', 'CAtemp', 'CAdob', ]]
    df_candidate_st = df_candidate_st.merge(df_user_options_st, left_on='CAcons', right_on='UOloginname', how='left')

    #
    # fill owner info by COdateby@edenscott.com (fake email)) fil na by values of another column values
    df_candidate_st['UOemail'].fillna(df_candidate_st['CAcons'] + "@edenscott.com", inplace=True)
    df_candidate_st['UOemail'] = df_candidate_st['UOemail'].map(lambda x: re.search(r'[a-zA-Z0-9_][^\\\r\n\t\f\v ]*@[a-zA-Z0-9.-]*\w{1,}', x).group(0))  # email can not contain \

    df_candidate_st['candidate-jobType'] = [
        'PERMANENT' if x['CAperm'] else ('CONTRACT' if x['CAtemp'] else 'TEMPORARY_TO_PERMANENT') for index, x in
        df_candidate_st.iterrows()]
    df_candidate_st['candidate-company1'] = [
        '%s %s' % ('' if str(x['CAemp']) == 'nan' else x['CAemp'], '' if str(x['CAempadd']) == 'nan' else x['CAempadd']) for
        index, x in df_candidate_st.iterrows()]
    df_candidate_st.rename(columns={
        'CAnum': 'candidate-externalId',
        'CAfnames': 'candidate-firstName',
        'CAsname': 'candidate-Lastname',
        'CAmtel': 'candidate-phone',
        'CAhtel': 'candidate-homePhone',
        'CAwtel': 'candidate-workPhone',
        'CApc': 'candidate-zipcode',
        'UOemail': 'candidate-owners',
        'CAnat': 'candidate-citizenship',
        'CAsex': 'candidate-gender',
        'CAslry': 'candidate-currentSalary',
        'CAtit': 'candidate-title',
        'CAadd': 'candidate-address',
        'CAemail': 'candidate-email',
        'CAcpos': 'candidate-jobTitle1',
        'CAlinkedin': 'candidate-Linkedin',
        'CAdob': 'candidate-dob',
    }, inplace=True)

    df_candidate_st = df_candidate_st.merge(df_candidate_note, left_on='candidate-externalId', right_on='CAnum', how='left')
    df_candidate_st = vincere_standard_migration.process_vincere_cand(df_candidate_st, mylog)

    ''' write the output to file '''
    df = df_candidate_st.filter(regex='^candidate')

    # t0 = datetime.now()
    # df_comp_files.to_csv(r'standard_migration/edenscott_candidate.csv', index=False, header=True, sep=",")
    # mylog.info('Time to write the csv: %s' % (datetime.now() - t0))

    # dict_of_df = {g: x for g, x in df_comp_files.groupby('candidate-gender')}
    # for f, d in dict_of_df.items():
    #     d.to_csv(r'standard_migration/edenscott_candidate_gender_{}.csv'.format(f), index=False, header=True, sep=",")

    max_rows = 50000
    dataframes = []
    while len(df) > max_rows:
        top = df[:max_rows]
        dataframes.append(top)
        df = df[max_rows:]
    else:
        dataframes.append(df)
    for _, frame in enumerate(dataframes):
        frame.to_csv(os.path.join(standard_file_upload, 'edenscott_candidate_{0}_{1}.csv'.format(_, len(frame))), index=False, header=True, sep=",")






