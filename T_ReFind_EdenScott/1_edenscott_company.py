"""
Company import requirement specs: 
https://hrboss.atlassian.net/wiki/spaces/SB/pages/19071426/Requirement+specs+Company+import

'company-externalId'                   COnum
'company-name'                         COname
'company-locationAddress'              COadd
'company-locationName'                 2
'company-locationCountry'              1, does not work, custom script
'company-locationState'                
'company-locationCity'                 
'company-locationDistrict'             
'company-locationZipCode'              COpc
'company-nearestTrainStation'          
'company-headQuater'                   
'company-switchBoard'                  COtel
'company-phone'                        COtel
'company-fax'                          COfax
'company-website'                      COweb (length 100)
'company-owners'                       UserOptions.UOemail how to join to company : COdateby????
'company-document'                     Attachments????
'company-note'                         COnum
                                                                                                 
1:
Select SubClass, Comps.*
from Comps
left join ClassSelect  on COnum=Reference 
where ClassName = '(EP1) Country'
                                                                                                 
2:
company-locationName= company_city + ' ' + company-locationCountry + ' ' + company-locationZipCode
                                                                                                 
 * ----------------------------------------------------------------------------------------------
 * don't add or map columns that require value is no in the field mapping
 * check if any contacts have no companies to referer to, then add a default company
 * replace concat/coalesce by concat_ws 
 * replace stuff by string_agg
 * ----------------------------------------------------------------------------------------------
 * Activities Comments: migrate by spoon
 * 
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

    df_company = pd.read_csv(os.path.join(data_input,'company.csv'), dtype=dtype_company, parse_dates=parse_dates_company)
    df_company_st = df_company[['COnum', 'COname', 'COadd', 'COpc', 'COtel', 'COfax', 'COweb', 'COdateby', 'COGDPRjobEnforce', 'CObus', ]] # select columns
    # COinfo: company comment

    df_user_options = pd.read_csv(os.path.join(data_input,'user_options.csv'))
    df_user_options_st = df_user_options[['UOemail', 'UOloginname', ]]

    '''
    join to user options to get user login email (as owner info)
    , remove unnescessary columns
    , renames columns following to template
    '''
    df_company_st = df_company_st.merge(df_user_options_st, left_on='COdateby', right_on='UOloginname', how='left')
    #
    # fill owner info by COdateby@edenscott.com (fake email)) fil na by values of another column values
    df_company_st['UOemail'].fillna(df_company_st['COdateby']+"@edenscott.com", inplace=True)

    ''' drop unnecessary columns '''
    df_company_st.drop('COdateby', axis=1, inplace=True) # drop / remove / delete column
    df_company_st.drop('UOloginname', axis=1, inplace=True) # drop / remove / delete column

    ''' renames one column '''
    df_company_st.rename(columns={'COnum':'company-externalId'}, inplace=True)
    df_company_st.rename(columns={'COname':'company-name'}, inplace=True)
    df_company_st.rename(columns={'COadd':'company-locationAddress'}, inplace=True)
    df_company_st.rename(columns={'COpc':'company-locationZipCode'}, inplace=True)
    df_company_st.rename(columns={'COtel':'company-switchBoard'}, inplace=True)
    df_company_st.rename(columns={'COfax':'company-fax'}, inplace=True)
    df_company_st.rename(columns={'COweb':'company-website'}, inplace=True)
    df_company_st.rename(columns={'UOemail':'company-owners'}, inplace=True)

    '''
    assign company-phone equal to company-switchBoard
    '''
    df_company_st['company-phone'] = df_company_st['company-switchBoard']
    '''
    assign company-note contains external company id
    '''
    # df_company_st['company-note'] = 'Company External Id: ' + df_company_st['company-externalId']
    df_company_st['company-note'] = df_company_st.apply(
        lambda x: re.sub(r"\s{1,}\n", "",
                    re.sub(r".*:\s{3,}\n", "",
                    re.sub(r".*:\s(nan\n|nan$)", '', '\n'.join([
                '%s %s' % ('Company External Id:', x['company-externalId']),                                    # Company External Id
                '%s %s' % ('Enforce GDPR per Job:', ('YES' if x['COGDPRjobEnforce'] else 'NO')),                # Enforce GDPR per Job
                '%s %s' % ('Business:', x['CObus']),                                                            # Business
            ]) )))
        , axis=1)

    # CLEAN1: companies names must be unique
    df_company_st['company-name'] = df_company_st['company-name'].str.strip()
    df_company_st['rn']=df_company_st.groupby(df_company_st['company-name'].str.lower()).cumcount()+1 # group by string case insensitive
    df_company_st['company-name'] = df_company_st.apply(lambda x: x['company-name'] if x['rn']==1 else '%s_%s' % (x['company-name'], x['rn']), axis=1)

    # FORMAT address
    df_company_st['company-locationAddress'] = df_company_st['company-locationAddress'].fillna('')
    df_company_st['company-locationAddress'] = df_company_st['company-locationAddress'].apply(lambda x: x.replace('\n', ','))
    df_company_st['company-locationAddress'] = df_company_st['company-locationAddress'].apply(lambda x: x.replace('\r', ','))
    df_company_st['company-locationAddress'] = df_company_st['company-locationAddress'].apply(lambda x: re.sub(r"\,{2,}|,\s$|\,\s*\,{1,}", ',', x))
    df_company_st['company-locationAddress'] = df_company_st['company-locationAddress'].apply(lambda x: re.sub(r"\,\s{1,}$", '', x))

    ''' write the output to file '''
    df_company_st.to_csv(os.path.join(standard_file_upload, 'edenscott_company.csv'), index=False, header=True, sep=",")


