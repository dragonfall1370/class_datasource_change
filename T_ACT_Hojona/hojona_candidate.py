# -*- coding: UTF-8 -*-

import pandas as pd
import re
from hojona_util import hojona_candidate_process
import logger.logger
import vincere_custom_migration
import psycopg2
import connection_string
import pymssql

"""
ID                                         YES        Contact External ID                   CONTACTID                                   candidate-externalId
Company                                    YES        Company External ID                   
Contact                                    YES        Contact External ID                   

                                                                                            FIRST_NAME                                  candidate-firstName
                                                                                            LAST_NAME                                   candidate-Lastname


Phone                                      YES        Primary Phhone                        PHONE                                       candidate-workPhone
Phone Ext                                  YES        Primary Phone                         Work_EXTENSION                              candidate-workPhone
Mobile                                     YES        Mobile Phone                          MOBILE_PHONE                                candidate-phone
Personal Contact Info > Home Phone         YES        Home Phone                            HOME_PHONE                                  candidate-homePhone

Birthday                                   YES        Date of Birth                         BIRTH_DATE                                  candidate-dob

E-mail                                     YES        Primary Email Address                 E-MAIL                                      candidate-email
E-mail 2                                   YES        Personal Email Address                LAST_E-MAIL (ALWAYS NULL)
Personal Contact Info > Pers. E-Mail       YES        Personal Email Address                PERSONAL_E-MAIL (ALWAYS NULL)

Web Site                                   YES        Website                               WEB_SITE                                    candidate.website
Title (pulldown menu)                      YES        Job Title                             TITLE                                       candidate-jobTitle1
OPPORTUNITIES                              YES        JOBS                                  (NO OPPORTUNITIES NOW)
Department (pulldown menu)                 YES        Department                            DEPARTMENT

Service Group (pulldown menu)              YES        Brief                                 USER2                                       candidate-note
Team Name                                  YES        Brief                                 USER3                                       candidate-note
ID / Status                                YES        Brief                                 CATEGORY                                    candidate-note
Specialism 1                               YES        Brief                                 USER4                                       candidate-note
Specialism 2                               YES        Brief                                 USER5                                       candidate-note
DBS Number                                 YES        Brief                                 USER6                                       candidate-note
DBS Issue Date                             YES        Brief                                 USER7                                       candidate-note
DBS Counter Signatory                      YES        Brief                                 USER8                                       candidate-note
How did you hear about Hojona              YES        Brief                                 USER9                                       candidate-note
NI Number                                  YES        Brief                                 NINumber                                    candidate-note
HCPC Number                                YES        Brief                                 HCPCNumber                                  candidate-note
HCPC Expiry                                YES        Brief                                 HCPCExpiry                                  candidate-note
Pay Rate                                   YES        Brief                                 PayRate                                     candidate-note
Referred By                                YES        Brief                                 REFERRED_BY                                 candidate-note
Miscellaneous > Spouse                     YES        Brief                                 SPOUSE                                      candidate-note
Grade                                      YES        Grade                                 USER1                                       candidate-note
E-mail (disabled date field)               YES        Brief                                 
Call Attempt (disabled)                    YES        Brief                                 
Cll Reach (disabled)                       YES        Brief                                 
Meeting (disabled)                         YES        Brief                                 
Letter Sent (disabled)                     YES        Brief                                 
WEB INFO                                   YES        Brief                                 
SECONDARY CONTACTS                         YES        Brief                                 
Personal Contact Info > Alt. Phone         YES        Brief                                 
Personal Contact Info > Pager              YES        Brief                                 
Personal Contact Info > Messenger ID       YES        Brief                                 
Contact > Record Manager                   YES        Brief                                 
History > Create Date                      YES        Brief                                 
History > Record Creator                   YES        Brief                                 
History > Edit Date                        YES        Brief                                 
History > Edited By                        YES        Brief                                 
Access Level > Contact Access              YES        Brief                                 

NOTES                                      YES        Activities Comments                   
ACTIVITIES                                 YES        Activities Comments                   
HISTORY                                    YES        Activities Comments                   
TIMELINE                                   YES        Activities Comments                   
                    
GROUPS / COMPANIES                         YES        Distribution Lists                    

Address 1                                  YES        Current Location Address              ADDRESS_1                                   candidate-address
Address 1                                  YES        Current Location Name                 ADDRESS_1                                   candidate-address
Address 2                                  YES        Current Location Address              ADDRESS_2                                   candidate.address2
Address 2                                  YES        Current Location Name                 ADDRESS_2                                   candidate.address2
City                                       YES        Current Location Address              CITY                                        candidate-city
City                                       YES        Current Location Name                 CITY                                        candidate-city
County                                     YES        Current Location Address              STATE                                       candidate-State
County                                     YES        Current Location Name                 STATE                                       candidate-State
Postcode                                   YES        Current Location Address              ZIP_CODE                                    candidate-zipcode
Postcode                                   YES        Current Location Name                 ZIP_CODE                                    candidate-zipcode
Country                                    YES        Current Location Address              COUNTRY                                     candidate-country
Country                                    YES        Current Location Name                 COUNTRY                                     candidate-country
Home Address > Address 1                   YES        Current Location Address              HOME_ADDRESS_1 (always null)
Home Address > Address 2                   YES        Current Location Address              HOME_ADDRESS_2 (always null)
Home Address > City                        YES        Current Location Address              HOME_CITY (always null)
Home Address > County                      YES        Current Location Address              HOME_STATE (always null)
Home Address > Country                     YES        Current Location Address              HOME_COUNTRY (always null)
Home Address > Post                        YES        Current Location Address              HOME_ZIP_CODE (always null)
Home Address > Address 1                   YES        Current Location Name                 HOME_ADDRESS_1 (always null)
Home Address > Address 2                   YES        Current Location Name                 HOME_ADDRESS_2 (always null)
Home Address > City                        YES        Current Location Name                 HOME_CITY (always null)
Home Address > Country                     YES        Current Location Name                 HOME_COUNTRY (always null)
Home Address > Post                        YES        Current Location Name                 HOME_ZIP_CODE (always null)
Home Address > County                      YES        Current Location Name                 HOME_STATE (always null)
Home Address > County                      YES        Current Location County               HOME_STATE (always null)
Home Address > City                        YES        Current Location Town / City          HOME_CITY
Home Address > Country                     YES        Current Location Country              HOME_COUNTRY (always null)

County                                     YES        Current Location County               STATE                                       candidate-State
City                                       YES        Current Location Town / City          CITY                                        candidate-city
Postcode                                   YES        Current Location Postcode             ZIP_CODE                                    candidate-zipcode
Home Address > Post                        YES        Current Location Postal (ZIP) Code    HOME_ZIP_CODE                               candidate-zipcode
Country                                    YES        Current Location Country              COUNTRY                                     candidate-country

USER FIELDS                                YES        N/A
DOCUMENTS                                  NO         FILES
RELATIONSHIPS                              NO         N/A
"""
mylog = logger.logger.get_logger('hojona.log')

df_cand_men = pd.read_csv('data_file/candidate_mental_health.csv')
df_cand_soc = pd.read_csv('data_file/candidate_socialcare.csv')
df_cand_men.rename(columns=lambda x: re.sub(r' ', '', x), inplace=True)  # replace all space ' ' characters in columns names by ''
df_cand_soc.rename(columns=lambda x: re.sub(r' ', '', x), inplace=True)  # replace all space ' ' characters in columns names by ''

df_cand_men_standard = hojona_candidate_process.gen_candidate_standardfile(df_cand_men, mylog)
df_cand_soc_standard = hojona_candidate_process.gen_candidate_standardfile(df_cand_soc, mylog)

df_cand_men_standard.to_csv('output_standard_files/men_cand.csv', index=False)
df_cand_soc_standard.to_csv('output_standard_files/soc_cand.csv', index=False)



