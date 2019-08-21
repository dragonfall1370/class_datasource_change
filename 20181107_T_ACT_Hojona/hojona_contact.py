# -*- coding: UTF-8 -*-
import logger.logger
from hojona_util import hojona_contact_process
import pandas as pd
import re

"""
                                                                            FIRST_NAME          contact-firstName
                                                                            LAST_NAME           contact-lastName
ID                                  Contact External ID
Company                             Company External ID                     COMPANYID           contact-companyId
Contact                             Contact External ID                     CONTACTID           contact-externalId
Phone                               Primary Phhone                          PHONE               contact-phone
Phone Ext                           Primary Phone                           Work_EXTENSION      contact-phone
Mobile                              Mobile Phone                            MOBILE_PHONE        contact.mobile_phone
Birthday                            Date of Birth                           BIRTH_DATE          contact.date_of_birth
Address 1                           Current Location Address                ADDRESS_1
Address 1                           Current Location Name"                  ADDRESS_1
Address 2                           Current Location Address                ADDRESS_2
Address 2                           Current Location Name                   ADDRESS_2
City                                Current Location Address                CITY
City                                Current Location Name                   CITY
City                                Current Location Town / City            CITY
County                              Current Location Address                STATE
County                              Current Location Name                   STATE
County                              Current Location County                 STATE
Postcode                            Current Location Address                ZIP_CODE
Postcode                            Current Location Name                   ZIP_CODE
Postcode                            Current Location Postcode               ZIP_CODE
Country                             Current Location Address                COUNTRY
Country                             Current Location Name                   COUNTRY
Country                             Current Location Country                COUNTRY
E-mail                              Primary Email Address                   E-MAIL              contact-email
E-mail 2                            Personal Email Address                  LAST_E-MAIL         contact.personal_email
Web Site                            Website                                 WEB_SITE
Grade                               Grade                                   USER1               contact-Note
Service Group (pulldown menu)       Brief                                   USER2               contact-Note
Team Name                           Brief                                   USER3               contact-Note
Specialism 1                        Brief                                   USER4               contact-Note
Specialism 2                        Brief                                   USER5               contact-Note
DBS Number                          Brief                                   USER6               contact-Note
DBS Issue Date                      Brief                                   USER7               contact-Note
DBS Counter Signatory               Brief                                   USER8               contact-Note
How did you hear about Hojona       Brief                                   USER9               contact-Note
ID / Status                         Brief                                   CATEGORY            contact-Note
NI Number                           Brief                                   NINumber            contact-Note
HCPC Number                         Brief                                   HCPCNumber          contact-Note
HCPC Expiry                         Brief                                   HCPCExpiry          contact-Note
Pay Rate                            Brief                                   PayRate             contact-Note
Referred By                         Brief                                   REFERRED_BY         contact-Note
Title (pulldown menu)               Job Title                               TITLE               contact-jobTitle
Department (pulldown menu)          Department (Pulldown menu)              DEPARTMENT          contact.department
Home Address > Address 1            Current Location Address                HOME_ADDRESS_1
Home Address > Address 1            Current Location Name                   HOME_ADDRESS_1
Home Address > Address 2            Current Location Address                HOME_ADDRESS_2
Home Address > Address 2            Current Location Name                   HOME_ADDRESS_2
Home Address > City                 Current Location Address                HOME_CITY
Home Address > City                 Current Location Name                   HOME_CITY
Home Address > City                 Current Location Town / City            HOME_CITY
Home Address > County               Current Location Address                HOME_STATE
Home Address > County               Current Location Name                   HOME_STATE
Home Address > County               Current Location County                 HOME_STATE
Home Address > Post                 Current Location Address                HOME_ZIP_CODE
Home Address > Post                 Current Location Name                   HOME_ZIP_CODE
Home Address > Post                 Current Location Postal (ZIP) Code      HOME_ZIP_CODE
Home Address > Country              Current Location Address                HOME_COUNTRY
Home Address > Country              Current Location Name                   HOME_COUNTRY
Home Address > Country              Current Location Country                HOME_COUNTRY

NOTES                               Activities Comments
GROUPS / COMPANIES                  Distribution Lists
"""
mylog = logger.logger.get_logger('hojona.log')

df_contact_men = pd.read_csv('data_file/contact_mental_health.csv')
df_contact_soc = pd.read_csv('data_file/contact_socialcare.csv')

df_contact_men.rename(columns=lambda x: re.sub(r' ', '', x), inplace=True)
df_contact_soc.rename(columns=lambda x: re.sub(r' ', '', x), inplace=True)

df_contact_men_standard = hojona_contact_process.gen_contact_standard_file(df_contact_men, mylog)
df_contact_soc_standard = hojona_contact_process.gen_contact_standard_file(df_contact_soc, mylog)

df_contact_men_standard.to_csv('output_standard_files/men_cont.csv', index=False)
df_contact_soc_standard.to_csv('output_standard_files/soc_cont.csv', index=False)