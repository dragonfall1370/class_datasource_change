# -*- coding: UTF-8 -*-
import pandas as pd
import logger.logger
from hojona_util import hojona_company_process

"""
ID                       Company External ID              COMPANYID                  company-externalId
Company                  Company Name                     COMPANY                    company-name
Phone                    Phone                            PHONE                      company-phone
Toll-Free                Phone                            TOLL_FREE_EXTENSION        company-phone
Web Site                 Website                          WEB_SITE                   company-website
Address 1                Location Address                 ADDRESS_1                  company-locationAddress
Address 1                Location Name                    ADDRESS_1                  company-locationAddress
Address 2                Location Address                 ADDRESS_2                  company-locationAddress
Address 2                Location Name                    ADDRESS_2                  company-locationAddress
City                     Location Address                 CITY                       company-locationCity
City                     Location Name                    CITY                       company-locationCity
City                     Location City"                   CITY                       company-locationCity
County                   Location Address                 STATE                      company-locationState
County                   Location Name                    STATE                      company-locationState
County                   Location State                   STATE                      company-locationState
Post                     Location Address                 ZIP_CODE                   company-locationZipCode
Post                     Location Name                    ZIP_CODE                   company-locationZipCode
Post                     Location Postal (ZIP) Code       ZIP_CODE                   company-locationZipCode
Country                  Location Address                 COUNTRY                    company-locationCountry
Country                  Location Name                    COUNTRY                    company-locationCountry
Country                  Location Country                 COUNTRY                    company-locationCountry
Industry                 INDUSTRY                         INDUSTRY                   company_industry
ID                       Brief                            COMPANYID                  company-note
Description              Brief                            COMPANY_DESCRIPTION        company-note
ID/Status                Brief                            ID/STATUS                  company-note
Referred By              Brief                            REFERRED_BY                company-note
Division                 Brief                            DIVISION                   company-note

Ticker                   Brief                                                       company-note
CONTACTS                 CONTACTS                           
OPPORTUNITIES            JOBS                               
ACTIVITIES               Activities Comments                
HISTORY                  Activities Comments                
NOTES                    Activities Comments                
DOCUMENTS                FILES                              

                         BILLING                            
Billing > Address 1      Location Address                   BILLING_ADDRESS_1
Billing > Address 1      Location Name                      BILLING_ADDRESS_1
                         BILLING                            
Billing > Address 2      Location Addres                    BILLING_ADDRESS_2
Billing > Address 2      Location Name                      BILLING_ADDRESS_2
                         BILLING                            
Billing > Address 3      Location Addres                    BILLING_ADDRESS_3
Billing > Address 3      Location Name                      BILLING_ADDRESS_3
                         BILLING                            
Billing > City           Location Address                   BILLING_CITY
Billing > City           Location Name                      BILLING_CITY
Billing > City           Location City                      BILLING_CITY
                         BILLING                            
Billing > County         Location Address                   BILLING_STATE
Billing > County         Location Name                      BILLING_STATE
Billing > County         Location State                     BILLING_STATE
                         BILLING                            
Billing > Post           Location Address                   BILLING_ZIP_CODE
Billing > Post           Location Name                      BILLING_ZIP_CODE
Billing > Post           Location Postal (ZIP) Code         BILLING_ZIP_CODE
                         BILLING                            
Billing Country          Location Address                   BILLING_COUNTRY
Billing Country          Location Name                      BILLING_COUNTRY
Billing Country          Location Country                   BILLING_COUNTRY
                         SHIPPING                           
Shipping > Address 1     Location Address                   SHIPPING_ADDRESS_1        
Shipping > Address 1     Location Name                      SHIPPING_ADDRESS_1    
                         SHIPPING                           
Shipping > Address 2     Location Address                   SHIPPING_ADDRESS_2        
Shipping > Address 2     Location Name                      SHIPPING_ADDRESS_2    
                         SHIPPING                           
Shipping > Address 3     Location Address                   SHIPPING_ADDRESS_3        
Shipping > Address 3     Location Name                      SHIPPING_ADDRESS_3    
                         SHIPPING                           
Shipping > City          Location Address                   SHIPPING_CITY        
Shipping > City          Location Name                      SHIPPING_CITY    
Shipping > City          Location City                      SHIPPING_CITY    
                         SHIPPING                           
Shipping > County        Location Address                   SHIPPING_STATE
Shipping > County        Location Name                      SHIPPING_STATE
Shipping > County        Location State                     SHIPPING_STATE
                         SHIPPING                           
Shipping > Post          Location Address                   SHIPPING_ZIP_CODE
Shipping > Post          Location Name                      SHIPPING_ZIP_CODE
Shipping > Post          Location Postal (ZIP) Code         SHIPPING_ZIP_CODE
                         SHIPPING                           
Shipping > Country       Location Address                   SHIPPING_COUNTRY
Shipping > Country       Location Name                      SHIPPING_COUNTRY
Shipping > Country       Location Country                   SHIPPING_COUNTRY

"""
mylog = logger.logger.get_logger("hojona.log")

df_com_men = pd.read_csv('data_file/company_mental_health.csv')
df_com_soc = pd.read_csv('data_file/company_socialcare.csv')

df_com_men_standard = hojona_company_process.gen_company_standard_file(df_com_men, mylog)
df_com_soc_standard = hojona_company_process.gen_company_standard_file(df_com_soc, mylog)

df_com_men_standard.to_csv('output_standard_files/men_com.csv', index=False)
df_com_soc_standard.to_csv('output_standard_files/soc_com.csv', index=False)