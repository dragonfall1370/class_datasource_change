# -*- coding: UTF-8 -*-
from vincere import vincere_custom_migration
import pymssql
import logger.logger as log
import common.connection_string as cs

logger = log.get_logger("rlc.log")
fr = cs.client_rlc_prd
sdbconn = pymssql.connect(server=fr.get('server'), user=fr.get('user'), password=fr.get('password'), database=fr.get('database'), as_dict=True)
#
# extract files for companies
logger.info('extracting files for companies')
dfolder = r'd:\vincere\data_output\rlc\comp\\'
sql = """
SELECT 
    concat(doc_id,'_',replace(replace(doc_name,',',''),'.',''),rtrim(ltrim(doc_ext))) as file_name, 
    convert(varbinary(max), doc, 1) as file_data  
FROM tblCompanyDocs 
ORDER BY doc_id OFFSET %d ROWS FETCH NEXT %d ROWS ONLY; """
vincere_custom_migration.get_files_from_dbms_save_to_physical_files(sql, sdbconn, dfolder, 1000)
#
# extract files for jobs
logger.info('extracting files for jobs')
dfolder = r'd:\vincere\data_output\rlc\job\\'
sql = "SELECT concat(doc_id,'_',replace(replace(replace(doc_name,',',''),'.',''),'~$ ',''),rtrim(ltrim(doc_ext))) as file_name, convert(varbinary(max), doc, 1) as file_data " \
      "FROM tblVacanciesDocs ORDER BY doc_id OFFSET %d ROWS FETCH NEXT %d ROWS ONLY;"
vincere_custom_migration.get_files_from_dbms_save_to_physical_files(sql, sdbconn, dfolder, 1000)
#
# extract files for candidates
logger.info('extracting files for candidates')
dfolder = r'd:\vincere\data_output\rlc\cand\\'
sql = """
SELECT 
    concat(doc_id,'_',replace(replace(doc_name,',',''),'.',''),rtrim(ltrim(doc_ext))) as file_name, 
    convert(varbinary(max), doc, 1) as file_data  
FROM tblCandidateDocs where doc_ext is not NULL and doc_ext <> '' 
ORDER BY doc_id OFFSET %d ROWS FETCH NEXT %d ROWS ONLY; """
vincere_custom_migration.get_files_from_dbms_save_to_physical_files(sql, sdbconn, dfolder, 1000)
#
# extract files for contacts
logger.info('extracting files for contacts')
dfolder = r'd:\vincere\data_output\rlc\cont\\'
sql ="""
SELECT 
    concat(doc_id, '_', replace(replace(replace(replace(doc_name, ',',''),'.',''),'#',''),'(',''), '_', doc_ext) AS file_name, 
    doc as file_data 
FROM tblContactsDocs 
ORDER BY doc_id OFFSET %d ROWS FETCH NEXT %d ROWS ONLY;"""
vincere_custom_migration.get_files_from_dbms_save_to_physical_files(sql, sdbconn, dfolder, 1000)
#
# extract files for candidate (email)
logger.info('extracting email files for companies')
dfolder = r'd:\vincere\data_output\rlc\candemail\\'
sql = """
SELECT 
       concat(EmailID,rtrim(EmailFileExt)) as file_name, 
       convert(varbinary(max), EmailFile, 1)as file_data FROM tblEmails WHERE EmailID in ( 
       SELECT DISTINCT tblEmailCandidates.EmailID 
       FROM tblEmailCandidates 
         LEFT JOIN tblEmails ON tblEmails.EmailID = tblEmailCandidates.EmailID 
       WHERE tblEmails.EmailFile IS NOT NULL AND tblEmails.EmailFileExt IS NOT NULL 
       )
        ORDER BY EmailID OFFSET %d ROWS FETCH NEXT %d ROWS ONLY;"""
vincere_custom_migration.get_files_from_dbms_save_to_physical_files(sql, sdbconn, dfolder, 1000)
#
# extract files for contact (email)
logger.info('extracting email files for contacts')
dfolder = r'd:\vincere\data_output\rlc\contemail\\'
sql = """
SELECT 
    concat(EmailID,rtrim(EmailFileExt))  as file_name, 
    convert(varbinary(max), EmailFile, 1) as file_data 
FROM tblEmails  
WHERE EmailID in 
   (select distinct tblEmailContacts.EmailID from tblEmailContacts 
                           left join tblEmails on tblEmails.EmailID = tblEmailContacts.EmailID 
                           where tblEmails.EmailFile is not NULL) 
ORDER BY EmailID OFFSET %d ROWS FETCH NEXT %d ROWS ONLY;"""
vincere_custom_migration.get_files_from_dbms_save_to_physical_files(sql, sdbconn, dfolder, 1000)

logger.info('-- done --')

