"""
OwnCV               candidate
DespatchCV          candidate
VacJob              job
AWR                 candidate
Terms               contact
AWRcl               job
CandScannedImage    candidate
Opt In              candidate
PermRegForm         candidate
CompProfile         company
Refs                candidate
Benefits            candidate
CandNotes           candidate
Opt Out             candidate
IntRepts            candidate

"""

# -*- coding: UTF-8 -*-
from common import vincere_custom_migration
import common.logger_config as log
import psycopg2
import pymssql
import pandas as pd
import configparser
import os
import common.s3 as s3
import pathlib

#
# config
logger = log.get_info_logger("edenscott.log")
cf = configparser.RawConfigParser()
cf.read('_edenscott_config.ini', encoding='utf8')
fr = cf[cf['default'].get('src_db')]
to = cf[cf['default'].get('dest_db')]
upload_folder = cf['default'].get('upload_folder')
data_folder = cf['default'].get('data_folder')

if __name__ == '__main__':
    #
    # files meta data load
    files_metadata = s3.prepare_metadata_files(upload_folder)
    a= files_metadata.groupby('file').size()
    a[a>1]
    files_metadata[files_metadata['file']=='99342.661.pdf']

    sdbconn = pymssql.connect(server=fr.get('server'), user=fr.get('user'), password=fr.get('password'), database=fr.get('database'), as_dict=True)
    ddbconn = psycopg2.connect(host=to.get('server'), user=to.get('user'), password=to.get('password'), database=to.get('database'), port=to.get('port'))
    ddbconn.set_client_encoding('UTF8')
    #
    # company files load
    logger.info('companies'' files are loading...')
    df_comp_files = pd.read_sql("""
    select
    AttToRef as external_id,
    concat(ltrim(rtrim(AttToRef)),'.',ltrim(rtrim(BlobRec)), '.', ltrim(rtrim(Extension))) as file_name
    from Attachments
    where BlobFile in ('CompProfile');
    """, sdbconn)
    #
    # files meta data load for company
    files_metadata_comp = s3.prepare_metadata_files(os.path.join(upload_folder, 'CompProfile'))

    #
    # contact files load
    logger.info('companies'' files are loading...')
    df_cont_files = pd.read_sql("""
    select
    AttToRef as external_id,
    concat(ltrim(rtrim(AttToRef)),'.',ltrim(rtrim(BlobRec)), '.', ltrim(rtrim(Extension))) as file_name
    from Attachments
    where BlobFile in ('Terms');
    """, sdbconn)
    #
    # files meta data load for contact
    files_metadata_cont = s3.prepare_metadata_files(os.path.join(upload_folder, 'Terms'))

    #
    # candidate resume files load
    logger.info('candidates'' files are loading...')
    df_cand_files1 = pd.read_sql("""
    select 
    AttToRef as candidate_external_id,
    concat(ltrim(rtrim(AttToRef)),'.',ltrim(rtrim(BlobRec)), '.', ltrim(rtrim(Extension))) as file_name
    from Attachments
    where BlobFile in ('OwnCV','DespatchCV')
    """, sdbconn)
    #
    # files meta data load for candidate1
    files_metadata_cand_1 = pd.concat([s3.prepare_metadata_files(os.path.join(upload_folder, 'OwnCV'))
                                     , s3.prepare_metadata_files(os.path.join(upload_folder, 'DespatchCV')),])

    #
    # candidate other docs files load
    logger.info('candidates'' files are loading...')
    df_cand_files2 = pd.read_sql("""
    select 
    AttToRef as candidate_external_id,
    concat(ltrim(rtrim(AttToRef)),'.',ltrim(rtrim(BlobRec)), '.', ltrim(rtrim(Extension))) as file_name
    from Attachments
    where BlobFile in ('AWR','CandScannedImage','Opt In','PermRegForm','Refs','Benefits','CandNotes','Opt Out','IntRepts')
    """, sdbconn)
    #
    # files meta data load for candidate2
    files_metadata_cand_2 = pd.concat([s3.prepare_metadata_files(os.path.join(upload_folder, 'AWR'))
                                          , s3.prepare_metadata_files(os.path.join(upload_folder, 'CandScannedImage'))
                                          , s3.prepare_metadata_files(os.path.join(upload_folder, 'Opt In'))
                                          , s3.prepare_metadata_files(os.path.join(upload_folder, 'PermRegForm'))
                                          , s3.prepare_metadata_files(os.path.join(upload_folder, 'Refs'))
                                          , s3.prepare_metadata_files(os.path.join(upload_folder, 'Benefits'))
                                          , s3.prepare_metadata_files(os.path.join(upload_folder, 'CandNotes'))
                                          , s3.prepare_metadata_files(os.path.join(upload_folder, 'Opt Out'))
                                          , s3.prepare_metadata_files(os.path.join(upload_folder, 'IntRepts'))
                                          , ])

    #
    # job description files load
    logger.info('jobs'' files are loading...')
    df_job_files = pd.read_sql("""
    select 
    AttToRef as external_id,
    concat(ltrim(rtrim(AttToRef)),'.',ltrim(rtrim(BlobRec)), '.', ltrim(rtrim(Extension))) as file_name
    from Attachments
    where BlobFile in ('VacJob','AWRcl')
    """, sdbconn)
    #
    # files meta data load for job
    files_metadata_job = pd.concat([s3.prepare_metadata_files(os.path.join(upload_folder, 'VacJob'))
        , s3.prepare_metadata_files(os.path.join(upload_folder, 'AWRcl'))])

if False:
    t1 = df_comp_files.merge(files_metadata_comp, left_on='file_name', right_on='file', how='inner', indicator=True)
    # t1[t1['_merge']!='both']
    t1['file_name'] = t1['alter_file1']
    vincere_custom_migration.insert_bulk_upload_document_mapping_4_company(t1[t1['file']=='6.4.doc'], ddbconn)

    t5 = df_cand_files2.merge(files_metadata_cand_2, left_on='file_name', right_on='file', how='inner', indicator=True)
    # t5[t5['_merge'] != 'both'].shape
    t5['file_name'] = t5['alter_file1']
    logger.info('inserting into bulk_upload_document_mapping for candidates')
    t5.rename(columns={'candidate_external_id': 'external_id'}, inplace=True)
    vincere_custom_migration.insert_bulk_upload_document_mapping_4_candidate(t5[(t5['file'] == '2111.43.doc') | (t5['file'] == '122751.924.pdf')], ddbconn)

if False:
    #
    # for company
    t1 = df_comp_files.merge(files_metadata_comp, left_on='file_name', right_on='file', how='inner', indicator=True)
    # t1[t1['_merge']!='both']
    t1['file_name'] = t1['alter_file1']
    logger.info('inserting into bulk_upload_document_mapping for companies')
    vincere_custom_migration.insert_bulk_upload_document_mapping_4_company(t1, ddbconn)

    #
    # for candidate 1
    t4 = df_cand_files1.merge(files_metadata_cand_1, left_on='file_name', right_on='file', how='inner', indicator=True)
    # t4[t4['_merge']!='both'].shape
    t4['file_name'] = t4['alter_file1']
    logger.info('inserting into bulk_upload_document_mapping for candidates')
    t4.rename(columns={'candidate_external_id':'external_id'}, inplace=True)
    vincere_custom_migration.insert_bulk_upload_document_mapping_4_candidate(t4, ddbconn)

    #
    # for candidate 2
    t5 = df_cand_files2.merge(files_metadata_cand_2, left_on='file_name', right_on='file', how='inner', indicator=True)
    # t5[t5['_merge'] != 'both'].shape
    t5['file_name'] = t5['alter_file1']
    logger.info('inserting into bulk_upload_document_mapping for candidates')
    t5.rename(columns={'candidate_external_id': 'external_id'}, inplace=True)
    vincere_custom_migration.insert_bulk_upload_document_mapping_4_candidate(t5, ddbconn)

    #
    # for contacts
    t2 = df_cont_files.merge(files_metadata_cont, left_on='file_name', right_on='file', how='inner', indicator=True)
    # t2[t2['_merge']!='both']
    t2['file_name'] = t2['alter_file1']
    logger.info('inserting into bulk_upload_document_mapping for contacts')
    vincere_custom_migration.insert_bulk_upload_document_mapping_4_contact(t2, ddbconn)

    #
    # for jobs
    t3 = df_job_files.merge(files_metadata_job, left_on='file_name', right_on='file', how='inner', indicator=True)
    # t3[t3['_merge']!='both'].shape
    t3['file_name'] = t3['alter_file1']
    logger.info('inserting into bulk_upload_document_mapping for jobs')
    vincere_custom_migration.insert_bulk_upload_document_mapping_4_position_description(t3, ddbconn)

    destfolder = os.path.join(data_folder, 'bulk_upload_metadata')
    pathlib.Path(destfolder).mkdir(parents=True, exist_ok=True)  # create folder if not exist
    t1.to_csv(os.path.join(destfolder, 't1.csv'), index=False)
    t2.to_csv(os.path.join(destfolder, 't2.csv'), index=False)
    t3.to_csv(os.path.join(destfolder, 't3.csv'), index=False)
    t4.to_csv(os.path.join(destfolder, 't4.csv'), index=False)
    t5.to_csv(os.path.join(destfolder, 't5.csv'), index=False)


