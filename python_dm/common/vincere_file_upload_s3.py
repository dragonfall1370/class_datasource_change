# -*- coding: UTF-8 -*-

import psycopg2
from common import vincere_custom_migration as vc
import common.connection_string as cs
from common import logger_config as log
from common import s3
import pandas as pd
import uuid
import re
# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# mylog = log.get_logger("private_site.log")
fr = cs.production_rlc_p35432
ddbconn = psycopg2.connect(host=fr.get('server'), user=fr.get('user'), password=fr.get('password'), database=fr.get('database'), port=fr.get('port'))
ddbconn.set_client_encoding('UTF8')
#
# get redady to parse files
ready_to_parse_files = pd.read_sql("""
select * from bulk_upload_detail where id in (select id from bulk_upload_detail_view where status=2)  -- ready to parse
""", ddbconn)
#
# get appended files
appended_files = pd.read_sql("""
select 'appended' as file_processing_status, * from bulk_upload_detail where id in (select id from bulk_upload_detail_view where status=1)  -- appended
""", ddbconn)
#
# get waiting_for_mapping_files
waiting_for_mapping_files = pd.read_sql("""
select * from bulk_upload_detail where id in (select id from bulk_upload_detail_view where s3_file_path is not null and status=3)  -- waiting for mapping
""", ddbconn)
#
# get bulk mapping
mapping = pd.read_sql("""select * from bulk_upload_document_mapping""", ddbconn)
#
# get mapping detail
mapping_detail = pd.read_sql("""select * from bulk_upload_detail""", ddbconn)
#
# get candidate documents
map_cand_doc = pd.read_sql("""select * from candidate_document""", ddbconn)

# all_mapping = mapping.merge(map_cand_doc, left_on='entity_id', right_on='candidate_id', how='left')
# fail_mapping = all_mapping[all_mapping['candidate_id'].isnull() & (all_mapping['entity_type']=='CANDIDATE')]
# fail_mapping['file_ext'] = fail_mapping.apply(lambda x: ('%s%s') % (str(uuid.uuid4()), re.search(r'\.\w+$', x['file_name']).group()), axis=1)


if False:
    #
    # these files actually are appended but some how, they have been duplicated and these duplicated files have not been mapped
    t2 = waiting_for_mapping_files.merge(appended_files[['file_name',]], on='file_name')
    vc.psycopg2_bulk_delete(t2, ddbconn, 'id', 'bulk_upload_detail', chunk_size=1000)
    t2 = ready_to_parse_files.merge(appended_files[['file_name',]], on='file_name')
    vc.psycopg2_bulk_delete(t2, ddbconn, 'id', 'bulk_upload_detail', chunk_size=1000)

    # files have info in bulk mapping but not map to entity (these file should be re-uploaded)
    t1 = waiting_for_mapping_files.merge(mapping[['file_name', 'entity_id',]], on='file_name')

    vc.psycopg2_bulk_delete(waiting_for_mapping_files, ddbconn, 'id', 'bulk_upload_detail', chunk_size=1000)
    vc.psycopg2_bulk_delete(ready_to_parse_files, ddbconn, 'id', 'bulk_upload_detail', chunk_size=1000)


    re_up = mapping.merge(appended_files[['file_name', 'file_processing_status',]], on='file_name', how='left')
    re_up = re_up[re_up['file_processing_status'].isnull()]
    re_up['new_file_name'] = re_up.apply(lambda x: re.sub(r"%|\$|&| |'|\[|\]|-|\+", '_', x['file_name']), axis=1)
    #re_up['file_name'] = re_up['new_file_name']
    #vc.psycopg2_bulk_update(re_up, ddbconn, ['file_name',], ['id',], 'bulk_upload_document_mapping')

    s3.upload_files_to_s3_filter_rename(upload_folder=r"D:\vincere\data_output\rlc",
                                        df=re_up
                                      , to_folder=r"d:\vincere\data_output\rlc_reup"
                                      )
