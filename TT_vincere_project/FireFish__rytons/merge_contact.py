# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import re
# from edenscott._edenscott_dtypes import *
import os
import psycopg2
import common.vincere_common as vincere_common
import common.vincere_custom_migration as vincere_custom_migration
import common.vincere_standard_migration as vincere_standard_migration
import pandas as pd
import sqlalchemy
# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('rt_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)
sqlite_path = cf['default'].get('sqlite_path')
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% dest db
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
# %% dest db
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
ddbconn = engine_postgre_review.raw_connection()

# %% backup db
conn_str_ddb_bkup = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format('postgres', '123456', 'dmpfra.vinceredev.com', '5432', 'rytons_backup_restored')
engine_postgre_bkup = sqlalchemy.create_engine(conn_str_ddb_bkup, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
ddbconn_bkup = engine_postgre_bkup.raw_connection()

# %% backup db
contact_merge = pd.read_csv('merge_contact.csv')
# contact = pd.read_sql("""select id, note from "rytonsassociates.vincere.io".public.contact where id in (90649
# ,87598
# ,89771
# ,85194
# ,82258
# ,83833
# ,89370
# ,92002
# ,89231
# ,86876
# ,87989
# ,85412
# ,82707
# ,33135
# ,82384
# ,33095
# ,88082
# ,89322
# ,84240
# ,33091
# ,84685
# ,85341
# ,81936
# ,33131
# ,84543
# ,84557
# ,82438
# ,93100
# ,82944
# ,33124
# ,81935
# ,33119
# ,82804
# ,70803
# ,82637
# ,86916
# ,90332
# ,88782
# ,82111
# ,82102
# ,86819
# ,93094
# ,87594
# ,87778
# ,81950
# ,93102
# ,33127
# ,33123
# ,91380
# ,89593
# ,84170
# ,89730
# ,82935
# ,82930
# ,93091
# ,87323
# ,86616
# ,33136
# ,84883
# ,83234
# ,89008
# ,83020
# ,89565
# ,90444
# ,89680
# ,92580
# ,92154
# ,85864
# ,92604
# ,87369
# ,85349
# ,88027
# ,82107
# ,92985
# ,70806
# ,87227
# ,85555
# ,87024
# ,33105
# ,83101
# ,92984
# ,81940
# ,33120
# ,82934
# ,93086
# ,90621
# ,84583
# ,82096
# ,33160
# ,81924
# ,33125
# ,84194
# ,91305
# ,88964
# ,88180
# ,83774
# ,89475
# ,83506
# ,92052
# ,83959
# ,85651
# ,82806
# ,33139
# ,83078
# ,33084
# ,83424
# ,92053
# ,82639
# ,91708
# ,81926
# ,70795
# ,88327
# ,92299
# ,81963
# ,82933
# ,82228
# ,93096
# ,70788
# ,86829
# ,86789
# ,85818
# ,33085
# ,82195
# ,86002
# ,85631
# ,84298
# ,88362
# ,90709
# ,82303
# ,90903
# ,82399
# ,88079
# ,90400
# ,91249
# ,88948
# ,87255
# ,82396
# ,70816
# ,82068
# ,92987
# ,81988
# ,81990
# ,33110
# ,85621
# ,85794
# ,85677
# ,87137
# ,88921
# ,83575
# ,82677
# ,83668
# ,81951
# ,89751
# ,86865
# ,33128
# ,86859
# ,85832
# ,86682
# ,33100
# ,82240
# ,82033
# ,83095
# ,83093
# ,82193
# ,33113
# ,85490
# ,85892
# ,89399
# ,87848
# ,86699
# ,84158
# ,84558
# ,84682
# ,87750
# ,33090
# ,81927
# ,33134
# ,88902
# ,91175)""", engine_postgre_review)
# contact_merge = contact_merge.where(contact_merge.notnull(), None)
# contact_merge = contact_merge.loc[contact_merge['MASTER'] == 'NO'].reset_index()
# contact_merge['ori_id'] = contact_merge['ori_id'].apply(lambda x: int(x) if x else x)
# assert False
# contact_merge = contact_merge.merge(contact, left_on='contact_id', right_on='id')
# contact_merge = contact_merge.merge(contact, left_on='ori_id', right_on='id')
# contact_merge_2 = contact_merge[['id_y','id_x','note_x','note_y']].rename(columns={'id_y':'ori_id','id_x':'merge_id','note_y':'note_ori','note_x':'note_merge'})
# contact_merge_2['merge'] = '\n\n###################\nMerged from contact ID: ' + contact_merge_2['merge_id'].astype(str)
# contact_merge_2['note_merge'] = contact_merge_2[['merge', 'note_merge']].apply(lambda x: '\n'.join([e for e in x if e]), axis=1)
# contact_merge_note = contact_merge_2[['ori_id','note_merge']]
# contact_merge_note = contact_merge_note.groupby('ori_id')['note_merge'].apply('\n\n'.join).reset_index()
# contact_merge_2 = contact_merge_2.merge(contact_merge_note, on='ori_id')
# contact_merge_2 = contact_merge_2.drop_duplicates()
# contact_merge_2['note'] = contact_merge_2[['note_ori', 'note_merge_y']].apply(lambda x: '\n'.join([e for e in x if e]), axis=1)
# tem = contact_merge_2[['ori_id', 'note']]
# tem['id'] = tem['ori_id']
# vincere_custom_migration.psycopg2_bulk_update_tracking(tem, ddbconn, ['note'], ['id'], 'contact', mylog)

# %%
contact = pd.read_sql("""select id as contact_id, company_id from "rytonsassociates.vincere.io".public.contact""", engine_postgre_review)
merge_cont = contact_merge[['contact_id','ori_id']]
merge_cont = merge_cont.merge(contact,left_on='ori_id',right_on='contact_id')
merge_cont = merge_cont[['contact_id_x','ori_id','company_id']]
job = pd.read_sql("""select id, contact_id, company_id from position_description where contact_id in (90649
,89771
,82258
,92002
,89231
,85412
,82707
,82384
,89322
,84240
,84685
,81936
,84557
,82438
,82944
,81935
,82804
,82637
,90332
,82102
,86819
,87594
,81950
,93102
,33123
,91380
,89730
,82935
,87323
,86616
,83234
,89008
,90444
,92580
,92154
,92604
,85349
,82107
,92985
,87227
,87024
,83101
,81940
,82934
,90621
,82096
,81924
,91305
,88964
,83774
,92052
,85651
,82806
,83078
,92053
,91708
,81926
,92299
,82933
,82228
,70788
,86789
,85818
,86002
,84298
,90709
,90903
,88079
,91249
,88948
,82396
,82068
,81988
,33110
,85794
,87137
,88921
,83668
,81951
,89751
,86859
,86682
,82033
,83093
,82193
,85892
,87848
,86699
,84558
,87750
,81927
,91175
)""", engine_postgre_review)

job_2 = job.merge(merge_cont, left_on='contact_id',right_on='contact_id_x')
job_2 = job_2[['id','ori_id','company_id_y']].rename(columns={'ori_id':'contact_id','company_id_y':'company_id'})
vincere_custom_migration.psycopg2_bulk_update_tracking(job_2, ddbconn, ['contact_id', 'company_id'], ['id'], 'position_description', mylog)


activity_cont = pd.read_sql("""select * from "rytonsassociates.vincere.io".public.activity where contact_id in (90649
,89771
,82258
,92002
,89231
,85412
,82707
,82384
,89322
,84240
,84685
,81936
,84557
,82438
,82944
,81935
,82804
,82637
,90332
,82102
,86819
,87594
,81950
,93102
,33123
,91380
,89730
,82935
,87323
,86616
,83234
,89008
,90444
,92580
,92154
,92604
,85349
,82107
,92985
,87227
,87024
,83101
,81940
,82934
,90621
,82096
,81924
,91305
,88964
,83774
,92052
,85651
,82806
,83078
,92053
,91708
,81926
,92299
,82933
,82228
,70788
,86789
,85818
,86002
,84298
,90709
,90903
,88079
,91249
,88948
,82396
,82068
,81988
,33110
,85794
,87137
,88921
,83668
,81951
,89751
,86859
,86682
,82033
,83093
,82193
,85892
,87848
,86699
,84558
,87750
,81927
,91175)""", engine_postgre_review)
activity_cont_2 = activity_cont.merge(merge_cont, on='contact_id')
activity_cont_2['contact_id'] = activity_cont_2['ori_id']
activity_cont_2['contact_id'] = activity_cont_2['contact_id'].astype(int)
col_activity = list(activity_cont_2.columns)
col_activity.remove('id')
col_activity.remove('ori_id')
col_activity.remove('external_map')
col_activity.remove('owner_map')
vincere_custom_migration.psycopg2_bulk_insert_tracking(activity_cont_2, ddbconn,col_activity, 'activity', mylog)



activity_cont_map = pd.read_sql("""select * from "rytonsassociates.vincere.io".public.activity where id > 184364""", engine_postgre_review)
col = ['activity_id', 'contact_id']
tem = activity_cont_map[['id','contact_id']].rename(columns={'id':'activity_id'})
vincere_custom_migration.psycopg2_bulk_insert_tracking(tem, ddbconn,col, 'activity_contact', mylog)



docs = pd.read_sql("""select * from "rytonsassociates.vincere.io".public.candidate_document where contact_id in (90649
,89771
,82258
,92002
,89231
,85412
,82707
,82384
,89322
,84240
,84685
,81936
,84557
,82438
,82944
,81935
,82804
,82637
,90332
,82102
,86819
,87594
,81950
,93102
,33123
,91380
,89730
,82935
,87323
,86616
,83234
,89008
,90444
,92580
,92154
,92604
,85349
,82107
,92985
,87227
,87024
,83101
,81940
,82934
,90621
,82096
,81924
,91305
,88964
,83774
,92052
,85651
,82806
,83078
,92053
,91708
,81926
,92299
,82933
,82228
,70788
,86789
,85818
,86002
,84298
,90709
,90903
,88079
,91249
,88948
,82396
,82068
,81988
,33110
,85794
,87137
,88921
,83668
,81951
,89751
,86859
,86682
,82033
,83093
,82193
,85892
,87848
,86699
,84558
,87750
,81927
,91175)""", engine_postgre_review)
docs_2 = docs.merge(merge_cont, on='contact_id')
docs_2['contact_id'] = docs_2['ori_id']
docs_2['contact_id'] = docs_2['contact_id'].astype(int)
col = list(docs_2.columns)
col.remove('id')
col.remove('ori_id')
# col_activity.remove('external_map')
# col_activity.remove('owner_map')
vincere_custom_migration.psycopg2_bulk_insert_tracking(docs_2, ddbconn,col, 'candidate_document', mylog)



cont_rmv = pd.read_sql("""select id from "rytonsassociates.vincere.io".public.contact where id in (87468
,87416
,83228
,91653
,86476
,82707
,82384
,87878
,83504
,91558
,86710
,92015
,92045
,91582
,91791
,82102
,92397
,86819
,93073
,92997
,93008
,93032
,93041
,93056
,93034
,93003
,93023
,93005
,93012
,93052
,92999
,93053
,93006
,93000
,93075
,92996
,93050
,93030
,93083
,93064
,93069
,93055
,93027
,93001
,93079
,93065
,93043
,93071
,93044
,93054
,93011
,93004
,93009
,93085
,93013
,81922
,93015
,93028
,93014
,93002
,93021
,93063
,93036
,93058
,93081
,93047
,92993
,93080
,93020
,93040
,93084
,93031
,93035
,92995
,93051
,93045
,93057
,93046
,93076
,93039
,93067
,92989
,93033
,93068
,93070
,92992
,93049
,93022
,92998
,93010
,93018
,93026
,93037
,93074
,93024
,93019
,93082
,93072
,92990
,93016
,93066
,93061
,93059
,93048
,92991
,93038
,93029
,93060
,93017
,93025
,92994
,93078
,93062
,93077
,93007
,93042
,92811
,91539
,91380
,92184
,92133
,92155
,92044
,92030
,92406
,92554
,92569
,92386
,92678
,92130
,87149
,92374
,92661
,88848
,89464
,92388
,92682
,92203
,87941
,91032
,92806
,91466
,92518
,92154
,91704
,92604
,91557
,87168
,90407
,85349
,90683
,86492
,92659
,92479
,91307
,84709
,91552
,92785
,82506
,91968
,82041
,86575
,88152
,91454
,86882
,92090
,91738
,91593
,92020
,91997
,91477
,92691
,88909
,90363
,92429
,91772
,88034
,91519
,92565
,91668
,83236
,82440
,90506
,92067
,88087
,91644
,91617
,92819
,85651
,86382
,88785
,92959
,86479
,91600
,83368
,86603
,90508
,88918
,91634
,91531
,81957
,86156
,92470
,91318
,92418
,92368
,92716
,88024
,89455
,91640
,91417
,92699
,92473
,92731
,92657
,92844
,92114
,86072
,88106
,92144
,92557
,91998
,92410
,91674
,92280
,91523
,92528
,92101
,91713
,92464
,92024
,91534
,89186
,92810
,92078
,91781
,91345
,92105
,91399
,91996
,92824
,91595
,91633
,82972
,91504
,91624
,91750
,86688
,91297
,91739
,91285
,91702) and deleted_timestamp is null""", engine_postgre_review)
cont_rmv['deleted_timestamp'] = '2020-05-05 02:49:35.649000'
cont_rmv['deleted_timestamp'] = pd.to_datetime(cont_rmv['deleted_timestamp'])
vincere_custom_migration.psycopg2_bulk_update_tracking(cont_rmv, ddbconn, ['deleted_timestamp'], ['id'], 'contact', mylog)