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
import numpy as np
import sqlalchemy
# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('tj_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
src_db = cf[cf['default'].get('src_db')]
sqlite_path = cf['default'].get('sqlite_path')
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% data connections
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')
user = pd.read_csv('user.csv')
user['matcher'] = user['Username'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
# conn_str_sdb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(src_db.get('user'), src_db.get('password'), src_db.get('server'), src_db.get('port'), src_db.get('database'))
# engine_postgre_src = sqlalchemy.create_engine(conn_str_sdb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)

# %%
company = pd.read_sql("""
select ClientID, CompanyName, Manager from  Clients
where ClientID not in 
(362
,481
,742
,779
,829
,938
,970
,988
,1020
,2198
,2402
,2769
,3341
,3708
,4279
,16824
,17182
,20065
,20105
,20156
,20224
,20549
,20767
,20896
,20923
,21136
,21297
,21445
,21490
,21584
,21592
,21673
,21707
,21811
,21812
,21865
,21874
,21894
,21937
,22055
,22062
,22069
,22110
,22217
,22423
,22447
,22500
,22543
,22633
,22646
,22687
,22846
,22911
,22971
,23007
,23039
,23244
,23274
,23275
,23331
,23398
,23499
,23538
,23547
,23569
,23663
,23749
,23750
,23762
,23804
,23858
,23889
,23926
,23946
,23991
,24023
,24124
,24149
,24203
,24234
,24250
,24274
,24306
,24316
,24337
,24351
,24387
,24404
,24425
,24456
,24477
,24506
,24557
,24558
,24559
,24561
,24577
,24593
,24602
,24657
,24787
,24788
,24790
,24805
,24871
,24873
,24928
,24931
,25025
,25032
,25081
,25125
,25182
,25396
,25413
,25442
,25490
,25534
,25580
,25615
,25629
,25666
,25680
,25687
,25691
,25705
,25838
,25854
,25911
,25918
,25979
,25982
,25992
,26014
,26032
,26047
,26048
,26052
,26063
,26065
,26068
,26082
,26087
,26096
,26107
,26112
,26126
,26142
,26143
,26186
,26196
,26207
,26214
,26252
,26254
,26255
,26270
,26298
,26301
,26303
,26329
,26330
,26332
,26333
,26365
,26378
,26379
,26380
,26382
,26387
,26394
,26404
,26407
,26428
,26430
,26436
,26453
,26470
,26471
,26472
,26473
,26475
,26481
,26482
,26484
,26509
,26525
,26550
,26558
,26564
,26566
,26582
,26586
,26597
,26598
,26640
,26648
,26672
,26686
,26687
,26689
,26703
,26724
,26737
,26740
,26751
,26792
,26800
,26855
,26888
,26908
,26915
,26934
,26937
,26999
,27002
,27009
,27028
,27083
,27088
,27089
,27108
,27111
,27117
,27155
,27164
,27165
,27212
,27214
,27215
,27241
,27244
,27255
,27256
,27257
,27272
,27311
,27318
,27319
,27338
,27340
,27347
,27362
,27368
,27373
,27417
,27419)
""", engine_mssql)
company['Manager'] = company['Manager'].fillna('')
company['matcher'] = company['Manager'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
company = company.merge(user, on='matcher',how='left')
company['ClientID'] = 'FC'+company['ClientID'].astype(str)

contact = pd.read_sql("""
select ContactID, ClientID, FirstName, LastName, Email, Manager,MiddleName from ClientContacts
where ClientID not in
(362
,481
,742
,779
,829
,938
,970
,988
,1020
,2198
,2402
,2769
,3341
,3708
,4279
,16824
,17182
,20065
,20105
,20156
,20224
,20549
,20767
,20896
,20923
,21136
,21297
,21445
,21490
,21584
,21592
,21673
,21707
,21811
,21812
,21865
,21874
,21894
,21937
,22055
,22062
,22069
,22110
,22217
,22423
,22447
,22500
,22543
,22633
,22646
,22687
,22846
,22911
,22971
,23007
,23039
,23244
,23274
,23275
,23331
,23398
,23499
,23538
,23547
,23569
,23663
,23749
,23750
,23762
,23804
,23858
,23889
,23926
,23946
,23991
,24023
,24124
,24149
,24203
,24234
,24250
,24274
,24306
,24316
,24337
,24351
,24387
,24404
,24425
,24456
,24477
,24506
,24557
,24558
,24559
,24561
,24577
,24593
,24602
,24657
,24787
,24788
,24790
,24805
,24871
,24873
,24928
,24931
,25025
,25032
,25081
,25125
,25182
,25396
,25413
,25442
,25490
,25534
,25580
,25615
,25629
,25666
,25680
,25687
,25691
,25705
,25838
,25854
,25911
,25918
,25979
,25982
,25992
,26014
,26032
,26047
,26048
,26052
,26063
,26065
,26068
,26082
,26087
,26096
,26107
,26112
,26126
,26142
,26143
,26186
,26196
,26207
,26214
,26252
,26254
,26255
,26270
,26298
,26301
,26303
,26329
,26330
,26332
,26333
,26365
,26378
,26379
,26380
,26382
,26387
,26394
,26404
,26407
,26428
,26430
,26436
,26453
,26470
,26471
,26472
,26473
,26475
,26481
,26482
,26484
,26509
,26525
,26550
,26558
,26564
,26566
,26582
,26586
,26597
,26598
,26640
,26648
,26672
,26686
,26687
,26689
,26703
,26724
,26737
,26740
,26751
,26792
,26800
,26855
,26888
,26908
,26915
,26934
,26937
,26999
,27002
,27009
,27028
,27083
,27088
,27089
,27108
,27111
,27117
,27155
,27164
,27165
,27212
,27214
,27215
,27241
,27244
,27255
,27256
,27257
,27272
,27311
,27318
,27319
,27338
,27340
,27347
,27362
,27368
,27373
,27417
,27419)
or ContactID not in (157655
,164137
,166115
,190823
,201031
,300308
,303864
,304300
,304440
,305924
,306548
,307129
,307212
,307245
,307507
,307667
,308616
,309019
,309776
,309824
,309922
,309948
,309949
,309950
,310245
,310979
,311659
,312521
,313151
,314694
,314872
,315113
,315619
,316043
,316304
,316401
,316502
,317150
,317830
,317938
,318084
,318139
,318205
,318584
,318720
,318925
,319074
,319551
,319553
,319773
,319943
,319960
,319966
,320255
,320291
,320319
,320354
,320519
,320591
,320626
,320690
,320765
,321020
,321063
,321108
,321235
,321332
,321487
,322106
,322145
,322163
,322342
,322547
,322641
,322746
,322900
,323341
,323342
,323633
,323793
,323979
,324771
,324890
,325242
,325371
,325944
,326001
,326098
,326181
,326252
,326389
,326707
,326717
,327206
,327390
,327424
,327505
,327514
,327565
,327707
,327850
,327909
,328254
,328870
,329035
,329078
,329230
,329751
,330170
,330206
,330245
,330881
,331201
,331615
,331676
,331981
,332073
,332120
,332187
,332188
,332189
,332320
,332363
,332365
,332403
,332408
,332421
,332550
,332732
,332823
,333001
,333016
,333037
,333107
,333154
,333196
,333208
,333368
,333460
,333604
,333684
,333729
,333750
,333758
,333879
,333880
,333881
,333884
,333885
,333887
,333992
,334079
,334093
,334137
,334163
,334165
,334167
,334169
,334174
,334177
,334178
,334221
,334242
,334246
,334265
,334418
,334447
,334507
,334647
,334818
,334820
,334825
,334830
,334831
,334832
,334834
,334836
,334837
,334838
,334840
,334841
,334842
,334907
,334908
,334909
,334927
,334948
,335059
,335175
,335470
,335473
,335682
,335912
,335955
,336006
,336042
,336063
,336113
,336130
,336131
,336132
,336133
,336134
,336135
,336162
,336163
,336164
,336165
,336167
,336168
,336169
,336170
,336171
,336172
,336173
,336175
,336176
,336177
,336179
,336182
,336185
,336186
,336187
,336194
,336195
,336200
,336201
,336206
,336398
,336401
,336444
,336629
,336686
,336847
,336852
,337093
,337104
,337436
,337567
,337584
,337655
,337807
,337935
,338132
,338243
,338253
,338254
,338454
,338476
,338573
,338574
,338630
,338797
,338801
,338910
,338911
,338914
,338915
,338916
,338917
,338977
,338980
,339025
,339070
,339111
,339138
,339273
,339276
,339280
,339281
,339303
,339374
,339375
,339428
,339454
,339582
,339756
,339797)
""", engine_mssql)
discard_list = [351705,334327
,346927
,355562
,349116
,353511
,326632
,351124
,350519
,357042
,335594
,346741
,360946
,355688
,355045
,329761
,353243
,350564
,352468
,348848
,362085
,358649
,361890
,332652
,357121
,358405
,361937
,331082
,338687
,352631
,356703]
contact = contact.loc[~contact['ContactID'].isin(discard_list)]
contact['Manager'] = contact['Manager'].fillna('')
contact['matcher'] = contact['Manager'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
contact = contact.merge(user, on='matcher',how='left')
contact['ClientID'] = 'FC'+contact['ClientID'].astype(str)
contact['ContactID'] = 'FC'+contact['ContactID'].astype(str)

job = pd.read_sql("""
select ContactID, EnquiryID, RealTitle, Manager, Consultant from Enquiries
where ContactID not in (157655
,164137
,166115
,190823
,201031
,300308
,303864
,304300
,304440
,305924
,306548
,307129
,307212
,307245
,307507
,307667
,308616
,309019
,309776
,309824
,309922
,309948
,309949
,309950
,310245
,310979
,311659
,312521
,313151
,314694
,314872
,315113
,315619
,316043
,316304
,316401
,316502
,317150
,317830
,317938
,318084
,318139
,318205
,318584
,318720
,318925
,319074
,319551
,319553
,319773
,319943
,319960
,319966
,320255
,320291
,320319
,320354
,320519
,320591
,320626
,320690
,320765
,321020
,321063
,321108
,321235
,321332
,321487
,322106
,322145
,322163
,322342
,322547
,322641
,322746
,322900
,323341
,323342
,323633
,323793
,323979
,324771
,324890
,325242
,325371
,325944
,326001
,326098
,326181
,326252
,326389
,326707
,326717
,327206
,327390
,327424
,327505
,327514
,327565
,327707
,327850
,327909
,328254
,328870
,329035
,329078
,329230
,329751
,330170
,330206
,330245
,330881
,331201
,331615
,331676
,331981
,332073
,332120
,332187
,332188
,332189
,332320
,332363
,332365
,332403
,332408
,332421
,332550
,332732
,332823
,333001
,333016
,333037
,333107
,333154
,333196
,333208
,333368
,333460
,333604
,333684
,333729
,333750
,333758
,333879
,333880
,333881
,333884
,333885
,333887
,333992
,334079
,334093
,334137
,334163
,334165
,334167
,334169
,334174
,334177
,334178
,334221
,334242
,334246
,334265
,334418
,334447
,334507
,334647
,334818
,334820
,334825
,334830
,334831
,334832
,334834
,334836
,334837
,334838
,334840
,334841
,334842
,334907
,334908
,334909
,334927
,334948
,335059
,335175
,335470
,335473
,335682
,335912
,335955
,336006
,336042
,336063
,336113
,336130
,336131
,336132
,336133
,336134
,336135
,336162
,336163
,336164
,336165
,336167
,336168
,336169
,336170
,336171
,336172
,336173
,336175
,336176
,336177
,336179
,336182
,336185
,336186
,336187
,336194
,336195
,336200
,336201
,336206
,336398
,336401
,336444
,336629
,336686
,336847
,336852
,337093
,337104
,337436
,337567
,337584
,337655
,337807
,337935
,338132
,338243
,338253
,338254
,338454
,338476
,338573
,338574
,338630
,338797
,338801
,338910
,338911
,338914
,338915
,338916
,338917
,338977
,338980
,339025
,339070
,339111
,339138
,339273
,339276
,339280
,339281
,339303
,339374
,339375
,339428
,339454
,339582
,339756
,339797)
""", engine_mssql)
job.loc[job['ContactID']==346953]
job.loc[(job['ContactID'] == 329761), 'ContactID'] = 331334
job.loc[(job['ContactID'] == 334327), 'ContactID'] = 332574
job.loc[(job['ContactID'] == 351705), 'ContactID'] = 346953
job.loc[(job['ContactID'] == 346927), 'ContactID'] = 346894
job.loc[(job['ContactID'] == 355562), 'ContactID'] = 355911
job.loc[(job['ContactID'] == 349116), 'ContactID'] = 354796
job.loc[(job['ContactID'] == 353511), 'ContactID'] = 349862
job.loc[(job['ContactID'] == 326632), 'ContactID'] = 327514
job.loc[(job['ContactID'] == 351124), 'ContactID'] = 347456
job.loc[(job['ContactID'] == 350519), 'ContactID'] = 351811
job.loc[(job['ContactID'] == 357042), 'ContactID'] = 355462
job.loc[(job['ContactID'] == 335594), 'ContactID'] = 335473
job.loc[(job['ContactID'] == 346741), 'ContactID'] = 346740
job.loc[(job['ContactID'] == 360946), 'ContactID'] = 355687
job.loc[(job['ContactID'] == 355688), 'ContactID'] = 355687
job.loc[(job['ContactID'] == 355045), 'ContactID'] = 351201
job.loc[(job['ContactID'] == 353243), 'ContactID'] = 359521
job.loc[(job['ContactID'] == 350564), 'ContactID'] = 352836
job.loc[(job['ContactID'] == 352468), 'ContactID'] = 354577
job.loc[(job['ContactID'] == 348848), 'ContactID'] = 348062
job.loc[(job['ContactID'] == 362085), 'ContactID'] = 361660
job.loc[(job['ContactID'] == 358649), 'ContactID'] = 358119
job.loc[(job['ContactID'] == 361890), 'ContactID'] = 361826
job.loc[(job['ContactID'] == 332652), 'ContactID'] = 326181
job.loc[(job['ContactID'] == 357121), 'ContactID'] = 361786
job.loc[(job['ContactID'] == 358405), 'ContactID'] = 358403
job.loc[(job['ContactID'] == 361937), 'ContactID'] = 361718
job.loc[(job['ContactID'] == 331082), 'ContactID'] = 343065
job.loc[(job['ContactID'] == 338687), 'ContactID'] = 320993
job.loc[(job['ContactID'] == 352631), 'ContactID'] = 353027
job.loc[(job['ContactID'] == 356703), 'ContactID'] = 356702
job['Manager'] = job['Manager'].fillna('')
job['Consultant'] = job['Consultant'].fillna('')
job['matcher1'] = job['Manager'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
job['matcher2'] = job['Consultant'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
job = job.merge(user[['matcher','Email Address']].rename(columns={'Email Address':'owner1'}), left_on='matcher1',right_on='matcher',how='left')
job = job.merge(user[['matcher','Email Address']].rename(columns={'Email Address':'owner2'}), left_on='matcher2',right_on='matcher',how='left')
job['owner1'] = job['owner1'].where(job['owner1'].notnull(),None)
job['owner2'] = job['owner2'].where(job['owner1'].notnull(),None)
job['owner1'] = job['owner1'].apply(lambda x: str(x) if x else x)
job['owner2'] = job['owner2'].apply(lambda x: str(x) if x else x)
job['owner'] = job[['owner1', 'owner2']].apply(lambda x: ','.join(set([e for e in x if e])), axis=1)
job['EnquiryID'] = 'FC'+job['EnquiryID'].astype(str)
job['ContactID'] = 'FC'+job['ContactID'].astype(str)
job['position-companyId'] =''

candidate = pd.read_sql("""
select CandidateID, FirstName, Surname, nullif(HomeEmail,'') as HomeEmail, Recruiter,MiddleName from Candidates
""", engine_mssql) #59359
candidate['Recruiter'] = candidate['Recruiter'].fillna('')
candidate['matcher'] = candidate['Recruiter'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
candidate = candidate.merge(user, on='matcher',how='left')
candidate['CandidateID'] = 'FC'+candidate['CandidateID'].astype(str)
assert False
# %% transpose
company = company.where(company.notnull(),None)
contact = contact.where(contact.notnull(),None)
job = job.where(job.notnull(),None)
candidate = candidate.where(candidate.notnull(),None)
company.rename(columns={
    'ClientID': 'company-externalId',
    'CompanyName': 'company-name',
    'Email Address': 'company-owners',
}, inplace=True)
company = vincere_standard_migration.process_vincere_comp(company, mylog)

contact.rename(columns={
    'ContactID': 'contact-externalId',
    'ClientID': 'contact-companyId',
    'LastName': 'contact-lastName',
    'FirstName': 'contact-firstName',
    'MiddleName': 'contact-middleName',
     'Email': 'contact-email',
     'Email Address': 'contact-owners',
}, inplace=True)
contact, default_company = vincere_standard_migration.process_vincere_contact_2(contact)

job.rename(columns={
    'EnquiryID': 'position-externalId',
    'ContactID': 'position-contactId',
    'RealTitle': 'position-title',
    'owner': 'position-owners',
}, inplace=True)
job, default_contacts = vincere_standard_migration.process_vincere_job_2(job, mylog)

candidate.rename(columns={
    'CandidateID': 'candidate-externalId',
    'FirstName': 'candidate-firstName',
    'Surname': 'candidate-lastName',
    'MiddleName': 'candidate-middleName',
    'HomeEmail': 'candidate-email',
    'Email Address': 'candidate-owners',
}, inplace=True)
candidate = vincere_standard_migration.process_vincere_cand(candidate, mylog)

# %% to csv files
candidate.to_csv(os.path.join(standard_file_upload, '6_candidate_tj.csv'), index=False)
job.to_csv(os.path.join(standard_file_upload, '5_job_tj.csv'), index=False)
contact.to_csv(os.path.join(standard_file_upload, '4_contact_tj.csv'), index=False)
company.to_csv(os.path.join(standard_file_upload, '2_company_tj.csv'), index=False)
if len(default_contacts):
    default_contacts.to_csv(os.path.join(standard_file_upload, '3_default_contacts_tj.csv'), index=False)

tem = default_contacts.loc[default_contacts['contact-companyId'] == 'DEFAULT_COMPANY']
if len(default_company):
    default_company.to_csv(os.path.join(standard_file_upload, '1_default_company_tj.csv'), index=False)
elif len(tem):
    default_company = pd.DataFrame({'company-externalId': ['DEFAULT_COMPANY'], 'company-name': ['DEFAULT COMPANY']})
    default_company.to_csv(os.path.join(standard_file_upload, '1_default_company_tj.csv'), index=False)
