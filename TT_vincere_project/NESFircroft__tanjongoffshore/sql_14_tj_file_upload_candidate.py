# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import time
import re
import os
import psycopg2
import common.vincere_common as vincere_common
import common.vincere_custom_migration as vincere_custom_migration
import common.vincere_standard_migration as vincere_standard_migration
import sqlalchemy
import datetime
from functools import reduce
from common import vincere_job_application
import pandas as pd

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
# data_folder = cf['default'].get('data_folder')
data_folder = '/Users/truongtung/Desktop'

data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
dest_db = cf[cf['default'].get('dest_db')]
src_db = cf[cf['default'].get('src_db')]
sqlite_path = cf['default'].get('sqlite_path')
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')
# engine_postgre_src = sqlalchemy.create_engine(conn_str_sdb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
# engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
ddbconn = engine_postgre_review.raw_connection()
document_path_cand = r'D:\Tony\File\tanjong\CandidateAttachments (admin)\file'

# %% extract data
temp_msg_metadata_cand = vincere_common.get_folder_structure(document_path_cand)
temp_msg_metadata_cand['matcher'] = temp_msg_metadata_cand['file'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

cand_sql = pd.read_sql("""
SELECT CA.AttachmentID,CA.[CandidateID],CA.[AttachmentTypeID],CA.[Description],CA.[FilenameExtension],
  CAT.[CategoryDescription] AS [Category],ATT.[Description] AS [Type]
  FROM CandidateAttachments CA
  LEFT JOIN AttachmentTypes ATT ON ATT.[AttachmentTypeID] = CA.[AttachmentTypeID]
  LEFT JOIN AttachmentTypeCategory CAT ON CAT.[CategoryID] = ATT.[CategoryID]
where CandidateID not in(1127637
,1127489
,1125219
,1124906
,1124419
,1124370
,1121697
,1119915
,1119895
,1105515
,1104882
,1102642
,1097823
,1090998
,1089447
,1089444
,1089433
,1077096
,1076927
,1076619
,1066376
,1061240
,1057333
,1054630
,1051243
,1048144
,1047724
,1045963
,1045448
,1044654
,1042668
,1042401
,1042063
,1042002
,1041030
,1038219
,1038060
,1037524
,1037346
,1037301
,1036830
,1035686
,1032107
,1031665
,1028367
,1028122
,1026651
,1026376
,1024470
,1024437
,1023503
,1023502
,1023501
,1023500
,1023499
,1023498
,1023495
,1023494
,1023061
,1022950
,1021151
,1021102
,1020492
,1013601
,1013600
,1013354
,1013346
,1013344
,1013322
,1013320
,1013317
,1011537
,1009995
,1002756
,1002212
,1002210
,1002203
,1002196
,998580
,998218
,998200
,998193
,994838
,994833
,994829
,994818
,994817
,994815
,994792
,994791
,994790
,994788
,994786
,994783
,994357
,993478
,991214
,976300
,974775
,968308
,966240
,959170
,943750
,925832
,901244
,875950
,874067
,861455
,845390
,829397
,824027
,768716
,763610
,762240
,752229
,686246
,685268
,684683
,655021
,627823
,604108
,598285
,585544
,584128
,571774
,554828
,553136
,550536
,544986
,523948
,515974
,489796
,415355
,193224
,156747
,11075
,1281467
,1276925
,1263450
,1256927
,1256582
,1254040
,1251634
,1249308
,1246753
,1241156
,1235580
,1230659
,1230273
,1226241
,1224064
,1219741
,1219738
,1217766
,1216644
,1216547
,1215790
,1207554
,1212308
,1211719
,1208660
,1208659
,1207707
,1207676
,1207675
,1207528
,1207516
,1207506
,1207505
,1207504
,1207503
,1208744
,1208662
,1207502
,1207341
,1207340
,1207336
,1207333
,1207328
,1207316
,1207305
,1206915
,1206277
,1195461
,1194948
,1194937
,1187710
,1182007
,1168120
,1165943
,1159203
,1157687
,1150217
,1134609
,1134280
,1483844
,1483843
,1481613
,1481604
,1479880
,1476558
,1477936
,1469623
,1469531
,1443369
,1432500
,1431544
,1429125
,1429118
,1429110
,1429078
,1429076
,1429018
,1427480
,1427317
,1426238
,1424840
,1422042
,1422025
,1418403
,1415490
,1406444
,1400682
,1383739
,1383731
,1383705
,1378860
,1373727
,1362487
,1362480
,1362466
,1362459
,1362453
,1362451
,1362431
,1362424
,1362419
,1362413
,1362408
,1362392
,1362388
,1362376
,1362364
,1362322
,1362307
,1362296
,1362290
,1362283
,1362272
,1362266
,1362255
,1362252
,1362249
,1362246
,1362243
,1362236
,1362220
,1361526
,1361517
,1361511
,1361506
,1361501
,1361484
,1361455
,1361426
,1361103
,1361088
,1361081
,1369130
,1368082
,1362495
,1361358
,1361340
,1361331
,1361304
,1361301
,1361216
,1361206
,1361192
,1361162
,1361158
,1361137
,1361125
,1361115
,1360494
,1360486
,1360477
,1360457
,1360448
,1360441
,1360419
,1360405
,1360392
,1360378
,1360340
,1360333
,1360324
,1359123
,1346571
,1334164
,1330382
,1328924
,1327457
,1326418
,1324224
,1323438
,1322781
,1321860
,1318642
,1316270
,1315490
,1310381
,1307984
,1307774
,1306557
,1303726
,1302976
,1302238
,1304416
,1302236
,1300043
,1300013
,1300012
,1300011
,1300007
,1300006
,1300003
,1300001
,1299976
,1289420
,1541713
,1491763
,1695032
,1694185
,1693106
,1693103
,1692634
,1691599
,1689753
,1689504
,1689062
,1686105
,1686027
,1685358
,1679675
,1664017
,1662568
,1662563
,1658153
,1657862
,1657332
,1653170
,1653169
,1651746
,1650087
,1649286
,1648392
,1647518
,1647517
,1645239
,1644021
,1632705
,1632175
,1629283
,1623930
,1621645
,1613971
,1612820
,1600026
,1596966
,1589647
,1582254
,1571859
,1571852
,1571047
,1570196
,1566330
,1560349
,1557946
,1557103
,1545451
,1545942
,1542336
,1540342
,1539720
,1525811
,1522064
,1520517
,1516062
,1510010
,1497818
,1494676
,1493497
,1491585
,1489311
,1484720
,1484714
,1940615
,1935327
,1934700
,1933573
,1931004
,1930963
,1922373
,1922270
,1921601
,1921572
,1921461
,1921428
,1921332
,1917533
,1917414
,1914796
,1912986
,1912075
,1910554
,1902603
,1901630
,1894443
,1887976
,1876927
,1874774
,1874035
,1873807
,1873085
,1857528
,1856212
,1853197
,1852239
,1850602
,1849137
,1849089
,1821663
,1815745
,1815739
,1814251
,1811208
,1804180
,1803139
,1802326
,1798083
,1795623
,1791742
,1785739
,1784820
,1784532
,1776894
,1772813
,1772802
,1771960
,1764954
,1762366
,1762353
,1763154
,1757968
,1755751
,1754939
,1751107
,1749425
,1749405
,1749377
,1745203
,1740428
,1736156
,1730938
,1730361
,1729001
,1719789
,1715507
,1715485
,1713889
,1712586
,1710441
,1708617
,1705772
,1705299
,1705283
,1703809
,1701720
,1971089
,2014695
,2007899
,1984312
,1976095
,1955439
,1951022
,1950973
,1950941
,1948662
,1948645
,1947454)
""", engine_mssql)
cand_sql['CandidateID'] = 'FC'+cand_sql['CandidateID'].astype(str)
cand_sql['AttachmentID'] = cand_sql['AttachmentID'].astype(str)
cand_sql['matcher'] = cand_sql['AttachmentID']+'.'+cand_sql['FilenameExtension']
cand_sql['matcher'] = cand_sql['matcher'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

temp_msg_metadata_cand = temp_msg_metadata_cand.merge(cand_sql, on='matcher')
temp_msg_metadata_cand = temp_msg_metadata_cand.drop_duplicates()
temp_msg_metadata_cand['file_name'] = temp_msg_metadata_cand['alter_file2']
temp_msg_metadata_cand['external_id'] = temp_msg_metadata_cand['CandidateID']
temp_msg_metadata_cand['uploaded_filename'] = temp_msg_metadata_cand['Description']
temp_msg_metadata_cand['primary_document']=0
temp_msg_metadata_cand.loc[temp_msg_metadata_cand['Type']=='CV','primary_document'] = 1
temp_msg_metadata_cand.loc[temp_msg_metadata_cand['uploaded_filename']=='','uploaded_filename'] = temp_msg_metadata_cand['file']
temp_msg_metadata_cand = temp_msg_metadata_cand.loc[temp_msg_metadata_cand['primary_document']==1]
assert False
# %% candidate
candidate_file = vincere_custom_migration.insert_candidate_documents_candidate(temp_msg_metadata_cand, ddbconn, dest_db, mylog)
candidate_file = candidate_file.drop_duplicates()
# assert False
# %% upload files to s3
s3_bucket = cf['default'].get('s3_bucket')
s3_key = cf['default'].get('s3_key')
REGION_HOST = 's3.ap-southeast-1.amazonaws.com'

from common import s3_add_thread_pool

s3_add_thread_pool.upload_multi_files_parallelism_1_2(candidate_file, 'file', 'alter_file2', 'root'
                                                      , bucket=s3_bucket
                                                      , key=s3_key, log=mylog, region_host=REGION_HOST)  # singapore server does not need region host

assert False