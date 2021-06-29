edm_production_logininfo = {'server': '10.10.2.6', 'user': 'vreport0101', 'password': '3dmR3p0rt@', 'database': 'COMPANY_EDM_VN'}
edm_uat3_logininfo = {'server': '10.10.1.148', 'user': 'vsql0103', 'password': 'Life123456@', 'database': 'COMPANY_EDM_UAT_3'}

sis_prd_logininfo = {'server': '10.10.2.16', 'user': 'vsis', 'password': 'vsis#gvl#2016', 'database': 'sis_prd'}
sis_prd_issue_1_logininfo = {'server': '10.10.1.69', 'user': 'vsql0101', 'password': '1234Qwer', 'database': 'sis_prd_issue_1'}
sis_ap_pilot_logininfo = {'server': '10.10.1.69', 'user': 'vsql0101', 'password': '1234Qwer', 'database': 'sis_ap_pilot'}
sis_dev = {'server': '10.10.1.69', 'user': 'vsql0101', 'password': '1234Qwer', 'database': 'sis_dev'}

# connection string for sqlalchemy: dialect+driver://username:password@host:port/database
mssql_pymssql_sis_prd_issue_1 = 'mssql+pymssql://vsql0101:1234Qwer@10.10.1.69:1433/sis_prd_issue_1?charset=utf8'
mssql_pymssql_sis_dev = 'mssql+pymssql://vsql0101:1234Qwer@10.10.1.69:1433/sis_dev?charset=utf8'
mssql_pymssql_sis_prd = 'mssql+pymssql://vsis:vsis#gvl#2016@10.10.2.16:1433/sis_prd?charset=utf8'

# sqlite://<nohostname>/<path>
# where <path> is relative:
sqlite_foodb = 'sqlite:///foo.db'
sqlite_data = 'sqlite:///../database_sqlite/data.db'

# vincere sqlserver 2017
client_rlc = {'server': 'dmp.vinceredev.com', 'user': 'sa', 'password': '123$%^qwe', 'database': 'rlc', 'port': '1433'}
client_rlc_prd = {'server': 'dmp.vinceredev.com', 'user': 'sa', 'password': '123$%^qwe', 'database': 'RLCPROD', 'port': '1433'}
client_edenscott = {'server': 'dmpfra.vinceredev.com', 'user': 'sa', 'password': '123$%^qwe', 'database': 'edenscott', 'port': '1433'}
client_hojona_mentalhealth = {'server': 'dmpfra.vinceredev.com', 'user': 'sa', 'password': '123$%^qwe', 'database': 'Hojona2', 'port': '1433'}
client_hojona_socialcare = {'server': 'dmpfra.vinceredev.com', 'user': 'sa', 'password': '123$%^qwe', 'database': 'Hojona427', 'port': '1433'}
client_troy_quay = {'server': 'dmpfra.vinceredev.com', 'user': 'sa', 'password': '123$%^qwe', 'database': 'troy_quay', 'port': '1433'}

# vincere postgres
review_tung_vincere_io = {'server': 'randstad.vincere.io', 'user': 'tung', 'password': 'xD9LkVY2l6', 'database': 'tung.vincere.io', 'port': '15432'}
review_tung_vincere_io_p35432 = {'server': 'randstad.vincere.io', 'user': 'tung', 'password': 'xD9LkVY2l6', 'database': 'tung.vincere.io', 'port': '35432'}
review_rlc = {'server': 'randstad.vincere.io', 'user': 'tung', 'password': 'xD9LkVY2l6', 'database': 'rlc-review.vincere.io', 'port': '15432'}
review_edenscott = {'server': 'randstad.vincere.io', 'user': 'tung', 'password': 'xD9LkVY2l6', 'database': 'edenscott-review.vincere.io', 'port': '15432'}
review_paritas = {'server': '35.158.17.10', 'user': 'tung', 'password': 'xD9LkVY2l6', 'database': 'paritasrecruitment-review.vincere.io', 'port': '15432'}

# vincere production
production_bpmtech = {'server': '35.158.17.10', 'user': 'tung', 'password': 'xD9LkVY2l6', 'database': 'bpmtech.vincere.io', 'port': '15432'}
production_paritas = {'server': '35.158.17.10', 'user': 'tung', 'password': 'xD9LkVY2l6', 'database': 'paritas.vincere.io', 'port': '15432'}
production_hojona_mentalhealth = {'server': '35.158.17.10', 'user': 'tung', 'password': 'xD9LkVY2l6', 'database': 'hojona-mentalhealth.vincere.io', 'port': '15432'}
production_hojona_socialworkers = {'server': '35.158.17.10', 'user': 'tung', 'password': 'xD9LkVY2l6', 'database': 'hojona-socialworkers.vincere.io', 'port': '15432'}
production_rlc_p35432 = {'server': 'randstad.vincere.io', 'user': 'tung', 'password': 'xD9LkVY2l6', 'database': 'rlcasia.vincere.io', 'port': '35432'}

url_jdbc_postgre_tung_vincere_io = 'jdbc:postgresql://randstad.vincere.io:15432/tung.vincere.io'
url_jdbc_mssql_troy_quay = 'jdbc:sqlserver://dmpfra.vinceredev.com:1433;database=troy_quay'

