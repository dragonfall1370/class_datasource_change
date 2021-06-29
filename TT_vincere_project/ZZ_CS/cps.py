# -*- coding: UTF-8 -*-
import sqlalchemy
import numpy as np
import datetime
import pandas as pd
import common.rtf_util as rtfu

# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %% connect db
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://sa:123$%^qwe@dmpfra.vinceredev.com:1433/cps?charset=utf8')
df = pd.read_sql("""select notebookitemid,Memo
from dbo.notebookitemcontent
where 1=1
and nullif(FileExtension,'') is null
and charindex('rtf',Memo) >0""",engine_mssql)

df['Memo_2'] = df['Memo'].map(lambda x: rtfu.rtf_to_text(x) if x else x)

for col in df.columns:
    if df[col].dtype==object:
        df[col]=df[col].apply(lambda x: np.nan if x==np.nan else str(x).encode('utf-8', 'replace').decode('utf-8'))
df.to_csv('cps_rtf_2.csv',index=False)