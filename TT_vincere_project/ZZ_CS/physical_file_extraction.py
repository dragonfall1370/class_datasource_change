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

# %% connect db
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://sa:123$%^qwe@dmpfra.vinceredev.com:1433/cps?charset=utf8')

# %% query data and save to physical files
script = r"""
with tmp as(
select top 5 notebookitemid as readable_filename,REPLACE(CONCAT('RS_TIMESHEET_',notebookitemid,fileextension),'.txt','.doc') as file_name ,Memo as file_data
from dbo.notebookitemcontent
where nullif(fileextension,'') is not null
and nullif(Memo,'') is not null)
select * from tmp
order by readable_filename
OFFSET 0 ROWS FETCH NEXT 1000 ROWS ONLY;
"""
vincere_custom_migration.get_files_from_dbms_save_to_physical_files(script, engine_mssql, 'D:\\Tony\\project\\douglasjackson\\New folder', 1000)


