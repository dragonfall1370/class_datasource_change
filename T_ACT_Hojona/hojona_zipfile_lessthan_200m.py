# -*- coding: UTF-8 -*-
# import vincere.vincere_common as vc
import psycopg2
import pymssql
import common.connection_string as cs
import pandas as pd
import logger.logger as log
import multiprocessing
import datetime
import common.util_file_folder as uff

logfile = 'hojona.log'
t0 = datetime.datetime.now()
logger = log.get_logger(logfile)

if __name__ == '__main__':
    multiprocessing.freeze_support()
    queue = multiprocessing.Queue()

    source_folder = r'd:\vincere\data_output\hojona\Act! Hojona2.ZIP\Drive\G\ACTDB\Hojona2-database files\Attachments'
    destination_folder = r'd:\vincere\data_output\hojona\vincere_hojana_2_zip'
    sub_folder_prefix = '%s_upload'
    uff.zipfile_lessthan_200m_2(source_folder, destination_folder, sub_folder_prefix, selected_files=None, logger=logger)

    source_folder = r'd:\vincere\data_output\hojona\Act! Hojona427.ZIP\Share\MTAFWSQL1\Hojona427-database files\Attachments'
    destination_folder = r'd:\vincere\data_output\hojona\vincere_hojana_427_zip_soc'
    sub_folder_prefix = '%s_upload'
    uff.zipfile_lessthan_200m_2(source_folder, destination_folder, sub_folder_prefix, selected_files=None, logger=logger)

    logger.info("Completed in %s " %(datetime.datetime.now()-t0))