# -*- coding: UTF-8 -*-
from vincere import vincere_common
import logger.logger as log

mylog = log.get_logger("rlc.log")
vincere_common.upload_files_to_s3(upload_folder=r"D:\vincere\data_output\rlc"
                                  , key='rlcasia.vincere.io/upload/file/6277a401-1a6f-4532-9731-6756deb857f6/'
                                  , bucket='sin-vc-p1-file'
                                  , log=mylog
                                  )
