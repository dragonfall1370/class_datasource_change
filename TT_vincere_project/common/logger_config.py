import logging

def get_degub_logger(log_file, stream_handler_included=True):
    # create mylog
    logging.basicConfig(filename=log_file, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y/%m/%d %I:%M:%S %p', level=logging.DEBUG)
    logger = logging.getLogger(__name__)

    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y/%m/%d %I:%M:%S %p')

    # add ch to mylog
    if stream_handler_included:
        # create console handler and set level to debug
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        # add formatter to ch
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    # 'application' code
    """
    mylog.debug('debug message')
    mylog.info('info message')
    mylog.warn('warn message')
    mylog.error('error message')
    mylog.critical('critical message')
    """
    return logger

def get_info_logger(log_file, stream_handler_included=True):
    # create mylog
    logging.basicConfig(filename=log_file, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y/%m/%d %I:%M:%S %p', level=logging.INFO)
    logger = logging.getLogger(__name__)

    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y/%m/%d %I:%M:%S %p')

    # add ch to mylog
    if stream_handler_included:
        # create console handler and set level to debug
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        # add formatter to ch
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    # 'application' code
    """
    mylog.debug('debug message')
    mylog.info('info message')
    mylog.warn('warn message')
    mylog.error('error message')
    mylog.critical('critical message')
    """
    return logger
