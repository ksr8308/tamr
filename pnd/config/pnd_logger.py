#!/bin/python
import os
import logging
import datetime

def PndLogger(name, process_name=None):
    """
    Return logging.logger with pre-defined format
    :param name: Logger module name
    :return: logging.logger
    """
    # /logs folder    
    current_path = os.path.dirname(os.path.realpath(__file__))
    path_of_logs = os.path.abspath(current_path + "/../../logs")
    if not os.path.exists(path_of_logs):
        os.makedirs(path_of_logs)
    
    logger_level = logging.INFO
    logger = logging.getLogger(name)    
    if len(logger.handlers) > 0:
        return logger # Logger already exists

    logger.setLevel(logger_level)

    sh_formatter = logging.Formatter('%(message)s')
    sh = logging.StreamHandler()
    sh.setLevel(logger_level)
    sh.setFormatter(sh_formatter)
    logger.addHandler(sh)

    fh_formatter = logging.Formatter('%(levelname)6s  %(asctime)24s  [%(name)s, %(lineno)d]  %(message)s')
    if process_name != None :
        fh = logging.FileHandler(path_of_logs + '/{}_{}.log'.format(process_name, datetime.datetime.now().strftime('%Y-%m-%d')))  
    else :
        fh = logging.FileHandler(path_of_logs + '/{}.log'.format(datetime.datetime.now().strftime('%Y-%m-%d')))
    fh.setLevel(logger_level)
    fh.setFormatter(fh_formatter)
    logger.addHandler(fh)

    return logger