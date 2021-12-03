#!/bin/python
import os
import logging
import datetime

def CustomLogger(name):
    """
    Return logging.logger with pre-defined format
    :param name: Logger module name
    :return: logging.logger
    """
    # /logs folder    
    current_path = os.path.dirname(os.path.realpath(__file__))
    path_of_logs = os.path.abspath(current_path + "/../logs")
    if not os.path.exists(path_of_logs):
        os.makedirs(path_of_logs)
    
    logger = logging.getLogger(name)
    if len(logger.handlers) > 0:
        return logger # Logger already exists
    
    logger.setLevel(logging.INFO)
    
    sh_formatter = logging.Formatter('%(message)s')
    sh = logging.StreamHandler()
    sh.setLevel(logging.INFO)
    sh.setFormatter(sh_formatter)
    logger.addHandler(sh)
    
    fh_formatter = logging.Formatter('%(levelname)6s  %(asctime)24s  [%(name)s, %(lineno)d]  %(message)s')
    fh = logging.FileHandler(path_of_logs + '/{}.log'.format(datetime.datetime.now().strftime('%Y-%m-%d')))
    fh.setLevel(logging.INFO)
    fh.setFormatter(fh_formatter)
    logger.addHandler(fh)

    return logger