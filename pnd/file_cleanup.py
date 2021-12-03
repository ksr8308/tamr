#!/bin/python
import os
import sys
import logging
import argparse
import re
import json
import pandas as pd
import time
import datetime
import shutil

current_path = os.path.dirname(os.path.realpath(__file__))

sys.path.append(current_path + '/config')
from pnd_logger import PndLogger

if __name__ == "__main__":    
    process_name = "clear_file"
    logger = PndLogger("Clear files", process_name)

    log_file_path = "/home/pnd/customers-skhynix/logs"
    files = os.listdir(log_file_path)
    base_dt = (datetime.datetime.now() - datetime.timedelta(days=14)).date()
    for file in files :
        file_path = "{}/{}".format(log_file_path, file)
        create_dt =  datetime.datetime.strptime(re.search(r'\d{4}-\d{2}-\d{2}', file).group(), '%Y-%m-%d').date()
        
        try:
            if base_dt > create_dt and os.path.isfile(file_path) :
                os.remove(file_path)
                logger.info("Deleted complete file={}".format(file_path))
        except Exception as e:
            logger.error(e)

    log_file_path = "/home/pnd/airflow/logs"
    dirs = os.listdir(log_file_path)
    for dir in os.listdir(log_file_path) :
        dir_path = "{}/{}".format(log_file_path, dir)
        try:
            if os.path.isdir(dir_path) :
                shutil.rmtree(dir_path)
                logger.info("Deleted complete forder={}".format(dir_path))
        except Exception as e:
            logger.error(e)

    log_file_path = "/home/tamr/tamr/unify-data/job/sparkEventLogs"    
    files = os.listdir(log_file_path)
    for file in files :
        file_path = "{}/{}".format(log_file_path, file)
        try:
            if os.path.isfile(file_path) :
                os.remove(file_path)
                logger.info("Deleted complete file={}".format(file_path))
        except Exception as e:
            logger.error(e)

    log_file_path = "/backup/unify_backup"
    dirs = os.listdir(log_file_path)
    backup_dirs = []
    idx = 1
    for dir in dirs :
        dir_status = {"no": idx, "name": dir, "file_path": os.path.join(log_file_path, dir) }
        try:
            if os.path.isdir(dir_status["file_path"]) :
                files = os.listdir(os.path.join(log_file_path, dir))
                if "_CANCELED" in ','.join(files) or "_FAILED" in ','.join(files) : 
                    shutil.rmtree(dir_status["file_path"])
                    logger.info("Deleted complete forder={}".format(dir_status["file_path"]))
                elif "_SUCCEEDED" in files :
                    dir_status["status"] = "SUCCEEDED"
                    backup_dirs.append(dir_status)
                    idx += 1
        except Exception as e :
            logger.error(e)

    for dir in backup_dirs :   
        logger.info(dir)    
        try:
            if dir["no"] <= (len(backup_dirs) - 2) :
                shutil.rmtree(dir["file_path"])
                logger.info("Deleted complete forder={}".format(dir["file_path"]))
        except Exception as e :
            logger.error(e)
