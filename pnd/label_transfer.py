#!/bin/python
import os
import sys
import csv
import logging
import argparse
import time
import datetime
import json
import requests
import pandas as pd
import tamr_unify_client as api
from tamr_unify_client.auth import UsernamePasswordAuth

current_path = os.path.dirname(os.path.realpath(__file__))

sys.path.append(current_path + '/config')
from config_manager import ConfigManager
from pnd_logger import PndLogger

sys.path.append(current_path + '/data')
from custom_unify import CustomUnify

def json_stream(it, **kwargs):    
    for i in it:        
        yield '{}'.format(json.dumps(i, **kwargs))

def get_table(path_to_file, logger):    
    result = []
    if os.path.exists(path_to_file):
        try:            
            # with open(path_to_file, encoding='utf-8') as input_file:
                # reader = csv.DictReader(input_file, delimiter=',', quotechar='"', escapechar='\\')
                # reader = csv.DictReader(input_file, delimiter='\t', quotechar='"', escapechar='\\')
                # for row in reader:                    
                #     logger.info(row)
            # return result
            reader = pd.read_excel('./' + path_to_file)
            for row in reader.values.tolist() :
                if row[2] != row[3] :
                    table_dict  = {'oracle':row[2], 'hive':row[3]}
                    result.append(table_dict)            
            return result
        except Exception as e:
            logger.error(e)
            return None
    else:
        logger.error("File {} doesn't exist".format(path_to_file))
        return None

def get_labels(unify, dataset_name, new_dataset_name) :
    labels = unify.dedup.get_mastering_labels(dataset_name)
    uploads = []    
    for label in labels:
        label_dict = {'datasetName1': new_dataset_name}        
        label_dict['datasetName2'] = new_dataset_name
        label_dict['originTransactionId1'] = label['originTransactionId1']
        label_dict['originTransactionId2'] = label['originTransactionId2']
        label_dict['originDatasetName1'] = label['originDatasetName1']
        label_dict['originDatasetName2'] = label['originDatasetName2']
        label_dict['transactionId1'] = label['transactionId1']
        label_dict['transactionId2'] = label['transactionId2']
        label_dict['manualLabel'] = label['manualLabel']
        uploads.append(label_dict)
    return uploads

if __name__ == "__main__":
    process_name = "label transfer"
    logger = PndLogger("transfer", process_name)
    cm = ConfigManager("unify")
    unify = CustomUnify(cm, process_name)

    labels = get_labels(unify, "Exception_Column_NEW_unified_dataset", "label_exception2_unified_dataset")    
    # labels = get_labels(unify, "Schema_Discovery_unified_dataset", "label_transfer_unified_dataset")
    
    table_map = get_table("RDB_HIVE.xlsx", logger)
    
    for lb in labels :
        for table in table_map :
            if table['oracle'] in str(lb['originTransactionId1']) :
                logger.info('originTransactionId1: {} -> {}'.format(lb['originTransactionId1'], str(lb['originTransactionId1']).replace(table['oracle'], table['hive'])))
                lb['originTransactionId1'] = str(lb['originTransactionId1']).replace(table['oracle'], table['hive'])

            if table['oracle'] in str(lb['originTransactionId2']) :                 
                lb['originTransactionId2'] = str(lb['originTransactionId2']).replace(table['oracle'], table['hive'])
                logger.info('originTransactionId2: {} -> {}'.format(lb['originTransactionId2'], str(lb['originTransactionId2']).replace(table['oracle'], table['hive'])))

    for lb in labels :
        logger.info(lb)    
    logger.info("transfer label count = {}".format(len(labels)))
    payload = ''.join(json_stream(labels))
    unify.dedup.upload_mastering_labels("label_transfer_unified_dataset", payload) 
    
    # for label in old_labels :        
    #     logger.info("join = {}".format(''.join(label)))
    #     logger.info("stream = {}".format(json_stream(label)))

    # logger.info("=================run===================")
    # for label in old_labels:
    #     logger.info(label)

    # logger.info("====================================")
    # for label in unify.dedup.get_mastering_labels("label_exception2_unified_dataset") :
    #     logger.info(label)