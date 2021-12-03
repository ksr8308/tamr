#!/bin/python
import os
import sys
import logging
import argparse

current_path = os.path.dirname(os.path.realpath(__file__))

sys.path.append(current_path + '/config')
from config_manager import ConfigManager
from pnd_logger import PndLogger

sys.path.append(current_path + '/data')
from data_manager import DataManager

if __name__ == "__main__":
    process_name = "set_cluster"
    logger = PndLogger("Settings Clusters", process_name)

    logger.info("Start set cluster lock.")
    manager = DataManager("legacy", "exception", process_name)

    ids = []
    
    rows = manager.get_unified_published_clusters()
    for row in rows:        
        if row["locked"] == True :            
            ids.append(row["persistentId"])
    ids = list(set(ids))

    dataset_name = "Exception column_unified_dataset"
    manager.set_lock_clusters(dataset_name, ids)
    logger.info("finish.")