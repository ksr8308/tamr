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
import numpy as np
from multiprocessing import Process, Manager, Pool

current_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(current_path + '/data')
from data_manager import DataManager

sys.path.append(current_path + '/config')
from config_manager import ConfigManager
from pnd_logger import PndLogger

if __name__ == "__main__":    
    process_name = "test"
    logger = PndLogger("test", process_name)    
    dm_legacy = DataManager(ConfigManager("legacy"), process_name)    
    dm_mdm = DataManager(ConfigManager("mdm"), process_name)    
    dm_mixed = DataManager(ConfigManager("mixed"), process_name)    

    dm = DataManager(ConfigManager("legacy"), process_name)        
    for tb in dm.get_info_tables() :
        logger.info(tb)


    # logger.info("========================legacy========================")
    # dm_legacy.connect()    
    # for tb in dm_legacy.get_all_table_columns() :
    #     logger.info(tb)    
    # dm_legacy.disconnect()    
    # logger.info("========================mdm========================")
    # dm_mdm.connect()
    # for tb in dm_mdm.get_all_table_columns() :
    #     logger.info(tb)
    # dm_mdm.disconnect()
    # logger.info("========================mixed========================")
    # dm_mixed.connect()
    # for tb in dm_mixed.get_all_table_columns() :
    #     logger.info(tb)
    # dm_mixed.disconnect()