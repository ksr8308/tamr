#!/bin/python
import os
import sys
import logging
import argparse
import cx_Oracle
import pandas as pd
import numpy as np
import csv
import glob
import re
import json
import base64
import requests
from os.path import basename
from chardet.universaldetector import UniversalDetector
from tamr_unify_client.auth import UsernamePasswordAuth

current_path = os.path.dirname(os.path.realpath(__file__))

sys.path.append(current_path + '/../src/')
from data_preprocessor import DataPreprocessor

sys.path.append(current_path + '/config')
from config_manager import ConfigManager
from pnd_logger import PndLogger

sys.path.append(current_path + '/data')
from data_manager import DataManager
from custom_unify import CustomUnify

class MixedSampler :
    def __init__(self, data_manager) :
        logger_name = "sampling"
        self.__logger = PndLogger("Extend sample Material", logger_name)
        self.__dm = data_manager
        self.__cm_database = ConfigManager("mixed", logger_name)
        self.__cm_unify = ConfigManager("unify", logger_name)

        self._unify_auth = UsernamePasswordAuth("admin", "dt")
        auth = '{}:{}'.format(self.__cm_unify.creds["user"], self.__cm_unify.creds["pwd"])
        encoded = base64.b64encode(auth.encode('latin1'))
        
        self._basicCreds = 'BasicCreds ' + requests.utils.to_native_string(encoded.strip())
        self._profileDatasetName = self.__cm_database.creds["profileDatasetName"]
        self._jdbc_url = "jdbc:tamr:oracle://{}:{};ServiceName={}".format(self.__cm_database.creds["host"], self.__cm_database.creds["port"],self.__cm_database.creds["sid"])
        self._jdbc_user = self.__cm_database.creds["user"]
        self._jdbc_password = self.__cm_database.creds["pwd"]
        self._url = "{}://{}:{}/api/jdbcIngest/profile".format(self.__cm_unify.creds["protocol"], self.__cm_unify.creds["hostname"], self.__cm_unify.creds["connectPort"])

    def get_sampled_data(self) :
        create_table_statement = "declare table_cnt number; begin select count(*) into table_cnt from user_tables where table_name = '{}'; if table_cnt > 0 then execute immediate 'drop table {}'; end if; execute immediate 'create table {} as select CN_MATL_DESC,ITG_MATL_ID,LEAF_CLASS_CD,LEAF_CLASS_NM,LOCK_CMT,MATC_MATL_DET_CATG_VAL,MATL_0_LEVEL_CD,MATL_1_LEVEL_CD,MATL_2_LEVEL_CD,MATL_3_LEVEL_CD,MATL_4_LEVEL_CD,MATL_5_LEVEL_CD,MATL_CHGR_CD_LVAL,MATL_DESC,MATL_GRP_DET_GBN_CD,MATL_GRP_ID,MATL_ID,MATL_NM_1,MATL_NM_2,MDM_STAT_CD,ORG_MATL_ID,PLANT_LVAL,PLANT_MATL_LVAL,PLANT_PURCHASE_GRP_LVAL,RGL_IRRGL_MATL_LVAL,SAP_MATL_DET_GRP_CD,SAP_MATL_STAT_LVAL,SAP_PLANT_LVAL,SHE_APPROVE_RSLT_VAL,SHE_USE_LVAL,SHORT_MATL_DESC,TECH_ATTR_GBN_CD,UOM_CD,USE_PLANT_LVAL,VENDOR_NM,VENDOR_PARTS_NO{} from TGF_MATL_M where matl_grp_det_gbn_cd = ''{}'' and tech_attr_gbn_cd = ''{}'''; end;"
        drop_table_statement = "drop table {}"
        mat_codes = self.__dm.get_matrial_unique_codes()
        mat_attr_columns = self.__dm.get_matrial_attr_columns()
        fail_cnt = 0
        success_cnt = 0
        failed_mat_codes = []

        for code in mat_codes :
            bindvar = {"table_name": "", "grp_cd": "", "attr_cd": "", "add_columns" : ""}
            add_cols = ','.join(['{} as "{}"'.format(row["TECH_COL_ID"], row["ATTR_EN_NM"].upper()) for row in mat_attr_columns if row["MATL_GRP_DET_GBN_CD"] == code["MATL_GRP_DET_GBN_CD"] and row["TECH_ATTR_GBN_CD"] == code["TECH_ATTR_GBN_CD"]])

            bindvar["table_name"] = "TGF_MATL_M__{}__{}".format(code["MATL_GRP_DET_GBN_CD"],code["TECH_ATTR_GBN_CD"]).replace(" ", "_").upper()
            bindvar["grp_cd"] = code["MATL_GRP_DET_GBN_CD"]
            bindvar["attr_cd"] = code["TECH_ATTR_GBN_CD"]
            bindvar["add_columns"] = "," + add_cols if len(add_cols) > 0 else ""
        
            create_query = create_table_statement.format(bindvar["table_name"], bindvar["table_name"], bindvar["table_name"], bindvar["add_columns"], bindvar["grp_cd"], bindvar["attr_cd"])
            drop_query = drop_table_statement.format(bindvar["table_name"])
        
            dm.execute_query(create_query)
            result = self.profile(bindvar["table_name"])

            if result == False :
                fail_cnt += 1
                self.__logger.info("[Source=mixed] Profiling table {} failed.".format(bindvar["table_name"]))
                failed_mat_codes.append(code)
            else :
                success_cnt += 1
                self.__logger.info("[Source=mixed] Profiling table {} Completed.".format(bindvar["table_name"]))                
            dm.execute_query(drop_query)
        
        attempt_cnt = 1
        while len(failed_mat_codes) != 0 :
            self.__logger.info("[Source=mixed] Profiling number of retries [{}].".format(attempt_cnt))
            idx = 0
            success_cnt = 0
            fail_cnt = 0

            for code in failed_mat_codes :
                bindvar = {"table_name": "", "grp_cd": "", "attr_cd": "", "add_columns" : ""}
                add_cols = ','.join(['{} as "{}"'.format(row["TECH_COL_ID"], row["ATTR_EN_NM"].upper()) for row in mat_attr_columns if row["MATL_GRP_DET_GBN_CD"] == code["MATL_GRP_DET_GBN_CD"] and row["TECH_ATTR_GBN_CD"] == code["TECH_ATTR_GBN_CD"]])

                bindvar["table_name"] = "TGF_MATL_M__{}__{}".format(code["MATL_GRP_DET_GBN_CD"],code["TECH_ATTR_GBN_CD"]).replace(" ", "_").upper()
                bindvar["grp_cd"] = code["MATL_GRP_DET_GBN_CD"]
                bindvar["attr_cd"] = code["TECH_ATTR_GBN_CD"]
                bindvar["add_columns"] = "," + add_cols if len(add_cols) > 0 else ""
        
                create_query = create_table_statement.format(bindvar["table_name"], bindvar["table_name"], bindvar["table_name"], bindvar["add_columns"], bindvar["grp_cd"], bindvar["attr_cd"])
                drop_query = drop_table_statement.format(bindvar["table_name"])
        
                dm.execute_query(create_query)
                result = self.profile(bindvar["table_name"])

                if result == False :
                    fail_cnt += 1
                    self.__logger.info("[Source=mixed] Profiling table {} failed.".format(bindvar["table_name"]))                    
                else :
                    success_cnt += 1
                    self.__logger.info("[Source=mixed] Profiling table {} Completed.".format(bindvar["table_name"]))
                    failed_mat_codes.remove(bindvar["table_name"])
                dm.execute_query(drop_query)

            if attempt_cnt > 5 :
                self.__logger.info("[Source=mixed] attempt count over stop.")
                break
            attempt_cnt += 1

        self.__logger.info("[Source=mixed] Profiling Completed.")        
        if len(failed_mat_codes) != 0 :
            self.__logger.info("[Source=mixed] Profiling failed tables={}".format(','.join(failed_mat_codes)))
        else : 
            self.__logger.info("[Source=mixed] All tables profiling SUCCESSED.")

    def profile(self, table):
        queryConfig = \
            {
                "queryConfig": {
                    "jdbcUrl": self._jdbc_url,
                    "dbUsername": self._jdbc_user,
                    "dbPassword": self._jdbc_password,
                    "fetchSize": 1
                },
                "queryTargetList": [
                    {
                        "query": "SELECT * FROM {}".format(table),
                        "datasetName": self._profileDatasetName,
                        "primaryKey": []
                    }
                ]
            }        
        self.__logger.debug("df connect info = {}".format(queryConfig["queryTargetList"]))
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json', 'Authorization': self._basicCreds}
        response = requests.post(self._url, headers=headers, data=json.dumps(queryConfig))
        if response.status_code != 200:
            self.__logger.error("Request to profile {} has failed".format(table))
            self.__logger.error(response.text)
            return False
        return

if __name__ == "__main__" :
    logger_name = "sampling"
    logger = PndLogger("Extend sample main", logger_name)
    parser = argparse.ArgumentParser()  
    subparsers = parser.add_subparsers(dest="command", help="help for subcommands")
    subparsers.required = True
    parser_sampling = subparsers.add_parser("sample", help="sample help")
    parser_sampling.add_argument("-n", "--name", dest="name", type=str, choices=["mixed", "erp", "dap"], default=None, required=True, help="name of data source")
    parser_sampling.add_argument("-r", "--reload", dest="does_reload", help="reload all tables", action="store_true")
    args = parser.parse_args()
    
    cm = ConfigManager(args.name, logger_name)
    unify = CustomUnify(ConfigManager("unify", logger_name), logger_name)
    dm = DataManager(args.name, "unify", logger_name)

    # Sample data
    if cm is None :
        logger.error("None or more than one source found with the specified source name '{}'".format(args.name))
        exit(0)
    
    if args.does_reload :
        dataset_name = cm.creds["profileDatasetName"]
        if unify.versioned.is_exist_dataset(dataset_name) :
            unify.dataset.truncate_dataset(dataset_name)
            logger.info("{} is truncated.".format(dataset_name))
        else :
            logger.info("{} not exist.".format(dataset_name))
        
    if args.name == "mixed" :
        logger.info("Sampling mixed...")
        sampler = MixedSampler(dm)
        sampler.get_sampled_data()        

    if args.name == "erp" :
        logger.info("Sampling ERP...")
        sampler = ErpSampler(dm)
        sampler.get_sampled_data()

    logger.info("finish.")

    