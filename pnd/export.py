#!/bin/python
import os
import sys
import logging
import argparse
import time
import datetime
import json
import numpy as np
from multiprocessing import Process, Manager, Pool
from concurrent import futures
from concurrent.futures import ProcessPoolExecutor, wait, as_completed

current_path = os.path.dirname(os.path.realpath(__file__))

sys.path.append(current_path + '/config')
from config_manager import ConfigManager
from pnd_logger import PndLogger

sys.path.append(current_path + '/data')
from data_manager import DataManager
from hive2 import Hive2
from profile_manager import ProfileManager

os.putenv('NLS_LANG', 'KOREAN_KOREA.AL32UTF8')
            
if __name__ == "__main__":
    process_name = "export"
    logger = PndLogger("Export Cluster", process_name)
    parser = argparse.ArgumentParser()  
    parser.add_argument("-t", "--type", dest="type", help="Export Clusters to Database", choices=["schema", "schema_hist", "values", "master", "metadata", "profiled", "table_status", "mixed", "pub_dt"], type=str, default=None)
    args = parser.parse_args()
    dm = DataManager(ConfigManager("tamr"), process_name)

    if args.type == 'schema_hist' :
        logger.info("Start insert cluster schema hist.")
        dm.insert_clusters_schema_hist(dm.get_unified_published_clusters())
        logger.info("Finish.")    
    elif args.type == 'schema' :
        logger.info("Start insert cluster schema.")
        dm.insert_clusters_schema(dm.get_unified_published_clusters())
        logger.info("Finish.")
    elif args.type == 'values' :
        logger.info("Start insert top and values.")
        dm.insert_top_values(dm.get_unified_published_clusters_top_values_to_rows())
        logger.info("Finish.")
    elif args.type == 'master' :
        logger.info("start insert cluster master.")
        dm.update_golden_record("Schema_Discovery_GR")
        logger.info("Golden record updated completed.")
        dm.insert_clusters_master(dm.get_golden_record())        
        logger.info("Insert Golden record completed.")
        # dm.update_clusters_publish_date(dm.get_unified_published_clusters_date())
        dm.insert_dataset_ui()
        logger.info("Update UI completed.")
        logger.info("Finish.")    
    elif args.type == 'metadata' :
        logger.info("start insert metadata dataset.")        
        dm.insert_metadata(dm.get_unified_metadata())
        logger.info("Finish.") 
    elif args.type == 'profiled' :
        logger.info("start insert profiled dataset.")
        dm.insert_profiled(dm.get_unified_metadata_profiled())
        logger.info("Finish.") 
    elif args.type == 'mixed' :
        logger.info("start insert mixed tables.")
        dm.insert_mixed_tables(dm.get_pair_comments_df())
        logger.info("Finish.") 
    elif args.type == 'table_status' :
        logger.info("start insert table status.")
        pm = ProfileManager('legacy', False, process_name)
        dm_mdm = DataManager(ConfigManager("mdm"), process_name)
        dm_mixed = DataManager(ConfigManager("mixed"), process_name)
        dm_legacy = DataManager(ConfigManager("legacy"), process_name)        
        
        mdm_tables = dm_mdm.get_info_tables()
        dm.insert_table_status(mdm_tables)
        logger.info("Mdm tables insert completed.")

        mixed_tables = dm_mixed.get_info_tables()
        dm.insert_table_status(mixed_tables)
        logger.info("Mixed tables insert completed.")

        unify_tables = dm.get_info_tables()        
        dm.insert_table_status(unify_tables)
        logger.info("Unify tables insert completed.")
        
        ######### oracle legacy 테이블 목록 ######### 
        oracle_tables = dm_legacy.get_sample_tables()
        
        ######### hive legacy 테이블 목록 #########         
        tables = dm_legacy.get_all_tables()
        thread_cnt = 16
        hive_tables = []
        all_tables = []
        thread_no = 0
        for t in tables :            
            table = {"THREAD_NO": thread_no, "PID": 0, "DB_TYPE": "HIVE", "DB_NAME": "" , "SOURCE": "LEGACY", "TABLE_NAME": "", "ORIGIN_TABLE_NAME": "", "ROW_CNT": 0, "COL_CNT": 0, "CREATE_DT": datetime.datetime.now().strftime('%Y-%m-%d'), "IS_RELOAD": "1", "COLUMN_NAMES1": "", "COLUMN_NAMES2": "", "LAST_MOD_DT": ""}
            table_names = t.split('.')
            if len(table_names) == 2 and pm.except_table(table_names[1].upper()) == False :                                
                if thread_no > thread_cnt : 
                    thread_no = 0
                table["DB_NAME"] = table_names[0]
                table["TABLE_NAME"] = table_names[1]
                table["ORIGIN_TABLE_NAME"] = '{}.{}'.format(table_names[0].upper(), table_names[1].upper())
                thread_no = thread_no + 1                
                hive_tables.append(table)

        ######### hive legacy 테이블의 컬럼 개수 정보 저장 ######### 
        if len(oracle_tables) > 0 :
            with Manager() as manager:
                threads = []
                for idx in range(0, thread_cnt) :
                    procs = []
                    for proc in hive_tables :
                        if idx == proc["THREAD_NO"] :
                            procs.append(proc)
                    threads.append(procs)

                p_list = []
                m_list = manager.list([0 for x in range(len(threads))])            
                for idx in range(0, len(threads)) :                
                    proc = Process(target=dm_legacy.get_hive_tables, args=(threads[idx], m_list, idx))
                    p_list.append(proc)
                    proc.start()

                for p in p_list:
                    p.join()

                hive_tables.clear()
                for procs in m_list :
                    for tb in procs :
                        hive_tables.append(tb)

        hive_table_names = list(set([''.join(row["TABLE_NAME"]) for row in hive_tables]))
        oracle_table_names = list(set([''.join(row["TABLE_NAME"]) for row in oracle_tables]))
        
        ######### 기존 oracle의 테이블 목록 중 hive에 있는 테이블만 추가 ######### 
        for ora in oracle_tables :
            if pm.except_table(ora["TABLE_NAME"].upper()) == False and ora["TABLE_NAME"] in hive_table_names :
                table = {"THREAD_NO": 0, "PID": 0, "DB_TYPE": ora["DB_TYPE"], "DB_NAME": ora["DB_NAME"] , "SOURCE": ora["SOURCE"], "TABLE_NAME": ora["TABLE_NAME"], "ORIGIN_TABLE_NAME": ora["ORIGIN_TABLE_NAME"], "ROW_CNT": ora["ROW_CNT"], "COL_CNT": ora["COL_CNT"], "CREATE_DT": datetime.datetime.now().strftime('%Y-%m-%d'), "IS_RELOAD": ora["IS_RELOAD"], "COLUMN_NAMES1": ora["COLUMN_NAMES1"],"COLUMN_NAMES2": ora["COLUMN_NAMES2"], "LAST_MOD_DT": ora["LAST_MOD_DT"]}
                if int(table["ROW_CNT"]) <= 100000 or len(table["COLUMN_NAMES1"]) == 0 :
                    table["IS_RELOAD"] = 1
                all_tables.append(table)

        ######### hive의 테이블 목록 중 기존 oracle의 테이블 목록에 없는 테이블만 추가 ######### 
        for hive in hive_tables :
            if hive["TABLE_NAME"] not in oracle_table_names :
                all_tables.append(hive)
        
        ######### oracle과 hive의 테이블 컬럼 수가 변경되었을 경우 다시 샘플링 ######### 
        for tb in all_tables :
            for hive in hive_tables :                
                if tb["TABLE_NAME"] == hive["TABLE_NAME"] and (tb["COL_CNT"] != hive["COL_CNT"] or tb["LAST_MOD_DT"] != hive["LAST_MOD_DT"]) :
                    tb["IS_RELOAD"] = 1

        ######### thread 설정 ######### 
        thread_no = 0
        for tb in all_tables :
            if thread_no > thread_cnt : 
                thread_no = 0
            tb["THREAD_NO"] = thread_no
            thread_no = thread_no + 1

        logger.info("Get statistics tables....")
        ######### 테이블 정보 저장 ######### 
        insert_tables = []        
        with Manager() as manager:
            threads = []
            for idx in range(0, thread_cnt) :
                procs = []
                for proc in all_tables :
                    if idx == proc["THREAD_NO"] :
                        procs.append(proc)
                threads.append(procs)

            p_list = []
            m_list = manager.list([0 for x in range(len(threads))])            
            for idx in range(0, len(threads)) :                
                proc = Process(target=dm_legacy.get_table_cnt, args=(threads[idx], m_list, idx))
                p_list.append(proc)
                proc.start()

            for p in p_list:
                p.join()

            for items in m_list :
                for item in items :
                    insert_tables.append(item)

        ####### oracle에 테이블 정보 저장 ######### 
        logger.info("Insert statistics tables....")
        dm.insert_table_status(insert_tables)
        logger.info("Legacy tables insert completed....")
        logger.info("Finish....") 