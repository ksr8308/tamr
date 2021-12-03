#!/bin/python
import os
import sys
import glob
import yaml
import pprint
import varyaml
import re

current_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(current_path + '/../config')
from pnd_logger import PndLogger
sys.path.append(current_path + '/../data')
from config_manager import ConfigManager
from data_manager import DataManager
from custom_unify import CustomUnify

class ProfileManager:
    def __init__(self, source, is_needLoadData, logger_nm=None):
        self.__logger_name = logger_nm
        self.__logger = PndLogger("Sampling and Profiling Class", logger_nm)
        self.__source = source
        self.__dataset_name = "{}_column_metadata".format(source)

        self.__dm = DataManager(ConfigManager(source), logger_nm)        
        self.__dm_tamr = DataManager(ConfigManager("tamr"), logger_nm)

        self.__metadata_tables = None
        self.__database_tables = None
        self.__metadata_tables_info = None
        self.__database_tables_info = None        
        self.__all_table_columns = None 
        if is_needLoadData :       
            self.__init_dataset_properties()
        return

    def __init_dataset_properties(self) :
        # datasets = None
        # try:            
            # source의 column_metadata
            # datasets = self.__dm.get_stream_dataset_by_name(self.__dataset_name)            
            # if datasets != None and len(datasets) > 0 and datasets[0] != None:                
            # self.__metadata_tables = list(set([''.join(row["TableName"]) for row in self.__dm.get_stream_dataset_by_name(self.__dataset_name)]))
            # self.__metadata_tables_info = self.__dm.get_stream_dataset_by_name(self.__dataset_name)            
        # except Exception:
            # cm = ConfigManager("tamr")
            # dm = DataManager(ConfigManager("tamr"), self.__logger_name)            
            # datasets = dm.execute_query(cm.queries["getTamrMetadata"])        
            # self.__metadata_tables = list(set([''.join(row["TableName"]) for row in self.__dm.get_stream_dataset_by_name(self.__dataset_name)]))
            # self.__metadata_tables_info = self.__dm.get_stream_dataset_by_name(self.__dataset_name)

        cm = ConfigManager("tamr")
        dm = DataManager(ConfigManager("tamr"), self.__logger_name)            
        datasets = None # dm.execute_query(cm.queries["getTamrMetadata"])
        # self.__metadata_tables = list(set([''.join(row["TABLENAME"]) for row in datasets]))
        # self.__metadata_tables_info = datasets
        self.__database_tables_info = self.__dm.get_sample_tables()
        # self.__database_tables = list(set([''.join(row["TABLE_NAME"]) for row in self.__database_tables_info]))            
        
        # self.__logger.info("metadata tables={}".format(self.__metadata_tables))
        # self.__logger.info("database tables={}".format(self.__database_tables))
        # cm = ConfigManager("tamr")
        # dm = DataManager(ConfigManager("tamr"), self.__logger_name)
        # self.__logger.info(cm.queries["getTamrMetadata"])
        # datasets = dm.execute_query(cm.queries["getTamrMetadata"])
        return 

    # sampling 대상 테이블 목록을 가져옴
    def getSampleTables(self):
        # sample_tables = self.__new_tables()
        sample_tables_info = []        
        # for table in self.__database_tables_info :
        #     if table["TABLE_NAME"] in sample_tables :
        #         sample_tables_info.append(table)
        
        for table in self.__database_tables_info :
            sample_tables_info.append(table)
        return sample_tables_info

    def getAllTableColumns(self):
        table_columns = self.__dm.get_table_columns()
        return table_columns
        # self.__all_table_columns = self.__dm.get_all_table_columns()    
        # return self.__all_table_columns

    # 새로 추가된 테이블 목록
    def __new_tables(self) :
        try :               
            metadata_tables = self.__metadata_tables if self.__metadata_tables != None else []
            new_tables = [row for row in self.__database_tables if row not in metadata_tables]
            new_tables = [row for row in new_tables if self.except_table(row) == False]

            for tab in self.__database_tables_info :
                if int(tab["IS_RELOAD"]) == 1 :
                    new_tables.append(tab)

            self.__logger.info("New count={}  tables={}".format(len(new_tables), ",".join(new_tables)))
            return new_tables
        except Exception as e :
            self.__logger.error(e)
        return

    # 삭제된 테이블 목록
    def __deleted_tables(self) :        
        try :
            deleted_tables = list(set([row for row in self.__metadata_tables if row not in self.__database_tables or self.except_table(row) == False]))        
            self.__logger.debug("Deleted count={} | tables={} ".format(len(deleted_tables), ",".join(deleted_tables)))
            return deleted_tables
        except Exception as e :
            self.__logger.error(e)
        return

    # sampling 제외 테이블 
    def except_table(self, table) :        
        table = ''.join(table)
        exp_tables = ['NAS_NAND_WTM_CJWT','NAND_WTM','LFDC_EQP_TRACE_VALUE_TRX_PFD_M14_NA','DCP_DCP_DCOLDATARSLT_INF_M14','TAS_Q_INL_SRC_WF_DATA_IC','TAS_Q_INL_REP_WF_SITE_DATA_IC','YES_T_YES_DEFECT_M14','TAS_Q_PKT_BIN_MAP_NEW_IC','TAS_Q_PRB_SDA_MAP_IC','SSD_WORKLOAD_DATACENTER_DATA_WIN','LFDC_EQP_TRACE_TRX_PFD_M14','TAS_Q_PKT_CATE_WF_DATA_IC','TAS_Q_INL_REP_WF_DATA_IC','SSD_WORKLOAD_DATACENTER_DATA_LINUX','APC_MI_SPEC_DATA_RMS_M14','TAS_Q_PRB_EDGE_DATA_IC','TAS_ITG_WFTOTLEGEND_INF_IC','TAS_U_PRMT_BY_OPER_IC','TAS_Q_PKT_ITG_CATE_WF_DATA_IC','TAS_Q_DFT_DFT_WF_IC','TAS_Q_PKT_ITG_BIN_WF_DATA_IC','CHP_FLS_RAW_TDBI_IC','FDC_EQP_SUM_VALUE_TRX_PP_WLP','PKM_PKM_ERLOG_I_2','TAS_Q_INL_SRC_LOT_DATA_IC']
        # except_list = ['$', '_H_','_H$','HIS','HIS_','_HIS_','_HIS$','HIST_','_HIST_','_HIST$','HST','_HST','HST_','_HST_','HISTORY','HISTORY_','_HISTORY_','_HISTORY$','SUMMARY','SUMMARY_','_SUMMARY','_SUMMARY_','TRACE_DATA','_TRACE_DATA','TRACE_DATA_','_TRACE_DATA_','T_WT_MAP_V2']

        if self.__source == "mdm" :
            if table in ["TGF_MATL_M","TGF_MATLTECHATTRNM_I"] :
                self.__logger.info("Except Table is {}.".format(table))
                return True

        if self.__source == "legacy" :
            if table in exp_tables :
                return True
            if "$" in table \
                or re.search("_H_|_H$|HIS|HIS_|_HIS_|_HIS$|HIST_|_HIST_|_HIST$|HST|_HST|HST_|_HST_|HISTORY|HISTORY_|_HISTORY_|_HISTORY$", table) \
                or re.search("SUMMARY|SUMMARY_|_SUMMARY|_SUMMARY_|TRACE_DATA|_TRACE_DATA|TRACE_DATA_|_TRACE_DATA_|T_WT_MAP_V2|TEMP|TEST|_TMP|_TMP_", table) \
                or re.search("TARCE|TRACE_|_TRACE|_TRACE_|T_WT_MAP_V2|TEMP|TEST|_TMP|_TMP_", table) \
                or re.search("TEMP|TEMP_|_TEMP|_TEMP_|TMP|TMP_|_TMP|_TMP_", table) \
                or re.search("TEST|TEST_|_TEST|_TEST_", table) \
                or re.search("WTM|WTM_|_WTM|_WTM_", table) \
                or re.search("DATACENTER|DATACENTER_|_DATACENTER|_DATACENTER_", table) \
                or re.search("T_WT_MAP_V2|_VALUE_TRX_", table) \
                or re.search("\D(\d{6,8})\D", table) or table.startswith("SA_") :
                # or re.search("_H_|_H$|HIS|HIS_|_HIS_|_HIS$|HIST_|_HIST_|_HIST$|HST|_HST|HST_|_HST_|HISTORY|HISTORY_|_HISTORY_|_HISTORY$|SUMMARY|SUMMARY_|_SUMMARY|_SUMMARY_|TRACE_DATA|_TRACE_DATA|TRACE_DATA_|_TRACE_DATA_|T_WT_MAP_V2|TEMP|TEST|_TMP|_TMP_", table) \                
                self.__logger.info("Except Table is {}.".format(table))
                return True

        return False
    # =================================================================================================================================
    # profiling 제외 컬럼
    def except_column(self, table_name, column_name, value) :
        except_value = ''.join(value)

        if re.search("blob|clob|date|timestamp", except_value) :
            self.__logger.info("[Source={}] Exception by '{}'.[table='{}' column='{}']".format(self.__source, value, table_name, column_name))
            return True

        if re.search("NOTICE|REACTION_PLAN|CONTROL_PROPERTY|CELL_POS_Y_LST|LAKE_LOAD_TM|MANDT|AEDAT_GL|AEDAT_LO|AENAM_CM|AEZET_GL|AEZET_LO|ERDAT_GL|ERDAT_LO|ERNAM_CM|ERZET_GL|ERZET_LO|COMMENT", except_value) :
            self.__logger.info("[Source={}] Exception by '{}'.[table='{}' column='{}']".format(self.__source, value, table_name, column_name))
            return True
        
        if re.search("^URL$|^사번$|^비고$|^기간$|^년도$|^년월$|^년주$|^년주월$|^년중일$|^년중주차$|^변경일시$|^분$|^분기$|^생성일시$|^시$|^시간$|^시분$|^시분초$|^일$|^일수$|^일시$|^일시V$|^일자$|^주당일수$|^시간V$|^월$|^년중주차V$|^반기$|^일시TS$|^HH$|^일시명$|^타임스탬프$|^주$|RAWID|사용자ID|^내용$|^순서$|^여부$|^건수$|건수V|^수량$|^율$|^율V$|^가동율$|^환율$|좌표|^경로$|^횟수$|^단가$|^원가$|^Z좌표$", except_value) :    
            self.__logger.info("[Source={}] Exception by '{}'.[table='{}' column='{}']".format(self.__source, value, table_name, column_name))
            return True

        if self.__source == "mdm" and except_value.startswith("X") or except_value.startswith("HEAD") :
            self.__logger.info("[Source={}] Exception by '{}'.[table='{}' column='{}']".format(self.__source, value, table_name, column_name))
            return True

        if self.__source == "mixed" and  re.search("MODIFY|QUICK PROGRESS|REGULARITY IRREGULARITY MATERIAL LIST VALUE", except_value.upper()) :
            self.__logger.info("[Source={}] Exception by '{}'.[table='{}' column='{}']".format(self.__source, value, table_name, column_name))
            return True

        return False

    # Table명을 기준으로 system / fab 명을 설정
    def get_convert_system_fab_name(self, datas, table_name) :
        result = { "SYSTEM":"", "FAB":"" }
        system_tamr = None

        for data in datas :            
            if data["LAKE_TABLE_NM"] == table_name :
                self.__logger.debug("Matched table = {}    system = {}".format(table_name, data["SYSTEM_NM"]))
                result["SYSTEM"] = data["SYSTEM_NM"]
                break
            else:
                self.__logger.debug("Unmatched table = {}    system = {}".format(table_name, data["SYSTEM_NM"]))

        return result
