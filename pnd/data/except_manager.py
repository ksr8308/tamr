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

class ExceptManager:
    def __init__(self, source_name, logger_name=None) :
        self.__logger = PndLogger("Exception Class", logger_name)
        self.__source = source_name
        self.sample_tables = None
        self.except_tables = None
        
    def except_table(self, table) :        
        table = ''.join(table)

        if self.__source == "mdm" :
            if table in ["TGF_MATL_M","TGF_MATLTECHATTRNM_I"] :
                return True
        elif self.__source == "legacy" :
            if "$" in table or re.search("_H_|_H$|_HIST$|HIST_|_HIST_|_HIS$|HIS_|_HIS_|_HISTORY$|HISTORY_|_HISTORY_", table) or re.search("\D(\d{6,8})\D", table) or table.startswith("SA_") :
                return True
        return False

    def remove_except_tables(self, tables) :
        self.sample_tables = tables.copy()
        self.except_tables = []
        
        for table in tables :     
            if self.except_table(table) :                
                self.except_tables.append(table)                
                self.sample_tables.remove(table)

        self.__logger.info("[Source={}] Sampling  table count={} tables={}".format(self.__source, len(self.sample_tables), ",".join(self.sample_tables)))
        self.__logger.info("[Source={}] Exception table count={} tables={}".format(self.__source, len(self.except_tables), ",".join(self.except_tables)))
        return self.sample_tables

    def except_column(self, table_name, column_name, value) :
        except_value = ''.join(value)

        if re.search("blob|clob|date|timestamp", except_value) :
            self.__logger.info("[Source={}] Exception by '{}'.[table='{}' column='{}']".format(self.__source, value, table_name, column_name))
            return True

        if re.search("NOTICE|REACTION_PLAN|CONTROL_PROPERTY|CELL_POS_Y_LST|LAKE_LOAD_TM|MANDT|AEDAT_GL|AEDAT_LO|AENAM_CM|AEZET_GL|AEZET_LO|ERDAT_GL|ERDAT_LO|ERNAM_CM|ERZET_GL|ERZET_LO", except_value) :
            self.__logger.info("[Source={}] Exception by '{}'.[table='{}' column='{}']".format(self.__source, value, table_name, column_name))
            return True
        
        if re.search("^URL$|^??????$|^??????$|^??????$|^??????$|^??????$|^??????$|^?????????$|^?????????$|^????????????$|^????????????$|^???$|^??????$|^????????????$|^???$|^??????$|^??????$|^?????????$|^???$|^??????$|^??????$|^??????V$|^??????$|^????????????$|^??????V$|^???$|^????????????V$|^??????$|^??????TS$|^HH$|^?????????$|^???????????????$|^???$|RAWID|?????????ID|^??????$|^??????$|^??????$|^??????$|??????V|^??????$|^???$|^???V$|^?????????$|^??????$|??????|^??????$|^??????$|^??????$|^??????$|^Z??????$", except_value) :    
            self.__logger.info("[Source={}] Exception by '{}'.[table='{}' column='{}']".format(self.__source, value, table_name, column_name))
            return True

        if self.__source == "mdm" and except_value.startswith("X") or except_value.startswith("HEAD") :
            self.__logger.info("[Source={}] Exception by '{}'.[table='{}' column='{}']".format(self.__source, value, table_name, column_name))
            return True

        if self.__source == "mixed" and  re.search("MODIFY|QUICK PROGRESS|REGULARITY IRREGULARITY MATERIAL LIST VALUE", except_value.upper()) :
            self.__logger.info("[Source={}] Exception by '{}'.[table='{}' column='{}']".format(self.__source, value, table_name, column_name))
            return True

        return False
