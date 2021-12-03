#!/bin/python
import os
import sys
import logging

current_path = os.path.dirname(os.path.realpath(__file__))

sys.path.append(current_path + '/config')
from config_manager import ConfigManager
from pnd_logger import PndLogger

sys.path.append(current_path + '/data')
from data_manager import DataManager
from custom_unify import CustomUnify
from except_manager import ExceptManager

class SchemaCompare :
    def __init__(self, database, logger_nm=None):
        logger_name = "compare" if logger_nm is None else logger_nm
        self.__logger = PndLogger("Schema Compare Class", logger_name)
        self.__source = database
        self.__dm = DataManager(database, "unify", logger_name)
        self.__dm_mdm = DataManager("mdm", "unify", logger_name)
        self.__cm_unify = ConfigManager("unify", logger_name)
        self.__cm_mdm = ConfigManager("mdm", logger_name)
        self.__unify = CustomUnify(self.__cm_unify, logger_name)
        self.__em = ExceptManager(database, logger_nm)
        
        self.is_exist_dataset = False
        self.is_exist()

        self.metadata_dataset = None
        self.all_table_columns = None
        self.metadata_tables = None
        self.database_tables = None
        self.lake_table_info = None
        self.lake_fabs_info = None
        
        self.new_tables = []
        self.deleted_tables = []
        self.updated_tables = []
        
        if self.is_exist_dataset :
            self.__init_dataset_properties()
            self.__new_tables()
            self.__deleted_tables()
            self.__updated_tables()
            self.__logger.info("Compare Tables  All={}  New={}  Update={}  Deleted={}".format(len(self.database_tables), len(self.new_tables), len(self.updated_tables), len(self.deleted_tables)))

    def is_exist(self) :
        dataset_name = "{}_column_metadata".format(self.__source)
        result = self.__unify.versioned.is_exist_dataset(dataset_name)
        if result :
            self.__logger.info("{} is exist.".format(dataset_name))
        else :
            self.__logger.info("{} is not exist.".format(dataset_name))

        self.is_exist_dataset = result

    def is_deleted_column(self, table_column) :
        if self.__source == 'mixed' and "ZTECH" not in table_column :
            return False

        database_columns = [row["TABLE_COLUMN"] for row in self.all_table_columns if row["TABLE_NAME"] not in self.deleted_tables]
        return True if table_column not in database_columns else False

    def __init_dataset_properties(self) :
        try:
            dataset_name = "{}_column_metadata".format(self.__source)
            #  table_column 별 spec 정보 목록
            self.all_table_columns = [row for row in self.__dm.get_all_table_columns()]
            # source의 column_metadata 
            self.metadata_dataset = [row for row in self.__dm.get_stream_dataset_by_name(dataset_name)]
            # source의 metadata table 목록
            self.metadata_tables = list(set([''.join(row["TableName"]) for row in self.metadata_dataset]))
            # database의 table 목록
            self.database_tables = list(set([''.join(row["TABLE_NAME"]) for row in self.__dm.get_all_table()]))
            # lake table 명칭 변환 정보 목록
            self.lake_table_info = self.__dm_mdm.execute_query(self.__cm_mdm.queries["getTableInfo"])
            # lake fab 명칭 변환 정보 목록
            self.lake_fabs_info = list(set([row["FAB_LAKE"] for row in self.lake_table_info]))
            
            self.__logger.info("All Talble Columns = {} | Metadata Dataset Rows/Tables = {}/{} | Database Tables = {}".format(len(self.all_table_columns), len(self.metadata_dataset), len(self.metadata_tables), len(self.database_tables)))
            self.__logger.debug("metadata tables={}".format(self.metadata_tables))
            self.__logger.debug("database tables={}".format(self.database_tables))
        except Exception as e:
            self.__logger.error(e)
        return 

    # 새로 추가된 테이블 목록
    def __new_tables(self) :
        try :
            new_tables = [row for row in self.database_tables if row not in self.metadata_tables and self.__em.except_table(row) == False]
            self.__logger.info("New count={}  tables={}".format(len(new_tables), ",".join(new_tables)))
            self.new_tables = new_tables
        except Exception as e :
            self.__logger.error(e)
        return

    # 삭제된 테이블 목록
    def __deleted_tables(self) :        
        try :
            deleted_tables = list(set([row for row in self.metadata_tables if row not in self.database_tables and self.__em.except_table(row) == False]))        
            self.__logger.debug("Deleted count={}  tables={} ".format(len(deleted_tables), ",".join(deleted_tables)))
            self.deleted_tables = deleted_tables
        except Exception as e :
            self.__logger.error(e)
        return

    # 변경된 테이블 목록
    def __updated_tables(self) :
        metadata_columns = [row["Tamr_Profiling_Seq"] for row in self.metadata_dataset if row["TableName"] not in self.deleted_tables]
        database_tables = list(set([''.join(row) for row in self.database_tables if row not in self.deleted_tables and self.__em.except_table(row) == False]))
        database_columns = [row["TABLE_COLUMN"] for row in self.all_table_columns if row["TABLE_NAME"] in database_tables and row["TABLE_NAME"] not in self.deleted_tables]
        updated_tables = []

        for db in database_columns :
            table = str(db.split("-")[0]).strip()
            if db not in metadata_columns and table not in self.new_tables and self.__em.except_table(table) == False :
                updated_tables.append(str(db.split("-")[0]).strip())
                #self.__logger.debug("[{}]Updated table_column - {}".format(self.__source, db))

        updated_tables = list(set(updated_tables))
        self.__logger.debug("Updated count={}  tables={} ".format(len(updated_tables), ",".join(updated_tables)))
        self.updated_tables = updated_tables
        return

    # unify에서 metadata를 삭제
    def truncate_unified_dataset(self, dataset_name) :        
        try :
            self.__unify.dataset.truncate_dataset(dataset_name)
        except Exception as e:
            self.__logger.error(e)
        return

    def get_convert_system_fab_name(self, table_name) :
        result = { "SYSTEM":None, "FAB":"" }
        system_tamr = None
        
        for info in self.lake_table_info :
            if info["LAKE_TABLE_NM"] == table_name :
                self.__logger.debug("Matched table = {}    system = {}".format(table_name, info["SYSTEM_NM"]))
                result["SYSTEM"] = info["SYSTEM_NM"]
                break
            else:
                self.__logger.debug("Unmatched table = {}    system = {}".format(table_name, info["SYSTEM_NM"]))

        return result

    #def get_convert_system_fab_name(self, table_name) :
    #    result = { "SYSTEM":None, "FAB":None }
        # system_lake = table_name.split("_")[0]
        # fab_lake = None if table_name.split("_")[-1] not in self.lake_fabs_info else table_name.split("_")[-1]
        # system_tamr = None
        # fab_tamr = None

        # if system_lake == "ERP" :
            # for info in self.lake_table_info :
                # if info["SYSTEM_LAKE"] == system_lake and info["FAB_LAKE"] == fab_lake :
                    # self.__logger.debug("Matched table = {}    system = {}    fab = {}".format(table_name, info["SYSTEM_TAMR"], "ALL"))
                    # result["SYSTEM"] = info["SYSTEM_TAMR"]
                    # result["FAB"] = "ALL"
                    # break
                # else:
                    # self.__logger.debug("Matched table = {}    system = {}    fab = {}".format(table_name, "GERP", "ALL"))
                    # result["SYSTEM"] = "GERP"
                    # result["FAB"] = "ALL"
        # else :
            # for info in self.lake_table_info :
                # if info["SYSTEM_LAKE"] == system_lake and info["FAB_LAKE"] == fab_lake :
                    # self.__logger.debug("Matched table = {}    system = {}    fab = {}".format(table_name, info["SYSTEM_TAMR"], info["FAB_TAMR"]))
                    # result["SYSTEM"] = info["SYSTEM_TAMR"]
                    # result["FAB"] = info["FAB_TAMR"]
                    # break
                # else:
                    # self.__logger.debug("Unmatched table = {}    system = {}    fab = {}".format(table_name, info["SYSTEM_TAMR"], info["FAB_TAMR"]))

        # return result

if __name__ == "__main__":
    compare = SchemaCompare("legacy")