import os
import sys
import logging
import pandas as pd
import json
import datetime
import time
import math
import multiprocessing

current_path = os.path.dirname(os.path.realpath(__file__))
from oracle import Oracle
from hive2 import Hive2
from custom_unify import CustomUnify

sys.path.append(current_path + '/../config')
from config_manager import ConfigManager
from pnd_logger import PndLogger

os.putenv('NLS_LANG', 'KOREAN_KOREA.AL32UTF8')

class DataManager:
    def __init__(self, config_manager, logger_name=None) :
        self.__logger_name = logger_name
        self.__logger = PndLogger("Data Manager Class", logger_name)
        self.__cm = config_manager
        self.__unify = CustomUnify(ConfigManager('unify'), logger_name)
        self.__database = None

    def connect(self) :
        try :            
            if self.__cm.db_type == 'hive' :
                self.__database = Hive2(self.__cm, self.__logger_name)

            if self.__cm.db_type == 'oracle' :
                self.__database = Oracle(self.__cm, self.__logger_name)

            self.__database.connect()            
        except Exception as e :
            self.__logger.error("Failed to connect database. - {}".format(e))

    def disconnect(self) :
        if self.__database != None :
            self.__database.disconnect()
    
    # query를 실행
    def execute_query(self, query) :
        try:
            self.connect()
            data_list = self.__database.execute(query)
            self.disconnect()
            return data_list
        except Exception as e:
            self.__logger.error("Failed to execute query. - {}".format(query))
    
    def execute_query_with_params(self, query, bindvars) :
        try:            
            self.connect()
            self.__database.execute(query, bindvars, True)                        
            self.disconnect()
            return True
        except Exception as e:
            self.__logger.error("Failed to execute query. - {}".format(e))

    # hive의 모든 database
    def get_all_databases(self) :        
        try:            
            statement = self.__cm.queries["getAllDatabases"]                        
            data_list = self.execute_query(statement)
            
            for item in data_list :
                if "_CSV" in item.upper() :
                    data_list.remove(item)           

            data_list.remove('default')
            data_list.remove('temp')
            data_list.remove('test_db')
            data_list.remove('user_info')
            data_list.remove('files')
            data_list.remove('happelin')
            # data_list = ["cim"]
            return data_list
        except Exception as e:
            self.__logger.error(e)        

    # database의 모든 table-column 형식 목록 데이터.
    def get_all_tables(self) :
        try:   
            data_list = []
            if self.__cm.db_type == 'hive' :
                databases = self.get_all_databases()
                for db in databases :
                    statement = self.__cm.queries["getAllTables"].format(db)
                    tables = self.execute_query(statement)
                    for t in tables :
                        data_list.append("{}.{}".format(db.upper(), t.upper()))
            else :
                statement = self.__cm.queries["getAllTables"]
                tables = self.execute_query(statement)
                for t in tables :
                    data_list.append(t["TABLE_NAME"].upper())
            return data_list
        except Exception as e:
            self.__logger.error("Failed to get row. - {}".format(e))        

    def get_all_table_columns(self) :
        try:
            data_list = []
            tables = self.get_all_tables()
            for table in tables :
                table_columns = {"TABLE_NAME": table, "COLUMN_NAMES": []}                
                statement = self.__cm.queries["getAllColumns"].format(table)                
                cols = self.execute_query(statement)                
                for col in cols :
                    if type(col) == dict :
                        if table == col["TABLE_NAME"] :
                            table_columns["COLUMN_NAMES"].append(col["COLUMN_NAME"].upper())
                    else :
                        table_columns["COLUMN_NAMES"].append(col.upper())
                data_list.append(table_columns)                
            return data_list
        except Exception as e:
            self.__logger.error(e)

    # database에 적재되어 있는 metadata profiled 데이터의 테이블 갯수, row, column 갯수 데이터.
    def get_info_tables(self) :
        try:
            data_list = []
            statement = self.__cm.queries["getInfoTables"].format(self.__cm.cred_src)            
            if self.__cm.db_type == 'hive' :                
                table_cols = self.get_all_table_columns()
                for t in table_cols :
                    temp = t["TABLE_NAME"].split('.')
                    table = {'DB_TYPE': 'HIVE', 'DB_NAME': '', 'SOURCE': self.__cm.cred_src.upper(), 'TABLE_NAME': '', 'ORIGIN_TABLE_NAME': '', 'ROW_CNT': 0, 'COL_CNT': 0, 'CREATE_DT': datetime.datetime.now().strftime('%Y-%m-%d'), 'IS_RELOAD': 0, 'COLUMN_NAMES1': '', 'COLUMN_NAMES2': ''}

                    if len(temp) == 2 :
                        table["DB_NAME"] = t["TABLE_NAME"].split('.')[0]
                        table["TABLE_NAME"] = t["TABLE_NAME"].split('.')[1]

                    table["ORIGIN_TABLE_NAME"] = t["TABLE_NAME"]
                    if len(','.join(t["COLUMN_NAMES"])) < 4000 :
                        table["COLUMN_NAMES1"] = ','.join(t["COLUMN_NAMES"]).upper()
                        table["COLUMN_NAMES2"] = ''
                    else :
                        n = len(t["COLUMN_NAMES"])
                        half = int(n/2) # py3                 
                        table["COLUMN_NAMES1"] = ','.join(t["COLUMN_NAMES"][:half]).upper()
                        table["COLUMN_NAMES2"] = ','.join(t["COLUMN_NAMES"][n-half:]).upper()
                    data_list.append(table)                
            else :
                tables = self.execute_query(statement)                
                cols = self.get_all_table_columns()                
                for t in tables :
                    table = {'DB_TYPE': t['DB_TYPE'], 'DB_NAME': t['DB_NAME'], 'SOURCE': t['SOURCE'], 'TABLE_NAME': t['TABLE_NAME'], 'ORIGIN_TABLE_NAME': '{}.{}'.format(t['DB_NAME'], t['TABLE_NAME']), 'ROW_CNT': t['ROW_CNT'], 'COL_CNT': t['COL_CNT'], 'CREATE_DT': datetime.datetime.now().strftime('%Y-%m-%d'), 'IS_RELOAD': 0, 'COLUMN_NAMES1': '', 'COLUMN_NAMES2': ''}
                    for col in cols :
                        if t["TABLE_NAME"] == col["TABLE_NAME"] :
                            if len(','.join(col["COLUMN_NAMES"])) < 4000 :
                                table["COLUMN_NAMES1"] = ','.join(col["COLUMN_NAMES"])
                                table["COLUMN_NAMES2"] = ''
                            else :
                                n = len(col["COLUMN_NAMES"])
                                half = int(n/2) # py3
                                table["COLUMN_NAMES1"] = ','.join(col["COLUMN_NAMES"][:half])
                                table["COLUMN_NAMES2"] = ','.join(col["COLUMN_NAMES"][n-half:])
                    data_list.append(table)
            return data_list
        except Exception as e:
            self.__logger.error(e)

    def get_hive_tables(self, tables, results, idx) :        
        result_tables = []
        failed_tables = []
        total = len(tables)
        successed = 0
        failed = 0        
        for table in tables :
            try:                
                column_query = self.__cm.queries["getAllColumns"]
                table["COL_CNT"] = len(self.execute_query(column_query.format(table["ORIGIN_TABLE_NAME"])))
                result_tables.append(table)
                successed = successed + 1
                self.__logger.debug("NO:[{:>2}]   Total:({:>5}/{:<5})   Failed:({:>3})   TABLE: {}".format(table["THREAD_NO"], successed, total, failed, table["TABLE_NAME"]))
            except Exception :
                failed = failed + 1
                failed_tables.append(table["TABLE_NAME"])
        results[idx] = result_tables        
        self.__logger.debug("NO:[{:>2}] ({}/{}/{}) FAILED TABLES={}".format(result_tables[0]["THREAD_NO"], successed, total, failed, failed_tables))        

    def get_table_cnt(self, tables, results, idx) :        
        result_tables = []
        failed_tables = []
        total = len(tables)
        successed = 0
        failed = 0        
        for table in tables :
            try:
                row_query = self.__cm.queries["getTableRowCnt"]
                column_query = self.__cm.queries["getAllColumns"]
                table["PID"] = os.getpid()
                if int(table["ROW_CNT"]) == 0 or int(table["IS_RELOAD"]) == 1 :
                    cols = self.execute_query(column_query.format(table["ORIGIN_TABLE_NAME"]))
                    table["ROW_CNT"] = int(self.execute_query(row_query.format(table["ORIGIN_TABLE_NAME"]))[0])
                    if len(','.join(cols)) < 4000 :
                        table["COLUMN_NAMES1"] = ','.join(cols).upper()
                        table["COLUMN_NAMES2"] = ''
                    else :
                        n = len(cols)
                        half = int(n/2) # py3                 
                        table["COLUMN_NAMES1"] = ','.join(cols[:half])
                        table["COLUMN_NAMES2"] = ','.join(cols[n-half:])
                    table["COL_CNT"] = len(cols)
                    table["IS_RELOAD"] = 0
                
                if table["ROW_CNT"] > 0 :
                    result_tables.append(table)
                    successed = successed + 1
                    self.__logger.info("NO:[{:>2}]   Total:({:>5}/{:<5})   Failed:({:>3})   TABLE: {}".format(table["THREAD_NO"], successed, total, failed, table["TABLE_NAME"]))
            except Exception as e :
                self.__logger.error(e)
                failed = failed + 1
                failed_tables.append(table["TABLE_NAME"])            
        results[idx] = result_tables        
        if len(result_tables) > 0 :
            self.__logger.info("NO:[{:>2}] ({}/{}/{}) FAILED TABLES={}".format(result_tables[0]["THREAD_NO"], successed, total, failed, failed_tables))
        else :
            self.__logger.info("No Tables Completed.")

    def get_sample_tables(self) :
        try:            
            statement = self.__cm.queries["getSampleTables"].format(source = self.__cm.cred_src.upper())
            print(statement)
            
            if self.__cm.db_type == 'hive' :
                cm = ConfigManager("tamr")
                database = Oracle(cm)
                database.connect()
                data_list = database.execute(statement)
                database.disconnect()
            else :
                data_list = self.execute_query(statement)
            self.__logger.info("Get table status SUCCESSED.")
            return data_list
        except Exception as e:
            self.__logger.error("Failed to table status. - {}".format(e))        

    def get_table_columns(self) :
        try:
            data_list = []
            if self.__cm.db_type == 'hive' :
                cm = ConfigManager("tamr")
                statement = cm.queries["getTableColumns"].format(source = self.__cm.cred_src.upper())
                database = Oracle(cm)
                database.connect()
                tables = database.execute(statement)
                database.disconnect()                
                for t in tables :
                    cols = "{},{}".format(t["COLUMN_NAMES1"], t["COLUMN_NAMES2"]).split(",")                    
                    for col in cols :
                        table = {"TABLE_NAME" : t["TABLE_NAME"], "COLUMN_NAME": col, "TABLE_COLUMN": "{}-{}".format(t["TABLE_NAME"], col),  "SYS_GBN_CD": "", "MST_TYP_ENG": "", "TABLE_KO_NM": "", "ATTR_EN_NM": "", "COL_KO_NM": "", "COL_DESC": ""}
                        data_list.append(table)
            else :
                statement = self.__cm.queries["getTableColumns"]
                tables = self.execute_query(statement)                
                data_list.extend(tables)

            return data_list
        except Exception as e:
            self.__logger.error(e)        

    def get_system_mapping_info(self) :
        try:        
            cm = ConfigManager("mdm")
            database = Oracle(cm)
            database.connect()
            statement = cm.queries["getSystemMappingInfo"]
            data_list = database.execute(statement)            
            self.__logger.info("Get table mapping infomation SUCCESSED.")
            return data_list
        except Exception as e:
            self.__logger.error("Failed to table mapping infomation. - {}".format(e))
        finally:
            database.disconnect()
        return

    def get_system_from_table(self) :
        try:        
            statement = self.__cm.queries["getSystemFromTable"]
            data_list = self.execute_query(statement)
            self.__logger.info("Get table mapping infomation SUCCESSED.")
            return data_list
        except Exception as e:
            self.__logger.error("Failed to table mapping infomation. - {}".format(e))

    # MATL 테이블의 matl_grp_det_gbn_cd, tech_attr_gbn_cd의 unique 코드 목록.
    def get_matrial_unique_codes(self) :
        try:
            statement = self.__cm.queries["getMatrialCodes"]
            self.__database.connect()
            data_list = self.__database.execute(statement, None)
            self.__database.disconnect()
            self.__logger.info("Get matrial unique codes SUCCESSED.")
            return data_list
        except Exception as e:
            self.__logger.error("Failed to get row. - {}".format(e))
        return

    # MATL 테이블의 column 속성 목록.
    def get_matrial_attr_columns(self) :
        try:
            statement = self.__cm.queries["getAllColumns"]
            self.__database.connect()
            data_list = self.__database.execute(statement, None)
            self.__database.disconnect()
            self.__logger.info("Get matrial attribute columns SUCCESSED.")
            return data_list
        except Exception as e:
            self.__logger.error("Failed to get row. - {}".format(e))
        return

    # MATL 테이블을 코드값 기준으로 분리한 데이터 테이블 생성.
    def create_matrial_seperated_table(self, bindvars) :
        try:
            statement = self.__cm.queries["createMixedTempTable"]
            self.__database.connect()
            data_list = self.__database.execute(statement, bindvars)
            self.__database.disconnect()            
            return data_list
        except Exception as e:
            self.__logger.error("Failed to create table. - {}".format(e))
        return

    # data 적재 부분
    # 최종 결과 dataset을 누적 적재
    def insert_clusters_schema_hist(self, bindvars) :
        try:
            self.__database.connect()            
            version = self.__database.execute(self.__cm.queries["getMaxVersionClusterSchemaHist"], None)
            self.__database.execute(self.__cm.queries["delete_mdm_clusters_schema_hist"], None)
            self.__logger.info("Deleted table MDM_CLUSTERS_SCHEMA_HIST.")
            statement = self.__cm.queries["insert_mdm_clusters_schema_hist"]
            cols = ['entityid','originsourceid','originentityid','business_type','distinct_value_count','column_name','max_value','column_name_tokenized','empty_value_count','min_value','table_name','column_name_tokenized_std','fab','record_count','system_name','clustername','persistentid','col_ko_nm','ver','create_dt','sourceid','attr_en_nm','column_type','col_desc','top_100_values','pattern','non_numeric_top_values','numeric_top_values','top_n_values']
            bindvar = []
            for var in bindvars:
                row = {**var, **{"ver": version[0]["VERSION"], "create_dt": datetime.datetime.now().strftime('%Y-%m-%d') }}
                dic = dict()
                for key, value in row.items() :
                    if key.lower() in cols:
                        key = str(key).replace("-", "_").lower()
                        if value == [None] or value == None :                            
                            dic[key] = None
                        else :
                            dic[key] = ''.join(value) if type(value) == list else str(value)
                bindvar.append(dic)

            bindvar = list({var["entityid"]: var for var in bindvar}.values())
            bindvar = list({var["originentityid"]: var for var in bindvar}.values())
            self.__logger.info("Start inserting the cluster into MDM_CLUSTERS_SCHEMA_HIST({} rows).".format(len(bindvar)))
            self.__database.execute(statement, bindvar, True)
            self.__database.disconnect()
            self.__logger.info("Clusters Insert to datbase SUCCESSED. [table = MDM_CLUSTERS_SCHEMA_HIST | version = {}]".format(version[0]["VERSION"]))
        except Exception as e:
            self.__logger.error("Failed to insert row. - {}".format(e))
        return

    # 최종 결과 dataset
    def insert_clusters_schema(self, bindvars) :
        try:
            self.__database.connect()
            self.__database.execute(self.__cm.queries["delete_mdm_clusters_schema"], None)
            self.__logger.info("Deleted table MDM_CLUSTERS_SCHEMA.")
            statement = self.__cm.queries["insert_mdm_clusters_schema"]
            cols = ["entityid","originsourceid","originentityid","business_type","distinct_value_count","column_name","max_value","column_name_tokenized","empty_value_count","min_value","table_name","column_name_tokenized_std","fab","record_count","system_name","clustername","persistentid","create_dt","column_type","top_100_values","locked"]
            bindvar = []
            for var in bindvars:    
                row = {**var, "create_dt": datetime.datetime.now().strftime('%Y-%m-%d') }
                dic = dict()
                for key, value in row.items() :                    
                    if key.lower() in cols:
                        key = str(key).replace("-", "_").lower()
                        if value == [None] or value == None :
                            dic[key] = None
                        else :
                            dic[key] = ''.join(value) if type(value) == list else str(value)
                            dic[key] = dic[key][:1999] if dic[key] != None and len(dic[key]) > 4000 else dic[key]                
                bindvar.append(dic)
            
            #bindvar = list({var["entityid"]: var for var in bindvar}.values())
            self.__logger.info("Start inserting the cluster into MDM_CLUSTERS_SCHEMA({} rows).".format(len(bindvar)))            
            self.__database.execute(statement, bindvar, True)
            self.__database.disconnect()
            self.__logger.info("Clusters Insert to datbase SUCCESSED. [table = MDM_CLUSTERS_SCHEMA]")
        except Exception as e:
            self.__logger.error("Failed to insert row. - {}".format(e))

        return

    # cluster별 value 정보 dataset
    def insert_top_values(self, bindvars) :
        try:
            self.__database.connect()
            self.__database.execute(self.__cm.queries["delete_top_values"], None)
            self.__logger.info("Deleted table MDM_TOP_VALUES_CNT.")
            statement = self.__cm.queries["insert_top_values"]
            bindvar = []
            cols = ["persistent_id", "table_name", "column_name", "value_name", "value_count", "record_count", "distinct_count", "value_ratio", "create_dt"]
            
            for var in bindvars:
                row = {**var, **{"value_ratio": 0, "create_dt": datetime.datetime.now().strftime('%Y-%m-%d') }}
                row["value_ratio"] = 0 if int(row["record_count"]) == 0 else int(row["value_count"]) / int(row["record_count"])
                bindvar.append(row)
            
            #bindvar = list({var["persistent_id"] and var["table_name"] and var["column_name"] and var["value_name"]: var for var in bindvar}.values())
            self.__logger.info("Start inserting the top and value count into MDM_TOP_VALUES_CNT({} rows).".format(len(bindvar)))
            self.__database.execute(statement, bindvar, True)            
            self.__database.disconnect()
            self.__logger.info("Top and Values Insert to datbase SUCCESSED. [table = MDM_TOP_VALUES_CNT]")
        except Exception as e:
            self.__logger.error("Failed to insert row. - {}".format(e))
        return

    # cluster alias와 publish date dataset 
    def insert_clusters_master(self, bindvars) :
        try:
            self.__database.connect()
            self.__database.execute(self.__cm.queries["delete_mdm_clusters_master"], None)
            self.__logger.info("Deleted table MDM_CLUSTERS_MASTER.")
            
            statement = self.__cm.queries["insert_mdm_clusters_master"]
            cols = ['persistentid','cluster_name','cluster_full_name','cluster_alias','create_dt']
            bindvar = []
            for var in bindvars:
                row = {**var, "create_dt": datetime.datetime.now().strftime('%Y-%m-%d') }                
                dic = dict()
                for key, value in row.items() :
                    if key.lower() in cols:                        
                        key = str(key).replace("-", "_").lower()
                        if value == [None] :
                            dic[key] = None
                        else :
                            dic[key] = ''.join(value) if type(value) == list else str(value)                            
                bindvar.append(dic)
            
            self.__logger.info("Start inserting the clusters into MDM_CLUSTERS_MASTER({} rows).".format(len(bindvar)))
            self.__database.execute(statement, bindvar, True)
            self.__database.disconnect()
            self.__logger.info("Clusters Insert to datbase SUCCESSED. [table = MDM_CLUSTERS_MASTER]")            
        except Exception as e:
            self.__logger.error("Failed to insert row. - {}".format(e))
        return

    # cluster master의 publish 날짜를 update.
    def update_clusters_publish_date(self, bindvars) :
        try:
            statement = self.__cm.queries["update_tamr_cluster_master_publish_date"]
            bindvar = []
            for var in bindvars:
                row = {**var}                
                bindvar.append(dic)

            self.__logger.info("Start updating the publish date to MDM_CLUSTERS_MASTER({} rows).".format(len(bindvars)))
            self.__database.connect()
            if len(bindvars) > 0 :
                self.__database.execute(statement, bindvars, True)
            self.__database.disconnect()
            self.__logger.info("Clusters Update to datbase SUCCESSED. [table = MDM_CLUSTERS_MASTER]")
        except Exception as e:
            self.__logger.error("Failed to update row. - {}".format(e))
        return

    # ingest 완료된 dataset.
    def insert_metadata(self, bindvars) :
        try:
            self.__database.connect()
            self.__database.execute(self.__cm.queries["delete_tamr_metadata"], None)
            self.__logger.info("Deleted table TAMR_METADATA.")
            
            statement = self.__cm.queries["insert_tamr_metadata"]
            cols = ['source','top100frequencies','columnname','tablename','top100values','columntype','tamr_profiling_seq','emptyvaluecount','minvalue','meanvalue','maxvalue','stddevvalue','recordcount','distinctvaluecount','tamrseq','create_dt']
            bindvar = []
            for var in bindvars:
                row = {**var, **{"create_dt": datetime.datetime.now().strftime('%Y-%m-%d') }}
                dic = dict()
                for key, value in row.items() :
                    if key.lower() in cols:
                        key = str(key).replace("-", "_").lower()
                        dic[key] = ''.join(value) if type(value) == list else str(value)                        
                bindvar.append(dic)

            bindvar = list({var["tamr_profiling_seq"]: var for var in bindvar}.values())
            self.__logger.info("Start inserting the clusters into TAMR_METADATA({} rows).".format(len(bindvar)))
            self.__database.execute(statement, bindvar)
            self.__database.disconnect()
            self.__logger.info("Clusters Insert to datbase SUCCESSED. [table = TAMR_METADATA]")
        except Exception as e:
            self.__logger.error("Failed to insert row. - {}".format(e))
        return

    # profiled 완료된 dataset.
    def insert_profiled(self, bindvars) :
        try:
            self.__database.connect()
            self.__database.execute(self.__cm.queries["delete_tamr_profiled"], None)
            self.__logger.info("Deleted table TAMR_PROFILED.")

            statement = self.__cm.queries["insert_tamr_profiled"]
            cols = ['source', 'columnname','tablename','columntype','tamr_profiling_seq','emptyvaluecount','minvalue','meanvalue','maxvalue','stddevvalue','recordcount','distinctvaluecount','tamrseq','column_name_tokenized','business_type','length','keys','sys_gbn_cd','mst_typ_eng','col_desc','col_ko_nm','attr_en_nm','create_dt', 'top_n_values']
            bindvar = []
            for var in bindvars:
                row = {**var, **{"create_dt": datetime.datetime.now().strftime('%Y-%m-%d') }}
                dic = dict()
                for key, value in row.items() :
                    if key.lower() in cols:
                        key = str(key).replace("-", "_").lower()
                        dic[key] = ''.join(value) if type(value) == list else str(value)                        
                bindvar.append(dic)

            bindvar = list({var["tamr_profiling_seq"]: var for var in bindvar}.values())
            self.__logger.info("Start inserting the clusters into TAMR_PROFILED({} rows).".format(len(bindvar)))
            self.__database.execute(statement, bindvar)
            self.__database.disconnect()
            self.__logger.info("Clusters Insert to datbase SUCCESSED. [table = TAMR_PROFILED]")
        except Exception as e:
            self.__logger.error("Failed to insert row. - {}".format(e))
        return

    # comment에 명시된 mixed table 및 column.
    def insert_mixed_tables(self, bindvars) : 
        try:            
            self.__database.connect()            
            statement = self.__cm.queries["getMixedTables"]
            mixed_tables = [row["TABLE_COLUMN"] for row in self.__database.execute(statement, None)]
            statement = self.__cm.queries["insert_tamr_mixed_tables"]            
            bindvar = []
            rows = []

            for var in bindvars:                
                if 'MIX:AB' in str(var['message']).upper() or 'MIX:BA' in str(var['message']).upper() :
                    rows.append({'table_name':str(var['originTransactionId1']).split('-')[0], 'column_name':str(var['originTransactionId1']).split('-')[1], 'create_dt': datetime.datetime.now().strftime('%Y-%m-%d')})
                    rows.append({'table_name':str(var['originTransactionId2']).split('-')[0], 'column_name':str(var['originTransactionId2']).split('-')[1], 'create_dt': datetime.datetime.now().strftime('%Y-%m-%d')})
                elif 'MIX:A' in str(var['message']).upper() :
                    rows.append({'table_name':str(var['originTransactionId1']).split('-')[0], 'column_name':str(var['originTransactionId1']).split('-')[1], 'create_dt': datetime.datetime.now().strftime('%Y-%m-%d')})
                elif 'MIX:B' in str(var['message']).upper() :
                    rows.append({'table_name':str(var['originTransactionId2']).split('-')[0], 'column_name':str(var['originTransactionId2']).split('-')[1], 'create_dt': datetime.datetime.now().strftime('%Y-%m-%d')})

            dic = dict()
            for row in rows :                
                if "{}-{}".format(str(row["table_name"]), str(row["column_name"])) not in mixed_tables :                        
                    for key, value in row.items() :
                        key = str(key).replace("-", "_")
                        dic[key] = ''.join(value) if type(value) == list else str(value)                        
                        bindvar.append(dic)
            
            self.__logger.info("Start inserting the mixed table TAMR_MIXED_TABLES({} rows).".format(len(bindvar)))
            self.__database.execute(statement, bindvar)
            self.__database.disconnect()
            self.__logger.info("Mixed tables Insert to datbase SUCCESSED. [table = TAMR_MIXED_TABLES]")
        except Exception as e:
            self.__logger.error("Failed to insert row. - {}".format(e))
        return
    
    # legacy table 및 tamr ingest 완료된 table 현황. 
    def insert_table_status(self, bindvars) :
        try:
            #self.__database.execute(self.__cm.queries["delete_tamr_table_status"], None)
            #self.__logger.info("Deleted table TAMR_TABLE_STATUS.")            
            statement = self.__cm.queries["insert_tamr_table_status"]            
            bindvar = []
            for var in bindvars:
                row = {**var}                
                dic = dict()
                for key, value in row.items() :
                    key = str(key).replace("-", "_")
                    dic[key] = ''.join(value) if type(value) == list else str(value)                        
                bindvar.append(dic)
                
            self.__logger.info("Start inserting table status TAMR_TABLE_STATUS({} rows).".format(len(bindvar))) 
            self.execute_query_with_params(statement, bindvar)
            self.__logger.info("Table status Insert to datbase SUCCESSED. [table = TAMR_TABLE_STATUS]")
        except Exception as e:
            self.__logger.error("Failed to insert row. - {}".format(e))
        return

    # 큐브 알람 전달. 
    def insert_feedback_notification(self, bindvars) :
        try:
            self.__database.connect()            
            seq = self.__database.execute(self.__cm.queries["getMaxSeqNotification"], None)

            statement = self.__cm.queries["insert_tamr_notifications"]
            bindvar = []
            for row in bindvars:
                row["msg_seq"] = seq[0]["SEQ"]
                dic = dict()
                for key, value in row.items() :
                    key = str(key).replace("-", "_")
                    dic[key] = ''.join(value) if type(value) == list else str(value)                        
                bindvar.append(dic)
 
            self.__logger.info("Start inserting notification CUBE_BOT_MSG.".format(len(bindvar)))
            self.__database.execute(statement, bindvar)
            self.__database.disconnect()
            self.__logger.info("Notification Insert to datbase SUCCESSED. [table = CUBE_BOT_MSG]")
        except Exception as e:
            self.__logger.error("Failed to insert row. - {}".format(e))
        return

    def insert_dataset_ui(self) :
        try:
            self.__database.connect()            
            statement = self.__cm.queries["insert_data_ui"]
            
            self.__logger.info("Start execute procedure SYNC_ALL.")
            self.__database.execute_proc(statement)
            self.__database.disconnect()
            self.__logger.info("Procedure call SUCCESSED.")
        except Exception as e:
            self.__logger.error("Failed call procedure. - {}".format(e))
        return

    ################################# unify #################################

    # unified_dataset_dedup_published_clusters_with_data 원본.
    def get_unified_published_clusters(self) :
        try:
            dataset_list = self.__unify.versioned.get_unified_dataset_dedup_published_clusters_with_data(self.__unify.proj_nm)
            return dataset_list
        except Exception as e:
            self.__logger.error("Failed to get unified published clusters with data. - {}".format(e))
        return

    def update_golden_record(self, project_name) :
        self.__unify.dedup.refresh_golden_records(project_name )
        self.__unify.dedup.publish_golden_records(project_name)
        self.__unify.dedup.generate_golden_records(project_name)
        return

    def get_golden_record(self) :
        try:
            dataset_list = []
            golden_record_draft_dataset = [{"persistentId": row["persistentId"], "cluster_full_name": row["COLUMN_NAME_TOKENIZED_STD"], "column_name": row["COLUMN_NAME"]} for row in self.__unify.versioned.get_golden_records_draft(self.__cm_unify.proj_nm)]
            published_dataset = [{"persistentId": row["persistentId"], "cluster_name": row["clusterName"]} for row in self.__unify.versioned.get_unified_dataset_dedup_published_clusters_with_data(self.__unify.proj_nm)]
            golden_records = [{"persistentId": row["persistentId"], "cluster_alias": row["COLUMN_NAME_TOKENIZED"]} for row in self.__unify.versioned.get_golden_records(self.__unify.proj_nm)]
            golden_record_override_dataset = [{"persistentId": row["persistentId"], "cluster_alias": row["value"]} for row in self.__unify.versioned.get_golden_records_overrides(self.__unify.proj_nm)]
            golden_record_alias = []
            golden_record_alias.extend(golden_record_override_dataset)
            
            ids = [row["persistentId"] for row in golden_record_alias]
            for row in golden_records :
                if row["persistentId"] not in ids :
                    golden_record_alias.append(row)
            
            for data in golden_record_draft_dataset :
                for pub in published_dataset:
                    if data["persistentId"] == pub["persistentId"] :
                        data = { **data, **pub }
                        break                                
                for alias in golden_record_alias:
                    if data["persistentId"] == alias["persistentId"] :
                        data = { **data, **{"cluster_alias": str(alias["cluster_alias"]).strip()} }
                        self.__logger.debug(data)
                        break
                
                if 'cluster_name' not in data :
                    data = {**data, **{"cluster_name": data["column_name"]}}
                if 'cluster_alias' not in data :
                    data = {**data, **{"cluster_alias": ""}}
                dataset_list.append(data)            

            return dataset_list
        except Exception as e:
            self.__logger.error("Failed to get unified published clusters with date. - {}".format(e))
        return

    # cluster가 publish 된 날짜 목록.
    def get_unified_published_clusters_date(self) :
        try:
            dataset_list = []
            for date in self.__unify.versioned.get_clusters_published_date(self.__unify.proj_nm) :
                row = {"persistent_id": date["persistentId"], "publish_dt": date["materializationDate"]}                
                dataset_list.append(row)
                self.__logger.info(row)
        except Exception as e:
            self.__logger.error("Failed to get unified published clusters date. - {}".format(e))
        return

    # unified_dataset_dedup_published_clusters_with_data의 top 100 count 컬럼을 key, value로 변환.
    def get_unified_published_clusters_top_values_to_rows(self) :
        try:
            published_dataset = self.__unify.versioned.get_unified_dataset_dedup_published_clusters_with_data(self.__unify.proj_nm)
            dataset_list = []
            for published in published_dataset :
                if ''.join( published["originEntityId"]) != 'Tamr_Profiling_Seq' and int(''.join(published["EMPTY_VALUE_COUNT"])) != 0 :
                    dataset_list.append({"persistent_id":published["persistentId"], "table_name":''.join(published["TABLE_NAME"]), "column_name":''.join(published["COLUMN_NAME"]), "value_name":"(null)", "record_count":int(''.join(published["RECORD_COUNT"])), "value_count":int(''.join(published["EMPTY_VALUE_COUNT"])), "distinct_count": int(''.join(published["DISTINCT_VALUE_COUNT"]))})
                if published["TOP_100_COUNT"] != None and ''.join(published["TOP_100_COUNT"]) != "" :
                    try:
                        json_data = json.loads(''.join(published["TOP_100_COUNT"]), encoding='UTF-8')
                        arr = pd.Series(json_data)                        
                        for name, value in arr.items():                            
                            if name is not None :
                                if len(name) == 0 : name = " "
                                dataset_list.append({"persistent_id":published["persistentId"], "table_name":''.join(published["TABLE_NAME"]), "column_name":''.join(published["COLUMN_NAME"]), "value_name":name.replace("\r\n", "").replace("\0", ""), "record_count":int(''.join(published["RECORD_COUNT"])), "value_count":int(value), "distinct_count": int(''.join(published["DISTINCT_VALUE_COUNT"]))})
                    except Exception as e:
                        self.__logger.error(e)
                        self.__logger.debug(published["TOP_100_COUNT"])
            return dataset_list
        except Exception as e:
            self.__logger.error(e)
        return

    def get_stream_dataset_by_name(self, dataset_name) :
        try:
            return self.__unify.versioned.get_stream_dataset(dataset_name)
        except Exception as e:
            self.__logger.error(e)
        return

    # column_metadata 원본.
    def get_unified_metadata(self, sources=None) :
        dataset_list = []
        try:
            if sources == None :
                datasets = self.__unify.versioned.get_dataset_metadata()
                for dataset in datasets:
                    for row in self.get_stream_dataset_by_name(dataset) :
                        dataset_list.append({**row, **{"source": dataset[:dataset.find("_")].upper() }})
            else :
                for source in sources :                    
                    for row in self.__unify.versioned.get_unified_metadata_dataset(source) :
                        dataset_list.append({**row, **{"source": str(source).upper() }})
            return dataset_list
        except Exception as e:
            self.__logger.error(e)
        return

    # column_metadata_profiled 원본.
    def get_unified_metadata_profiled(self, sources=None) : 
        try:
            dataset_list = []
            if sources == None :
                datasets = self.__unify.versioned.get_dataset_profiled()
                for dataset in datasets:
                    for row in self.get_stream_dataset_by_name(dataset) :
                        dataset_list.append({**row, **{"source": dataset[:dataset.find("_")].upper() }})
            else :
                for source in sources :                    
                    for row in self.__unify.versioned.get_unified_metadata_profiled_dataset(source) :
                        dataset_list.append({**row, **{"source": str(source).upper() }})
            return dataset_list
        except Exception as e:
            self.__logger.error(e)
        return

    # project의 unified dataset 목록
    def get_projects(self) : 
        try:
            response_json = self.__unify.versioned.get_projects()
            projects_d ={}
            for item in response_json:
                if 'unifiedDatasetName' in item:
                    projects_d[item['unifiedDatasetName']] = item['name']
            return projects_d
        except Exception as e:
            self.__logger.error(e)
        return

    # pair에 등록되어 있는 comment 목록
    def get_pair_comments_df(self) :
        dataset_list = []
        projects = self.get_projects()
        pairs = self.__unify.dedup.get_pairs(projects)
        comment_queries = self.__unify.dedup.get_pair_comments_query(pairs)

        for comment in comment_queries :
            for c in comment :
                arr = ({"originTransactionId1": "", "originTransactionId2":"", "message": ""})
                for pair in pairs:
                    for p in pair["items"]  :
                        if c["data"]["recordPairId"]["transactionId1"] == p["transactionId1"] and c["data"]["recordPairId"]["transactionId2"] == p["transactionId2"] :
                            arr["originTransactionId1"] = p["originTransactionId1"]
                            arr["originTransactionId2"] = p["originTransactionId2"]
                            arr["message"] = c["data"]["message"]
                            dataset_list.append(arr)

        return dataset_list   

    # cluster id 목록들을 lock
    def set_lock_clusters(self, dataset_name, ids)  :
        try:
            self.__logger.info("locked dataset = {}     cluster ids = {}".format(dataset_name, ','.join(ids)))
            self.__unify.dedup.set_lock_clusters(dataset_name, ids)
        except Exception as e:
            self.__logger.error(e)
        return 

    # 해당 dataset을 unify에서 삭제.
    def delete_dataset(self, dataset_name) :
        return

    # 피드백 정보
    def get_record_pair_feedback(self, data) :        
        try:
            result_sets = []

            for row in self.__unify.persistence.get_stream_query("record_pair_feedback", data) :                
                if "PENDING" == row["data"]["assignmentInfo"]["status"] :
                    result_sets.append(row)

            return result_sets
        except Exception as e:
            self.__logger.error(e)
        return 