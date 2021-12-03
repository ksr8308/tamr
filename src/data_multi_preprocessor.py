#!/bin/python
import sys
import csv
import glob
import re
from os.path import basename
import json
import os
import pandas as pd
import numpy as np
import operator
import time

from chardet.universaldetector import UniversalDetector
from custom_logger import CustomLogger
from unify import Unify
from collections import OrderedDict
from multiprocessing import Process

path_of_src = os.path.dirname(os.path.realpath(__file__))
sys.path.append(path_of_src + '/../pnd')

sys.path.append(path_of_src + '/../pnd/config')
from pnd_logger import PndLogger
from config_manager import ConfigManager

sys.path.append(path_of_src + '/../pnd/data')
from data_manager import DataManager
from profile_manager import ProfileManager

os.putenv('NLS_LANG', 'KOREAN_KOREA.AL32UTF8')

class DataMultiPreprocessor(object):
    """
    This is a class to generate metadata from source data csv files and save metadata into csv files
    """
    def __init__(self, input_folder, output_folder, unify_config, source_config, path_to_token_dict=None):

        self.input_folder = input_folder
        self.output_folder = output_folder
        self._unify_config = unify_config
        self._source_config = source_config
        # logger
        self.logger = CustomLogger("data_multi_preprocessor")

        if path_to_token_dict is not None:
            self.token_dictionary = self._get_token_dict(path_to_token_dict)
        else:
            self.token_dictionary = None

        # pnd customize
        self.pnd_logger = PndLogger("Data Multi Preprocessor", "preprocess")
        self._pm = ProfileManager(self._source_config["name"], True, "preprocess")        
        self._legacy_extend_columns = None        
        self._all_table_columns = self._pm.getAllTableColumns()

        if self._source_config["name"] == 'legacy' :
            cm = ConfigManager("tamr")
            dm = DataManager(ConfigManager("tamr"), "preprocess")
            self.__table_system_info = dm.get_system_from_table()
            self.__legacy_extend_columns = dm.execute_query(cm.queries["getColumnDictLegacy"])
            # self._erp_all_table_columns = dm_mdm.execute_query(cm.queries["getAllColumnsForErp"])
        
    def _get_token_dict(self, path_to_dict):
        """
        Get token dictionary from file
        :param path_to_dict: Absolute path to token dictionary file
        :return: Dictionary if successful, otherwise None
        """
        result = {}
        if os.path.exists(path_to_dict):
            try:
                with open(path_to_dict, encoding='utf-8') as input_file:
                    reader = csv.DictReader(input_file, delimiter='\t', quotechar='"', escapechar='\\')
                    for row in reader:
                        if row['full_words'] != '':
                            result[row['token']] = row['full_words']
                return result
            except Exception as e:
                self.pnd_logger.error(e)
                return None
        else:
            self.logger.error("File {} doesn't exist".format(path_to_dict))
            return None

    @staticmethod
    def _tokenize_column_name(column_name):
        """
        Split column name at underscore ('_') and numeric values
        :param column_name:
        :return: List of tokens
        """
        result = re.sub(r'[0-9_]+', ' ', column_name)
        return result.split()

    def _normalize_column_name(self, column_name):
        """
        Normalize tokens in column names using dictionary
        :param column_name: Already tokenized column name
        :return: Column name with token normalized using dictionary
        """
        result = column_name.lower()
        if self.token_dictionary is None:
            return result
        tokens = self.token_dictionary.keys()
        for token in tokens:
            regex_token = re.compile(r'\b{}\b'.format(token), re.IGNORECASE)
            result = regex_token.sub(self.token_dictionary[token], result)
        return result

    @staticmethod
    def _get_keys(phrases):
        """
        Get list of keys if phrases are json strings
        :param phrases: List of string values
        :return: List of json keys
        """
        result = set()
        for phrase in phrases:
            try:
                object_content = json.loads(phrase)
                if type(object_content) is list and type(object_content[0]) is dict:
                    result = result.union(set(object_content[0].keys()))
                elif type(object_content) is dict:
                    result = result.union(set(object_content.keys()))
                else:
                    continue
            except Exception as e:
                continue
        return ' '.join(list(result))

    @staticmethod
    def _get_patterns(top_n_values_freq):
        """
        Get patterns where character [a-zA-Z] is replaced by 'S' and [0-9] is replaced by 'N'
        :param top_n_values_freq: Dictionary of most frequent values and corresponding frequencies
        :return: Dictionary of most frequent patterns and corresponding frequencies
        """
        result = {}
        for value in top_n_values_freq:
            pattern = re.sub(r'[a-zA-Z]', 'S', str(value))
            pattern = re.sub(r'[0-9]', 'N', pattern)
            pattern = re.sub(r'[\u3131-\uD79D]', 'K', pattern)
            if pattern not in result:
                result[pattern] = float(top_n_values_freq[value])
            else:
                result[pattern] += float(top_n_values_freq[value])
        return result

    @staticmethod
    def _get_business_types_and_length(phrases):
        """
        Get business type labels as defined by regex
        :param phrase: String value
        :return: List of labels
        """
        values = [value.strip() for value in phrases]
        regex_numeric_int = re.compile(r'^[\-]*\d+$', re.IGNORECASE)
        regex_numeric_float = re.compile(r'^[\-]*([0-9]*\.[0-9]+|[0-9]+\.[0-9]*)$', re.IGNORECASE)
        regex_alphabetical = re.compile(r'^[a-zA-Z]*$', re.IGNORECASE)
        regex_mixed = re.compile(r'^(?=.*[a-zA-Z])(?=.*[0-9])', re.IGNORECASE)
        found_int = False
        found_float = False
        found_alphabetical = False
        found_mixed = False
        n_digits = set()
        for value in values:
            if len(regex_numeric_int.findall(value)) > 0:
                found_int = True
            if len(regex_numeric_float.findall(value)) > 0:
                found_float = True
            if len(regex_alphabetical.findall(value)) > 0:
                found_alphabetical = True
            if len(regex_mixed.findall(value)) > 0:
                found_mixed = True
            if len(value) > 0:
                n_digits.add(len(value))
        types = []
        if found_float:
            types.append('numeric_float')
        if found_int:
            types.append('numeric_integer')
        if found_alphabetical:
            types.append('alphabetical')
        if found_mixed:
            types.append('alphanumeric')
        return ' '.join(types), ' '.join([str(item) for item in n_digits])

    def worker(self, id, writer, data_list) :        
        # if id == 0:
        #     self.pnd_logger.info("header created...")
        #     writer.writeheader()
        cur_idx = 0
        end_idx = len(data_list)
        success_cnt = 0
        exp_cnt = 0
        for line in data_list:            
            cur_idx += 1
            try:
                record = json.loads(line)
                
                # blob 과 clob 타입인 컬럼은 제외
                if self._pm.except_column(''.join(record['TableName']), ''.join(record['ColumnName']), ''.join(record['ColumnType'])) :
                    exp_cnt += 1                    
                    continue
               
                # 비교 컬럼 설정
                keyword = record["Tamr_Profiling_Seq"].upper()
                mixed_column_tmp = None
                column_name = ''.join(record['ColumnName'])

                if self._source_config["name"] in ["mdm", "mixed"] :
                    for row in self._all_table_columns :
                        if keyword == row["TABLE_COLUMN"] :
                            record['ATTR_EN_NM'] = row['ATTR_EN_NM']
                            mixed_column_tmp = row['TECH_COL_ID'] if self._source_config["name"] == "mixed" else ""
                            p = re.compile("[^0-9]")
                            if re.match("^PROP$|^SAP$|^ZTECH$|^ATTR$|^BOM$", "".join(p.findall(''.join(record['ColumnName'])))) is not None and \
                                re.search("[ㄱ-ㅣ가-힣|\(|\)|\[|\]]+", row["ATTR_EN_NM"]) is None and \
                                row["ATTR_EN_NM"] is not None:
                                column_name = ''.join(row["ATTR_EN_NM"])
                            
                if len(record['ColumnName']) > 0:
                    column_name_tokenized = ' '.join(self._tokenize_column_name(column_name))
                    column_name_standardized = self._normalize_column_name(column_name_tokenized)
                else:
                    column_name = ''
                    column_name_tokenized = ''
                    column_name_standardized = ''

                keys = ''
                top_n_values = []
                top_n_values_freq = ''
                patterns = ''
                patterns_freq = ''
                if record['Top100Values'] is not None and len(str(record['Top100Values'][0]).strip()) > 0:
                    keys = self._get_keys(record['Top100Values'])                                                            
                    
                    sorted_Top100Values = OrderedDict(sorted(json.loads(record['Top100Counts'][0]).items(), key=lambda x: x[1], reverse=True))
                    top_n_values = list(sorted_Top100Values.keys())[:20]

                    if 'Top100Frequencies' in record and record['Top100Frequencies'] is not None and record['Top100Frequencies'][0] != '':
                        if record['Top100Frequencies'][0] == "{}":
                            record['Top100Frequencies'] = [""]
                            top_n_values_freq = {}                            
                        else:
                            try:
                                top_n_values_freq = json.loads(record['Top100Frequencies'][0])
                            except Exception as e:
                                self.pnd_logger.error(e)
                                top_n_values_freq = {}
                            record['Top100Frequencies'] = ['"""' + record['Top100Frequencies'][0] + '"""']
                    else:
                        top_n_values_freq = {}
                        for value in top_n_values:
                            top_n_values_freq[value] =  1./len(top_n_values)
                    patterns_freq = self._get_patterns(top_n_values_freq)
                    patterns = ' '.join(list(set(patterns_freq.keys())))

                business_type, length = self._get_business_types_and_length(top_n_values)
                top_n_values = ', '.join(top_n_values)
                for key in record.keys():
                    if isinstance(record[key], list):
                        record[key] = ' '.join(record[key])

                record['Tamr_Profiling_Seq'] = record['Tamr_Profiling_Seq'].upper()
                record['column_name_tokenized'] = column_name_tokenized.upper()
                record['column_name_tokenized_std'] = column_name_standardized.upper()
                record['business_type'] = business_type
                record['length'] = length
                record['keys'] = keys
                record['patterns'] = patterns                
                record['Top100Frequencies'] = ""
                record['KEY_DOM_NM'] = ""
                record['top_n_values'] = top_n_values

                if self._source_config["name"] == "mixed" :
                    record['SYSTEM_NAME'] = "GMDM"
                    record['FAB'] = "ALL"                    
                    record['ColumnName'] = mixed_column_tmp if mixed_column_tmp is not None else record['ColumnName']
                    record['KEY_DOM_NM'] =  "" if 'KEY_DOM_NM' not in row else str(row['KEY_DOM_NM']).replace("FAB", "공장").replace("스텝", "공정").replace("상세공장", "공장").replace("PKT", "패키지").replace("제조사", "업체")
                elif self._source_config["name"] == "mdm" :
                    for row in self._all_table_columns : 
                        if keyword == row["TABLE_COLUMN"] :                            
                            record['SYSTEM_NAME'] = "GMDM"
                            record['FAB'] = "ALL"                            
                            record['COL_DESC'] =  "" if 'COL_DESC' not in row else row['COL_DESC']
                            record['COL_KO_NM'] =  "" if 'COL_KO_NM' not in row else row['COL_KO_NM']
                            record['KEY_DOM_NM'] =  "" if 'KEY_DOM_NM' not in row else str(row['KEY_DOM_NM']).replace("FAB", "공장").replace("스텝", "공정").replace("상세공장", "공장").replace("PKT", "패키지").replace("제조사", "업체")
                elif self._source_config["name"] == "legacy" :                    
                    record["SYSTEM_NAME"] = self._pm.get_convert_system_fab_name(self.__table_system_info, str(record["TableName"]).replace("__", "_"))["SYSTEM"]
                    record["FAB"] = self._pm.get_convert_system_fab_name(self.__table_system_info, record["TableName"])["FAB"]                    
                    for row in self.__legacy_extend_columns : 
                        if record['ColumnName'] == row["DIC_PHY_NM"] :
                            record['COL_DESC'] =  "" if 'DIC_DESC' not in row else row['DIC_DESC']
                            record['COL_KO_NM'] =  "" if 'DIC_LOG_NM' not in row else str(row['DIC_LOG_NM']).replace("FAB", "공장").replace("스텝", "공정").replace("상세공장", "공장").replace("PKT", "패키지").replace("제조사", "업체")
                            record['KEY_DOM_NM'] =  "" if 'KEY_DOM_NM' not in row else str(row['KEY_DOM_NM']).replace("FAB", "공장").replace("스텝", "공정").replace("상세공장", "공장").replace("PKT", "패키지").replace("제조사", "업체")
                    # if str(record['Tamr_Profiling_Seq']).startswith("ERP_") :
                    #     for row in self._erp_all_table_columns : 
                    #         if keyword == row["TABLE_COLUMN"] :
                    #             record['ATTR_EN_NM'] =  "" if 'ATTR_EN_NM' not in row else row['ATTR_EN_NM']
                    #             record['COL_DESC'] =  "" if 'DIC_DESC' not in row else row['DIC_DESC']                            
                    #             record['KEY_DOM_NM'] = ""
                    # else :                        
                    #     for row in self.__legacy_extend_columns : 
                    #         if record['ColumnName'] == row["DIC_PHY_NM"] :
                    #             record['COL_DESC'] =  "" if 'DIC_DESC' not in row else row['DIC_DESC']
                    #             record['COL_KO_NM'] =  "" if 'DIC_LOG_NM' not in row else str(row['DIC_LOG_NM']).replace("FAB", "공장").replace("스텝", "공정").replace("상세공장", "공장").replace("PKT", "패키지").replace("제조사", "업체")
                    #             record['KEY_DOM_NM'] =  "" if 'KEY_DOM_NM' not in row else str(row['KEY_DOM_NM']).replace("FAB", "공장").replace("스텝", "공정").replace("상세공장", "공장").replace("PKT", "패키지").replace("제조사", "업체")

                record['ColumnName'] = record['ColumnName'].upper()
                # profiled 데이터에 아래 패턴의 컬럼 및 데이터가 있을 경우 profiled에서 제외
                if self._pm.except_column(''.join(record['TableName']), ''.join(record['ColumnName']), ''.join(record['ColumnName'])) :
                    exp_cnt += 1                    
                elif self._pm.except_column(''.join(record['TableName']), ''.join(record['ColumnName']), ''.join(record['KEY_DOM_NM'])) :
                    exp_cnt += 1                                    
                elif self._pm.except_column(''.join(record['TableName']), ''.join(record['ColumnName']), ''.join(record['column_name_tokenized_std'])) :
                    exp_cnt += 1
                else :                      
                    writer.writerow(record)
                    success_cnt += 1
                
                self.pnd_logger.info("[Source={}] [Thread No={}] {} / {} rows completed, success={} , except={}".format(self._source_config["name"], id, cur_idx, end_idx, success_cnt, exp_cnt))
            except Exception as e:
                exp_cnt += 1
                self.pnd_logger.error(e)                

        self.pnd_logger.info("[Source={}] [Thread No={}] Generate metadta completed.".format(self._source_config['name'], id))

    def process_unify_dataset(self, source_type):        
        path_of_src = os.path.dirname(os.path.realpath(__file__))
        path_of_tmp = os.path.abspath(path_of_src + "/../tmp/")
        path_of_output = os.path.abspath(path_of_src + "/../output/")

        if not os.path.exists(path_of_tmp):
            os.makedirs(path_of_tmp)
        if not os.path.exists(path_of_output):
            os.makedirs(path_of_output)

        myUnify = Unify(
            self._unify_config["protocol"],
            self._unify_config["hostname"],
            self._unify_config["port"],
            self._unify_config["grPort"],
            self._unify_config["user"],
            self._unify_config["pwd"]
        )
        output_file_json = open(path_of_tmp + '/' + self._source_config['profileDatasetName'], 'w')

        field_names = None
        for line in myUnify.stream_dataset(self._source_config['profileDatasetName']):
            if field_names is None:
                field_names = list(line.keys())
            output_file_json.write(json.dumps(line) + '\n')
        output_file_json.close()

        # erp_input_file_json = None
        # if self._source_config["name"] == "legacy" :
        #     output_file_json = open(path_of_tmp + '/erp_column_metadata' , 'w')
        #     for line in myUnify.stream_dataset("erp_column_metadata"):
        #         output_file_json.write(json.dumps(line) + '\n')
        #     output_file_json.close()
        #     erp_input_file_json = open(path_of_tmp + '/erp_column_metadata', 'r')
        
        ## 테스트시 사용
        #erp_input_file_json = None
        #if self._source_config["name"] == "legacy" :
        #    erp_input_file_json = open(path_of_tmp + '/erp_column_metadata', 'r')
        #field_names = ['MinValue', 'Top100Values', 'StdDevValue', 'TableName', 'MaxValue', 'MeanValue', 'TAMRSEQ', 'TotalValueCount', 'EmptyValueCount', 'ColumnType', 'RecordCount', 'Tamr_Profiling_Seq', 'ColumnName', 'Top100Counts', 'DistinctValueCount', 'Top100Frequencies']

        field_names.extend(['column_name_tokenized', 'column_name_tokenized_std','business_type', 'length', 'keys', 'top_n_values', 'patterns', 'SYSTEM_NAME', 'FAB', 'COL_DESC', 'COL_KO_NM', 'ATTR_EN_NM', 'KEY_DOM_NM'])
        input_file_json = open(path_of_tmp + '/' + self._source_config['profileDatasetName'], 'r')
        json_data = [row for row in input_file_json]

        # if erp_input_file_json is not None :
        #     erp_json_data = [row for row in erp_input_file_json]
        #     json_data.extend(erp_json_data)

        self.pnd_logger.info("Total Profiling Rows = {}".format(len(json_data)))
       
        max_thread = os.cpu_count()
        # job 목록을 설정.
        job_list = []        
        interval = int(len(json_data) / max_thread)
        if interval == 0:
            interval = 1
        quot =  len(json_data) // interval if len(json_data) % interval == 0 else len(json_data) // interval + 1
        loop_cnt = 0
        
        while loop_cnt < quot :            
            job = {"thread_no": loop_cnt, "rows": json_data[loop_cnt * interval:(loop_cnt + 1) * interval] }
            job_list.append(job)
            loop_cnt += 1
        
        self.pnd_logger.info("source={}   rows={}   max_thread={}   interval={}".format(self._source_config['name'], len(json_data), max_thread, interval))
        
        # 헤더 생성
        with open(path_of_output + '/' + self._source_config['profileDatasetName'] + '_profiled.csv', 'w') as output_file_csv :
            writer = csv.DictWriter(output_file_csv, fieldnames=field_names)
            writer.writeheader()
            
        # row 생성
        with open(path_of_output + '/' + self._source_config['profileDatasetName'] + '_profiled.csv', 'a') as output_file_csv :
            writer = csv.DictWriter(output_file_csv, fieldnames=field_names)
            process_list = []
            
            #프로세스 스레드 목록을 생성.
            for job in job_list :                
                proc = Process(target=self.worker, args=(job["thread_no"], writer, job["rows"]))
                process_list.append(proc)
                self.pnd_logger.info("[Source={}] [Thread No={}] Rows={}".format(self._source_config['name'], job["thread_no"], len(job["rows"])))
                proc.start()

            for proc in process_list :
                proc.join()

        self.pnd_logger.info("[Source={}] Generate metadata completed.".format(self._source_config['name']))        
        self.logger.info("[Source={}] Generate metadata completed.".format(self._source_config['name']))        
        return True
