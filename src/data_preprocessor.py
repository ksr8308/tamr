#!/bin/python
import csv
import glob
import re
from os.path import basename
import json
import os
import pandas as pd
import numpy as np
from chardet.universaldetector import UniversalDetector
from custom_logger import CustomLogger
from unify import Unify

import sys
path_of_src = os.path.dirname(os.path.realpath(__file__))
sys.path.append(path_of_src + '/../pnd')
from schema_compare import SchemaCompare 

sys.path.append(path_of_src + '/../pnd/data')
from config_manager import ConfigManager
from data_manager import DataManager

class DataPreprocessor(object):
    """
    This is a class to generate metadata from source data csv files and save metadata into csv files
    """
    def __init__(self, input_folder, output_folder, unify_config, source_config, path_to_token_dict=None):

        self.input_folder = input_folder
        self.output_folder = output_folder
        self._unify_config = unify_config
        self._source_config = source_config
        # logger
        self.logger = CustomLogger("data_preprocessor")

        if path_to_token_dict is not None:
            self.token_dictionary = self._get_token_dict(path_to_token_dict)
        else:
            self.token_dictionary = None

        # self.compare_schema = SchemaCompare(self._source_config["name"])
        # self._all_table_columns = self.compare_schema.all_table_columns
        # self._legacy_extend_columns = None
        
        # if self._source_config["name"] == 'legacy' :
        #     cm_mdm = ConfigManager("mdm")
        #     dm_mdm = DataManager("mdm")
        #     self.__legacy_extend_columns = dm_mdm.execute_query(cm_mdm.queries["getAllColumnsForLegacy"])
        
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
                    #reader = csv.DictReader(input_file, delimiter=',', quotechar='"', escapechar='\\')
                    reader = csv.DictReader(input_file, delimiter='\t', quotechar='"', escapechar='\\')
                    for row in reader:
                        if row['full_words'] != '':
                            result[row['token']] = row['full_words']
                return result
            except Exception as e:
                self.logger.error(e)
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
        # values = phrase.split(',')
        values = [value.strip() for value in phrases]
        regex_numeric_int = re.compile(r'^[\-]*\d+$', re.IGNORECASE)
        regex_numeric_float = re.compile(r'^[\-]*([0-9]*\.[0-9]+|[0-9]+\.[0-9]*)$', re.IGNORECASE)
        regex_alphabetical = re.compile(r'^[a-zA-Z]*$', re.IGNORECASE)
        # regex_mixed = re.compile(r'^[0-9a-zA-Z :_.-]+$', re.IGNORECASE)
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

    def process_unify_dataset(self, source_type):
        """
        Process the specified profileDatasetName in Unify
        :return: True if successful. Otherwise None
        """
        # get locations
        path_of_src = os.path.dirname(os.path.realpath(__file__))
        path_of_tmp = os.path.abspath(path_of_src + "/../tmp/")
        path_of_output = os.path.abspath(path_of_src + "/../output/")
        # mkdir if not exists
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

        # expected field names in the df-connect profiled dataset:
        # ['ColumnName', 'TableName', 'Top100Values', 'ColumnType', 'Tamr_Profiling_Seq', 'EmptyValueCount', 'MinValue',
        #   'MaxValue', 'RecordCount', 'DistinctValueCount', 'TAMRSEQ']
        field_names.extend(['table_name', 'column_name', 'column_name_tokenized', 'column_name_tokenized_std',
                            'business_type', 'length', 'keys', 'top_n_values', 'top_n_values_freq', 'patterns_freq', 'patterns', 'SYS_GBN_CD', 'MST_TYP_ENG', 'COL_DESC', 'COL_KO_NM', 'ATTR_EN_NM', 'KEY_DOM_NM'])
        input_file_json = open(path_of_tmp + '/' + self._source_config['profileDatasetName'], 'r')
        with open(path_of_output + '/' + self._source_config['profileDatasetName'] + '_profiled.csv', 'w') \
                as output_file_csv:
            writer = csv.DictWriter(output_file_csv, fieldnames=field_names)
            writer.writeheader()
            
            idx = 0
            for line in input_file_json:
                line = line[:-1]
                record = json.loads(line)
                
                if "blob" in ''.join(record['ColumnType']) or "clob" in ''.join(record['ColumnType']) :
                    self.logger.debug("blob & clob column type : {}".format(record))
                    continue
                    
                if len(record['ColumnName']) > 0:
                    column_name = record['ColumnName'][0]
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
                if record['Top100Values'] is not None and len(record['Top100Values']) > 0:
                    keys = self._get_keys(record['Top100Values'])
                    top_n_values = set([str(item).replace('"', '').strip() for item in record['Top100Values']][:100])
                    if 'Top100Frequencies' in record and record['Top100Frequencies'] is not None and record['Top100Frequencies'][0] != '':
                        if record['Top100Frequencies'][0] == "{}":
                            record['Top100Frequencies'] = [""]
                            top_n_values_freq = {}
                        else:
                            try:
                                top_n_values_freq = json.loads(record['Top100Frequencies'][0])
                            except Exception as e:
                                self.logger.error(e)
                                top_n_values_freq = {}
                            record['Top100Frequencies'] = ['"""' + record['Top100Frequencies'][0] + '"""']
                    else:
                        top_n_values_freq = {}
                        for value in top_n_values:
                            top_n_values_freq[value] =  1./len(top_n_values)
                    patterns_freq = self._get_patterns(top_n_values_freq)
                    patterns = ' '.join(list(patterns_freq.keys()))
                    patterns_freq = '"""' + json.dumps(patterns_freq) + '"""'
                    top_n_values_freq = '"""' + json.dumps(top_n_values_freq) + '"""'
                else:
                    record['Top100Frequencies'] = ""
                business_type, length = self._get_business_types_and_length(top_n_values)
                top_n_values = ', '.join(top_n_values)
                for key in record.keys():
                    if isinstance(record[key], list):
                        record[key] = ' '.join(record[key])
                record['table_name'] = record['TableName']
                record['column_name_tokenized'] = column_name_tokenized
                record['column_name_tokenized_std'] = column_name_standardized
                record['business_type'] = business_type
                record['length'] = length
                record['keys'] = keys
                record['patterns_freq'] = patterns_freq
                record['patterns'] = patterns
                record['top_n_values_freq'] = top_n_values_freq
                record['column_name'] = column_name
                record['KEY_DOM_NM'] = ""

                keyword = record["Tamr_Profiling_Seq"]
                table_column = [str(row["TABLE_COLUMN"]).strip() for row in self._all_table_columns]

                if self.compare_schema.is_deleted_column(keyword.strip()) :
                    self.logger.debug("[{}]Removed Unify Table-Column = {}".format(self._source_config['name'], keyword))
                    record['top_n_values'] = 'REMOVED'
                else :
                    record['top_n_values'] = top_n_values

                # if self._source_config["name"] == "mixed" :
                #     record['SYS_GBN_CD'] = "GMDM"
                #     record['MST_TYP_ENG'] = "MATERIAL"                    
                #     record['ATTR_EN_NM'] = ''.join([row['ATTR_EN_NM'] for row in self._all_table_columns if keyword == row["TABLE_COLUMN"]]).strip()                    
                #     record['column_name'] = ''.join([row['TECH_COL_ID'] for row in self._all_table_columns if keyword == row["TABLE_COLUMN"]]).strip()
                #     record['KEY_DOM_NM'] = ""
                #     if len(record['column_name']) == 0 : record['column_name'] = column_name
                # elif self._source_config["name"] == "mdm" :
                #     for row in self._all_table_columns : 
                #         if keyword == row["TABLE_COLUMN"] :
                #             record['SYS_GBN_CD'] = "" if 'SYS_GBN_CD' not in row else str(row['SYS_GBN_CD']).replace("G-MASTER", "GMDM")
                #             record['MST_TYP_ENG'] =  "" if 'MST_TYP_ENG' not in row else row['MST_TYP_ENG']
                #             record['COL_DESC'] =  "" if 'COL_DESC' not in row else row['COL_DESC']
                #             record['COL_KO_NM'] =  "" if 'COL_KO_NM' not in row else row['COL_KO_NM']
                #             record['ATTR_EN_NM'] =  "" if 'ATTR_EN_NM' not in row else row['ATTR_EN_NM']
                #             record['KEY_DOM_NM'] = ""
                # elif self._source_config["name"] == "legacy" :
                #     for row in self.__legacy_extend_columns : 
                #         if record['column_name'] == row["DIC_PHY_NM"] :                            
                #             record['COL_DESC'] =  "" if 'DIC_DESC' not in row else row['DIC_DESC']
                #             record['COL_KO_NM'] =  "" if 'DIC_LOG_NM' not in row else str(row['DIC_LOG_NM']).replace("FAB", "공장")                            
                #             record['KEY_DOM_NM'] =  "" if 'KEY_DOM_NM' not in row else str(row['KEY_DOM_NM']).replace("FAB", "공장")
                # elif self._source_config["name"] == "erp" :                    
                #     for row in self._all_table_columns : 
                #         if keyword == row["TABLE_COLUMN"] :
                #             record['ATTR_EN_NM'] =  "" if 'ATTR_EN_NM' not in row else row['ATTR_EN_NM']
                #             record['COL_DESC'] =  "" if 'DIC_DESC' not in row else row['DIC_DESC']                            
                #             record['KEY_DOM_NM'] = ""
                
                #writer.writerow(record)
                if record['column_name'] == "LAKE_LOAD_TM" :
                    #print("column_name = {}".format(record['column_name']))
                    self.logger.info("column_name = {}".format(record['column_name']))
                elif re.search("비고|기간|년도|년월|년주|년주월|년중일|년중주차|변경일시|분|분기|생성일시|시|시간|시분|시분초|일|일수|일시|일시V|일자|주당일수|시간V|월|년중주차V|반기|일시TS|HH|일시명|타임스탬프|주|RAWID|사용자ID|내용|설명|순서|여부|건수|건수V|수량|팩스번호|이동전화번호|전화번호|율|율V|상한값V|가동율|환율|하한값|한계값|수율|상한값|파라미터스펙하한값|파라미터스펙상한값|주소|이메일|좌표|Z좌표", str(record["KEY_DOM_NM"])) :    
                    #print("key_dom_nm = {}".format(record["KEY_DOM_NM"]))
                    self.logger.info("key_dom_nm = {}".format(record["KEY_DOM_NM"]))
                else : 
                    writer.writerow(record)
                #print("loaded = {}".format(idx))
                self.logger.info(record)
                idx += 1

        return True

    def process_local_files(self):
        """
        Process all files under input folder
        :return: True if successful. Otherwise None
        """

        # output file
        fieldnames = ['table_name', 'column_name', 'column_name_tokenized', 'column_name_tokenized_std',
                            'business_type', 'length', 'keys', 'top_n_values', 'top_n_values_freq', 'patterns_freq', 'patterns', 'SYS_GBN_CD', 'MST_TYP_ENG', 'COL_DESC', 'COL_KO_NM', 'ATTR_EN_NM']

        output_file = open(self.output_folder + '/' + 'table_column_metadata.csv', 'w', encoding='utf-8')
        output_writer = csv.DictWriter(output_file, fieldnames=fieldnames, delimiter=",", quotechar="\"", escapechar="\\",
                                       quoting=csv.QUOTE_ALL)
        output_writer.writeheader()

        candidate_primary_keys_file = open(self.output_folder + '/' + 'table_candidate_primary_keys.csv', 'w', encoding='utf-8')
        candidate_primary_keys_file.write('table,candidate_keys\n')

        detector = UniversalDetector()
        input_files = glob.glob(self.input_folder + '/*.csv')
        if len(input_files) == 0:
            self.logger.error("No input csv files are found in the input folder {}".format(self.input_folder))
            return None
        for input_file in input_files:
            if os.path.getsize(input_file) == 0:
                continue
            self.logger.info("Processing {}".format(input_file))
            detector.reset()

            for line in open(input_file, 'rb'):
                detector.feed(line)
                if detector.done: break
            detector.close()
            encoding = detector.result
            self.logger.info("Detected encoding: {}".format(encoding))

            my_df = pd.read_csv(
                input_file,
                delimiter=',',
                quotechar='"',
                escapechar='\\',
                encoding=encoding['encoding'],
                index_col=None
            )
            my_df = my_df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
            my_df = my_df.replace(r'^\s*$', np.nan, regex=True)
            
            column_names = list(my_df)           
            
            table_name = input_file.replace(input_file[:input_file.find('.')+1], '')            
            self.logger.info('Found columns for {}: {}'.format(table_name, column_names))
            candidate_keys = []

            for column_name in column_names:
                record = {}
                keys = ''
                values_count = my_df[column_name].value_counts(normalize=True, dropna=True)
                top_values = values_count.index.tolist()

                if len(my_df) == my_df[column_name].nunique():
                    candidate_keys.append(column_name)

                if len(top_values) > 0:
                    keys = self._get_keys(top_values)

                top_n_values = ''
                top_n_values_freq = ''
                patterns_freq = ''
                patterns = ''
                if keys == '':
                    top_n_values = [str(item).replace('"', '') for item in top_values[:100]]
                    top_n_values_freq = values_count.nlargest(100).to_dict()
                    patterns_freq = self._get_patterns(top_n_values_freq)
                    patterns = ' '.join(list(patterns_freq.keys()))
                    patterns_freq = json.dumps(patterns_freq)
                    top_n_values_freq = json.dumps(top_n_values_freq)
                    business_type, length = self._get_business_types_and_length(top_n_values)
                    top_n_values = ', '.join(top_n_values)
                else:
                    business_type, length = '', ''

                column_name_tokenized = ' '.join(self._tokenize_column_name(column_name))
                column_name_standardized = self._normalize_column_name(column_name_tokenized)
                # business_type = self._get_business_types(top_n_values)

                unique_id = table_name + '.' + column_name
                record['table_name'] = table_name
                record['column_name'] = column_name
                record['column_name_tokenized'] = column_name_tokenized
                record['column_name_tokenized_std'] = column_name_standardized
                record['business_type'] = business_type
                record['length'] = length
                record['keys'] = keys
                record['top_n_values'] = top_n_values
                record['top_n_values_freq'] = top_n_values_freq
                record['patterns_freq'] = patterns_freq
                record['patterns'] = patterns

                # keyword = record["Tamr_Profiling_Seq"]
                # table_column = [str(row["TABLE_COLUMN"]).strip() for row in self._all_table_columns]

                # if self.compare_schema.is_deleted_column(keyword.strip()) :
                #     self.logger.debug("[{}]Removed Unify Table-Column = {}".format(self._source_config['name'], keyword))
                #     record['top_n_values'] = 'REMOVED'
                # else :
                #     record['top_n_values'] = top_n_values

                # if self._source_config["name"] == "mixed" :
                #     record['SYS_GBN_CD'] = "GMDM"
                #     record['MST_TYP_ENG'] = "MATERIAL"                    
                #     record['ATTR_EN_NM'] = ''.join([row['ATTR_EN_NM'] for row in self._all_table_columns if keyword == row["TABLE_COLUMN"]]).strip()                    
                #     record['column_name'] = ''.join([row['TECH_COL_ID'] for row in self._all_table_columns if keyword == row["TABLE_COLUMN"]]).strip()
                #     record['KEY_DOM_NM'] = ""
                #     if len(record['column_name']) == 0 : record['column_name'] = column_name
                # elif self._source_config["name"] == "mdm" :
                #     for row in self._all_table_columns : 
                #         if keyword == row["TABLE_COLUMN"] :
                #             record['SYS_GBN_CD'] = "" if 'SYS_GBN_CD' not in row else row['SYS_GBN_CD']
                #             record['MST_TYP_ENG'] =  "" if 'MST_TYP_ENG' not in row else row['MST_TYP_ENG']
                #             record['COL_DESC'] =  "" if 'COL_DESC' not in row else row['COL_DESC']
                #             record['COL_KO_NM'] =  "" if 'COL_KO_NM' not in row else row['COL_KO_NM']
                #             record['ATTR_EN_NM'] =  "" if 'ATTR_EN_NM' not in row else row['ATTR_EN_NM']
                #             record['KEY_DOM_NM'] = ""
                # elif self._source_config["name"] == "legacy" :
                #     for row in self.__legacy_extend_columns : 
                #         if record['column_name'] == row["DIC_PHY_NM"] :                            
                #             record['COL_DESC'] =  "" if 'DIC_DESC' not in row else row['DIC_DESC']
                #             record['COL_KO_NM'] =  "" if 'DIC_LOG_NM' not in row else str(row['DIC_LOG_NM']).replace("FAB", "공장")                            
                #             record['KEY_DOM_NM'] =  "" if 'KEY_DOM_NM' not in row else str(row['KEY_DOM_NM']).replace("FAB", "공장")
                # elif self._source_config["name"] == "erp" :
                #     for row in self.__legacy_extend_columns : 
                #         if record['column_name'] == row["DIC_PHY_NM"] :
                #             record['ATTR_EN_NM'] =  "" if 'ATTR_EN_NM' not in row else row['ATTR_EN_NM']
                #             record['COL_DESC'] =  "" if 'DIC_DESC' not in row else row['DIC_DESC'] 
                #             record['KEY_DOM_NM'] = ""
                
                #if "dt" not in  record['KEY_DOM_NM'] :
                output_writer.writerow(record)

            candidate_primary_keys_file.write('"{}","{}"\n'.format(table_name, ','.join(candidate_keys)))
        output_file.close()
        return True