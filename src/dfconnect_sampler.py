#!/bin/python
import os
import sys
import base64
import requests
import json
from custom_logger import CustomLogger
from data_sampler import DataSampler
from tamr_unify_client.auth import UsernamePasswordAuth

import sys
path_of_src = os.path.dirname(os.path.realpath(__file__))
sys.path.append(path_of_src + '/../pnd')
from schema_compare import SchemaCompare 

class DfConnectSampler(DataSampler):
    """
    This is a class to use df-connect app to sample data from the specified Oracle server and save
    sampled data into a dataset on Unify
    """
    def __init__(self, source_sampler, unify_config, source_config, jdbc_url, jdbc_user, jdbc_password):

        # logger
        self.logger = CustomLogger("dfconnect_sampler")

        self.source_sampler = source_sampler

        self._unify_protocol = unify_config['protocol']
        self._unify_hostname = unify_config['hostname']
        self._unify_auth = UsernamePasswordAuth(unify_config['user'], unify_config['pwd'])
        auth = '{}:{}'.format(unify_config['user'], unify_config['pwd'])
        encoded = base64.b64encode(auth.encode('latin1'))
        self._basicCreds = 'BasicCreds ' + requests.utils.to_native_string(encoded.strip())

        self._connectPort = unify_config['connectPort']
        self._profileDatasetName = source_config['profileDatasetName']
        self._source_conf = source_config
        
        self._jdbc_url = jdbc_url
        self._jdbc_user = jdbc_user
        self._jdbc_password = jdbc_password
        self.url = '{}://{}:{}/api/jdbcIngest/profile'.format(self._unify_protocol, self._unify_hostname, self._connectPort)		

    def get_tables(self):
        """
        Get all tables in the specified server
        :return: List of table names
        """
        return self.source_sampler.get_tables()

    def profile(self, table, profileDatasetName=None):
        """
        Call df-connect to profile table and save metadata into a Unify dataset
        :param table: Table name to profile
        :return: None
        """
        #url = '{}://{}:{}/api/jdbcIngest/profile'.format(
        #    self._unify_protocol, self._unify_hostname, self._connectPort
        #)
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
                        # "query": "SELECT * FROM {}".format(table),
                        "query": self._source_conf['getDataQuery'].format(table),
                        "datasetName": self._profileDatasetName if profileDatasetName is None else profileDatasetName, 
                        "primaryKey": []
                    }
                ]
            }        
        
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json', 'Authorization': self._basicCreds}
        response = requests.post(self.url, headers=headers, data=json.dumps(queryConfig))
        if response.status_code != 200:
            self.logger.error("Request to profile {} has failed".format(table))
            self.logger.error(response.text)

    def get_sampled_data(self, path_of_data, does_reload=False):
        """
        Profile all tables in specified server and save metadata into a Unify dataset
        :param path_of_data: Dummy parameter not used.
        :return: True if successful. Otherwise None
        """
        self.logger.info("URL = {}".format(self.url))
        compare = SchemaCompare(self._source_conf['name'])        
        if does_reload or compare.is_exist_dataset == False :
            self.logger.info("Sampling all reload tables.")
            compare.truncate_unified_dataset(self._source_conf['profileDatasetName'])
            all_tables = self.get_tables()
        else:
            self.logger.info("Sampling new or changed tables.")
            all_tables = compare.new_tables
            all_tables.extend(compare.updated_tables)
            all_tables = list(set(all_tables))

        if len(all_tables) > 0:
            profiled_list = []
            idx = 0
            for table in all_tables:
                self.logger.info("Profiling table {} ({} / {} Completed)".format(table, idx, len(all_tables)))
                self.profile(table)
                idx += 1
        return True