#!/bin/python
import os
import sys
import base64
import requests
import json
import time
import copy
from custom_logger import CustomLogger
from data_sampler import DataSampler
from tamr_unify_client.auth import UsernamePasswordAuth
#from tamr_unify_client import Client
from multiprocessing import Process, Manager, Pool
from operator import itemgetter

path_of_src = os.path.dirname(os.path.realpath(__file__))
sys.path.append(path_of_src + '/../pnd/config')
from config_manager import ConfigManager
from pnd_logger import PndLogger

sys.path.append(path_of_src + '/../pnd/data')
from custom_unify import CustomUnify
from profile_manager import ProfileManager

class DfConnectMultiSampler(DataSampler):
    """
    This is a class to use df-connect app to sample data from the specified Oracle server and save
    sampled data into a dataset on Unify
    """
    def __init__(self, source_sampler, unify_config, source_config, jdbc_url, jdbc_user, jdbc_password):

        # logger
        self.logger = CustomLogger("dfconnect_multi_sampler")
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

        # pnd custom        
        self.pnd_logger = PndLogger("DfConnectMultiSampler", "sampling")       

    # def get_tables(self, database=None):
    #     """
    #     Get all tables in the specified server
    #     :return: List of table names
    #     """
    #     if self._source_conf['name'] == 'mdm' :
    #         return self.source_sampler.get_tables()
        
    #     if self._source_conf['name'] == 'legacy' :
    #         return self.source_sampler.get_tables(database)

    def profile(self, tables, results, idx, profileDatasetName=None):
        """
        Call df-connect to profile table and save metadata into a Unify dataset
        :param table: Table name to profile
        :return: None
        """        
        # self.pnd_logger.info("executed query = {}".format(table['execute_query']))
        result_tables = []
        total = len(tables)
        successed = 0
        failed = 0
        failed_tables = []
        for table in tables :
            url = self._jdbc_url
            # if self._source_conf['name'] == 'legacy' :
            #     url = url + "/{}?hive.resultset.use.unique.column.names=false".format(table["DATABASE"])

            queryConfig = \
                {
                    "queryConfig": {
                        "jdbcUrl":  url,
                        "dbUsername": self._jdbc_user,
                        "dbPassword": self._jdbc_password,
                        "fetchSize": 1
                    },
                    "queryTargetList": [
                        {
                            "query": table['EXECUTE_QUERY'],
                            "datasetName": self._profileDatasetName if profileDatasetName is None else profileDatasetName, 
                            "primaryKey": []
                        }
                    ]
                }        
            
            headers = {'Content-Type': 'application/json', 'Accept': 'application/json', 'Authorization': self._basicCreds}
            response = requests.post(self.url, headers=headers, data=json.dumps(queryConfig))

            if response.status_code != 200:                
                failed = failed + 1
                failed_tables.append(table["TABLE_NAME"])
                self.pnd_logger.debug("Request to profile {} has failed".format(table))
                self.pnd_logger.debug(response.text)
                # if "[Oracle]ORA-" in response.text :
                #     return "REMOVE"
                # return False
            else:
                table["pid"] = os.getpid()
                successed = successed + 1
                self.pnd_logger.info("NO:[{:>2}]   Total:({:>5}/{:<5})   Failed:({:>3})   TABLE: {}".format(table["THREAD_NO"], successed, total, failed, table["TABLE_NAME"]))
                # self.pnd_logger.info("Complated process = [NO: {}, PID: {}, TABLE: {}]".format(table["thread_no"], table["pid"], table["table_name"]))
            result_tables.append(table)
        results[idx] = result_tables
        self.pnd_logger.info("NO:[{:>2}] ({}/{}/{}) FAILED TABLES={}".format(result_tables[0]["THREAD_NO"], successed, total, failed, failed_tables))
        return

    def get_sampled_data(self, path_of_data, does_reload=False):         
        cm_unify = ConfigManager("unify")
        unify = CustomUnify(cm_unify)        
        pm = ProfileManager(self._source_conf['name'], "sampling")

        # if does_reload :
        #     self.pnd_logger.info("Sampling all reload tables.")
            # unify.dataset.truncate_dataset(self._source_conf['profileDatasetName'])
        
        all_tables = pm.getSampleTables()
        
        if len(all_tables) > 16 :
            thread_cnt = 16
        else :
            thread_cnt = len(all_tables)

        sample_tables = []
        thread_no = 0
        reverse = False
        for t in all_tables :            
            table = {"THREAD_NO": thread_no, "PID": 0, "DATABASE": t["DB_NAME"].upper(),"TABLE_NAME": t["TABLE_NAME"].upper(), "EXECUTE_QUERY": ""}
            # if self._source_conf['name'] == 'legacy' : 
            #     sample_query = self._source_conf['getDataQuery'].format(t["TABLE_NAME"].upper())
            #     if t["ROW_CNT"] > 100000 :                                        
            #         rate = "0."
            #         for i in range(2, len(str(t["ROW_CNT"]))) :
            #             rate = rate + "0"
            #         rate = rate + str(t["ROW_CNT"])[0]                    
            #         table["EXECUTE_QUERY"] = '{} where rand() <= {} distribute by rand() sort by rand() limit 100000'.format(sample_query, rate)
            #     else :
            #         table["EXECUTE_QUERY"] = sample_query

            if self._source_conf['name'] == 'legacy' :
                 table["EXECUTE_QUERY"] = self._source_conf['getDataQuery'].format(t["TABLE_NAME"])
            
            if reverse == False :
                thread_no = thread_no + 1
                if thread_no == thread_cnt :
                    reverse = True                    

            if reverse == True :
                thread_no = thread_no - 1
                if thread_no == -1 :
                    reverse = False
                    thread_no = 0

            sample_tables.append(table)
            self.pnd_logger.debug("{}".format(table))

        with Manager() as manager:            
            threads = []
            for idx in range(0, thread_cnt) :
                procs = []
                for proc in sample_tables :
                    if idx == proc["THREAD_NO"] :
                        procs.append(proc)
                threads.append(procs)

            p_list = []
            m_list = manager.list([0 for x in range(len(threads))])            
            for i in range(0, len(threads)) :
                proc = Process(target=self.profile, args=(threads[i],m_list,i))
                p_list.append(proc)
                time.sleep(1)
                proc.start()
            
            for p in p_list:
                p.join()                
                
            self.pnd_logger.info("Finish sampling.")
        return True
