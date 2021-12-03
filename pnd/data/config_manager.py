#!/bin/python
import os
import sys
import varyaml
import logging
from queries_config import QueriesConfig

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + '/../../src')
from creds import Creds
from project_config import ProjectConfig
from custom_logger import CustomLogger

class ConfigManager:
    def __init__(self, conf_name):
        # logger        
        self.__logger = CustomLogger("Config Manager Class")        
        self.__config_path = os.path.abspath(os.path.dirname(os.path.realpath(__file__)) + "/../conf")
        
        self.__config_manager = None
        self.__parse_config()
        
        self.__config_name = conf_name
        self.cred_cat = None
        self.cred_src = None
        self.query_src = None
        self.db_type = None
        self.proj_nm = None
        self.proj_gr_nm = None
        self.__set_config_manager(self.__config_name)

        self.creds = None
        self.__set_creds()
        
        self.queries = None
        if self.db_type != None and self.db_type != "" :
            self.__set_queries()
    
    def __parse_config(self):
        if os.path.isfile(self.__config_path + "/config-manager.yaml"):
            try:
                self.__config_manager = varyaml.load(open(self.__config_path + "/config-manager.yaml", "r"))
                self.__logger.info("Credentials have been loaded from {}".format(self.__config_path+"/config-manager.yaml"))
            except Exception as e:
                self.__logger.error(e)
        else:
            self.__logger.error("config-manager.yaml file not found under {}".format(self.__config_path))

    def __set_config_manager(self, conf_name) :
        try:
            self.cred_cat = '' if 'cred_cat' not in self.__config_manager[conf_name][0] else self.__config_manager[conf_name][0]["cred_cat"]
            self.cred_src = '' if 'cred_src' not in self.__config_manager[conf_name][0] else self.__config_manager[conf_name][0]["cred_src"]
            self.query_src = '' if 'query_src' not in self.__config_manager[conf_name][0] else self.__config_manager[conf_name][0]["query_src"]
            self.db_type = '' if 'database_type' not in self.__config_manager[conf_name][0] else self.__config_manager[conf_name][0]["database_type"]
            self.proj_nm = '' if 'project_name' not in self.__config_manager[conf_name][0] else self.__config_manager[conf_name][0]["project_name"]
            self.proj_gr_nm = '' if 'project_gr_name' not in self.__config_manager[conf_name][0] else self.__config_manager[conf_name][0]["project_gr_name"]
            self.__logger.info("Configure to config manager SUCCESSED. [category = '{}' | source = '{}' | query = '{}' | databaseType = '{}' | project = '{}']".format(self.cred_cat, self.cred_src, self.query_src, self.db_type, self.proj_nm))
        except Exception as e:
            self.__logger.error("Failed to configure config manager. - {}".format(e))
        return

    def __set_creds(self) : 
        try :
            creds = Creds(os.path.abspath(os.path.dirname(os.path.realpath(__file__)) + "/../../conf")).creds
            for name in creds :
                if self.cred_cat == name :
                    self.creds = creds[name]                    
                    break
            self.__logger.info("Configure to creds SUCCESSED.")
        except Exception as e :
            self.__logger.error("Failed to configure creds. - {}".format(e))
        return

    def __set_queries(self):
        try:
            queries = QueriesConfig().queries[self.db_type]
            idx = 0
            for query in queries :            
                if self.query_src == query['source'] :                   
                    self.queries = query
                    break
                idx += 1
            self.__logger.debug("QUERY = [{}]".format(self.queries))
            self.__logger.info("Configure to queries SUCCESSED.")
        except Exception as e:
            self.__logger.error("Failed to configure queries. - {}".format(e))
        return

    def get_config_manager_by_source(self, configs, name) :
        try:
            idx = 0
            for config in configs :
                if configs['source'] == name :
                    self.__logger.debug("CONFIG = [{}]".format(configs['source']))
                    self.__logger.info("Configure queries to SUCCESSED.")
                    return configs[idx]
                idx += 1
        except Exception as e:
            self.__logger.error("Faile to configure config manager by source name. - {}".format(e))
        return

    def get_creds_by_source(self, creds, name) :
        try :
            self.__logger.debug(creds)
            idx = 0
            for cred in creds :
                if cred['name'] == name :
                    self.__logger.info("Configure to cred SUCCESSED.")
                    return creds[idx]
                idx += 1
        except Exception as e:
            self.__logger.error("Faile to configure cred by source name. - {}".format(e))
        return

    def get_queries_by_source(self, queries, name) :
        try :
            self.__logger.debug("QUERY BY SOURCE = [{}]".format(query))
            self.__logger.info("Configure to queries SUCCESSED.")
            return queries[name]
        except Exception as e:
            self.__logger.error("Faile to configure queries by source name. - {}".format(e))
        return

if __name__ == "__main__":
    path_of_src = os.path.dirname(os.path.realpath(__file__))
    configManager = ConfigManager(os.path.abspath(path_of_src + "/../conf"))
    print(configManager.config_manager)