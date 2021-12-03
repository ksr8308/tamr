#!/bin/python
import os
import sys
import glob
import yaml
import pprint
import varyaml
from pnd_logger import PndLogger

class QueriesConfig:
    """
    This is a class for parsing project configuration files
    """

    def __init__(self, config_path=None, logger_name=None):
        # logger
        self.__logger = PndLogger("Query Config Class", logger_name)
        if config_path == None :
            self.__config_path = os.path.abspath(os.path.dirname(os.path.realpath(__file__)) + "/../conf")
        else : 
            self.__config_path = config_path

        self.__queries_filename = "/queries.yaml"
        self.queries = None
        self.__parse_configs()        

    def __parse_configs(self):
        if os.path.isfile(self.__config_path + self.__queries_filename):
            try:
                self.queries = varyaml.load(open(self.__config_path + self.__queries_filename, "r"))
                self.__logger.info("Credentials have been loaded from {}".format(self.__config_path + self.__queries_filename))
            except Exception as e:
                self.__logger.error(e)
        else:
            self.__logger.error("{} file not found under {}".format(self.__queries_filename, self.__config_path))

if __name__ == "__main__":
    path_of_src = os.path.dirname(os.path.realpath(__file__))
    myQueriesConfig = QueriesConfig(path_of_src + "/../conf")
    pprint.pprint(myQueriesConfig.queries)