import os
import sys
import logging
import datetime
from base import Base
from pyhive import hive
from pyhive.exc import OperationalError

current_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(current_path + '/../config')
from config_manager import ConfigManager
from pnd_logger import PndLogger

#os.putenv('NLS_LANG', 'KOREAN_KOREA.AL32UTF8')

class Hive2(Base):
    def __init__(self, conf_manager, logger_name=None) :
        self.__logger = PndLogger("Hive2 Database Class", logger_name)                
        self.creds = None
        self._set_creds_(conf_manager.creds)
        self.__cursor = None

    def _set_creds_(self, creds) :
        try:
            if type(creds) == dict :
                self.creds = creds

            if type(creds) == list :
                self.creds = creds[0]                
            self.__logger.debug("Configure to creds SUCCESSED.")
        except Exception as e:
            self.__logger.error("Failed to configure creds. - {}".format(e))
        return

    def connect(self, username=None, password=None, host=None, port=None, sid=None, auth=None):
        try:
            # self.__logger.debug(self.creds)
            if username == None : username = self.creds['user']            
            if password == None : password = self.creds['pwd']
            if host == None : host = self.creds['host']
            if port == None : port = self.creds['port']
            if sid == None : sid = self.creds['sid']
            if auth == None : auth = self.creds['authMechanism']
            
            self.__db = hive.Connection(host=host, port=port, username=username, password=password, auth=auth)
        except OperationalError as e:
            self.__logger.error(e.args)
            self.disconnect()
        self.__cursor = self.__db.cursor()        

    def disconnect(self):
        try:
            self.__cursor.close()
            self.__db.close()
        except OperationalError as e:
            pass
        return

    def execute_proc(self, sql):
        try:            
            self.__cursor.callproc(sql, [])            
        except OperationalError as e:            
            self.__logger.error(e)
            raise
        return

    def execute(self, sql):
        # self.__logger.info(sql)
        try:
            results = {'data': [], 'header': [] }
            self.__cursor.execute(sql)
            records = []
            results = []            
            try:
                records = self.__cursor.fetchall()
            except OperationalError as e:
                self.__logger.error('no record returned')            
            if records:                
                for row in records :
                    results.append(''.join(map(str, row)))
            return results
        except OperationalError as e:
            self.__logger.error("Error query is [{}]".format(sql))
            self.__logger.debug(e)
            raise
        return