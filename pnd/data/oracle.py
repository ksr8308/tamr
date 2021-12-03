import os
import sys
import cx_Oracle
import logging
import datetime
from base import Base

current_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(current_path + '/../config')
from config_manager import ConfigManager
from pnd_logger import PndLogger

os.putenv('NLS_LANG', 'KOREAN_KOREA.AL32UTF8')

class Oracle(Base):
    def __init__(self, conf_manager, logger_name=None) :
        self.__logger = PndLogger("Oracle Database Class", logger_name)
        self.creds = None
        self._set_creds_(conf_manager.creds)
        self.__cursor = None
            
    def _set_creds_(self, creds) :        
        try:
            if type(creds) == dict :
                self.creds = creds

            if type(creds) == list :
                self.creds = creds[0]

            # self.creds = creds[0] if len(creds) > 0 else creds
        except Exception as e:
            self.__logger.error("Failed to configure creds. - {}".format(e))
        return

    def connect(self, username=None, password=None, host=None, port=None, sid=None):
        try:
            if username == None : username = self.creds['user']
            if password == None : password = self.creds['pwd']
            if host == None : host = self.creds['host']
            if port == None : port = self.creds['port']
            if sid == None : sid = self.creds['sid']

            self.__db = cx_Oracle.connect(username, password, host + ":" + port + "/" + sid)
        except cx_Oracle.DatabaseError as e:
            error, = e.args
            if error.code == 1017:
                self.__logger.error("Please check your credentials.")
            else:
                self.__logger.error("Database connection error. - {}".format(e))
            raise
            self.disconnect()
        self.__cursor = self.__db.cursor()        

    def disconnect(self):
        try:
            self.__cursor.close()
            self.__db.close()
        except cx_Oracle.DatabaseError:
            pass
        return

    def execute_proc(self, sql):
        try:            
            self.__cursor.callproc(sql, [])            
        except cx_Oracle.DatabaseError as e:            
            error, = e.args
            self.__logger.error(error.message)
            raise
        return

    def execute(self, sql, bindvars=None, insert_once=False, commit=True):        
        total_cnt = 0 if bindvars == None else len(bindvars)
        success_cnt = 0
        failed_cnt = 0
        try:
            if total_cnt != 0 :
                self.__cursor.prepare(sql)
                if insert_once :
                    interval = 10000
                    rows_set = [bindvars[i * interval:(i + 1) * interval] for i in range((len(bindvars) + interval - 1) // interval)]
                    self.__logger.debug("// = {} % = {}".format(total_cnt // interval, total_cnt % interval))
                    self.__logger.debug("interval = {} iterate count = {}".format(interval, len(rows_set)))

                    for set in rows_set :
                        try :
                            self.__cursor.executemany(None, set)
                            if commit: 
                                self.__db.commit()
                                success_cnt += len(set)
                        except :                            
                            for var in set :
                                try :
                                    self.__cursor.execute(sql, var)
                                    if commit: 
                                        self.__db.commit()
                                        success_cnt += 1
                                except Exception as e:
                                    self.__logger.error(e)
                                    self.__logger.error(var)
                                    failed_cnt += 1                                        
                        self.__logger.info("Inserted rows {}".format(success_cnt))
                else :
                    current_cnt = 0
                    logging_interval = 10000 if total_cnt > 100000 else 1000
                    for var in bindvars :
                        self.__logger.debug(var)
                        try :                            
                            self.__cursor.execute(sql, var)
                            if commit: 
                                self.__db.commit()
                                success_cnt += 1                            
                        except Exception as e :
                            self.__logger.error(e)
                            self.__logger.error(var)
                            failed_cnt += 1
                        
                        if current_cnt != 0 and current_cnt % logging_interval == 0 : 
                            self.__logger.info("Inserted rows {}".format(current_cnt))

                        current_cnt += 1
                self.__logger.info("Total = {}    Successed = {}   Failed = {}".format(total_cnt, success_cnt, failed_cnt))
            else :
                self.__cursor.execute(sql)
                if self.__cursor.description != None :
                    self.__cursor.rowfactory = self.__factory(self.__cursor)
                    if commit: 
                        self.__db.commit()
                    return self.__cursor.fetchall()
                
        except cx_Oracle.DatabaseError as e:            
            error, = e.args
            if error.code == 955:
                self.__logger.error("Table already exists")
            elif error.code == 1031:
                self.__logger.error("Insufficient privileges")
            self.__logger.error(error.message)
            raise
        return

    def __factory(self, cursor) :
        columnNames = [d[0] for d in cursor.description]
 
        def createRow(*args):
            return dict(zip(columnNames, args))
        return createRow