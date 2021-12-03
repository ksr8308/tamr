import os
from abc import *

class Base(metaclass=ABCMeta) :

    @abstractmethod
    def _set_creds_(self):
        pass

    @abstractmethod
    def connect(self, username, password, hostname, port, servicename):
        pass
    
    @abstractmethod
    def disconnect(self) :
        pass

    @abstractmethod
    def execute(self, sql, bindvars=None, commit=False) :
        pass