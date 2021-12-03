#!/bin/python
import varyaml
import os
from custom_logger import CustomLogger
import pprint

class Creds:
    """
    This is a class for parsing all credentials from config file creds.yaml
    """
    def __init__(self, config_path):
        self._config_path = config_path
        self.creds = None

        # logger
        self.logger = CustomLogger("creds")

        self._parse_creds()

    def _parse_creds(self):
        if os.path.isfile(self._config_path + "/creds.yaml"):
            try:
                self.creds = varyaml.load(open(self._config_path + "/creds.yaml", "r"))
                #self.logger.info("Credentials have been loaded from {}".format(self._config_path+"/creds.yaml"))
            except Exception as e:
                self.logger.error(e)
        else:
            self.logger.error("creds.yaml file not found under {}".format(self._config_path))


if __name__ == "__main__":
    path_of_src = os.path.dirname(os.path.realpath(__file__))
    myCreds = Creds(os.path.abspath(path_of_src + "/../conf"))
    pprint.pprint(myCreds.creds)
