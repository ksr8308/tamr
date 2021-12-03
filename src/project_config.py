#!/bin/python
import os
import glob
import yaml
import pprint
from custom_logger import CustomLogger


class ProjectConfig:
    """
    This is a class for parsing project configuration files
    """

    def __init__(self, config_path="./conf"):
        self.config_path = config_path
        self.project_configs = []

        # logger
        self.logger = CustomLogger("projectConfig")

        self._load_configs()

    def _load_configs(self):
        #self.logger.info("Reading config files from {}".format(self.config_path))
        # config_files = glob.glob(self.config_path + "/schema-discovery-project.yaml")
        # self.logger.info("Found configuration files {}".format(config_files))
        # for config_file in config_files:
        #     if "creds.yaml" in config_file:
        #         continue
        #     self.project_configs.append(self._parse_config(config_file))
        path_of_config_file = self.config_path + "/schema-discovery-project.yaml"
        self.project_configs = self._parse_config(path_of_config_file)

    @staticmethod
    def _parse_config(config_file):
        config_dict = yaml.load(open(config_file, "r"))
        return config_dict


if __name__ == "__main__":
    path_of_src = os.path.dirname(os.path.realpath(__file__))
    myProjectConfig = ProjectConfig(path_of_src + "/../conf")
    pprint.pprint(myProjectConfig.project_configs)

