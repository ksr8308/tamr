#!/usr/bin/env python3
import os
import argparse
import csv
import re
from creds import Creds
from custom_logger import CustomLogger
from hive2_sampler import Hive2Sampler
from oracle_sampler import OracleSampler
#from dfconnect_sampler import DfConnectSampler
from data_preprocessor import DataPreprocessor
from dfconnect_multi_sampler import DfConnectMultiSampler
# from data_multi_preprocessor import DataMultiPreprocessor
from unify import Unify
from project_config import ProjectConfig

import sys
maxInt = sys.maxsize

while True:
    # decrease the maxInt value by factor 10
    # as long as the OverflowError occurs.

    try:
        csv.field_size_limit(maxInt)
        break
    except OverflowError:
        maxInt = int(maxInt/10)


def read_csv_header(logger, path_to_file):
    """
    Read header line of csv file
    :param logger: Logger
    :param path_to_file: Absolute path to csv file
    :return: Column names in a list if successful. Otherwise None.
    """
    column_names = None
    try:
        with open(path_to_file) as metadatafile:
            reader = csv.reader(metadatafile)
            column_names = next(reader)
    except Exception as e:
        logger.error(e)
    return column_names


def read_csv(logger, path_to_file):
    """
    Read content of csv file
    :param logger: Logger
    :param path_to_file: Absolute path to csv file
    :return: Iterator through the file, each row in a dictionary.
    """
    try:
        with open(path_to_file, "r") as metadatafile:
            reader = csv.reader(x.replace('\0', '') for x in metadatafile)
            # reader = csv.reader(re.sub(r'^null$', '', x.replace('\0', '')) for x in metadatafile)
            column_names = next(reader)
            for row in reader:
                result = {}
                for i in range(0, len(column_names)):
                    result[column_names[i]] = re.sub(r'(?i)(^null$|^none$)', '', row[i])
                yield result
    except Exception as e:
        logger.error(e)
        yield None


def update_input_datasets_for_project(logger, unify_client, project_config, project_id, path_of_output):
    """
    Update input datasets for project
    :param logger: logging.logger
    :param unify_client: Unify client
    :param project_config: Project config
    :param project_id: Project ID
    :param path_of_output: Path of output/ folder where the metadata input file lives
    :return: None
    """
    input_datasets = project_config["inputs"]
    project_name = project_config["name"]
    dataset_ids = []
    dataset_names = []
    for input_dataset in input_datasets:
        dataset_name = input_dataset["metadataFileName"]
        primary_key_column = input_dataset["primaryKeyColumnName"]
        column_names = read_csv_header(logger, path_of_output + '/' + dataset_name)
        if column_names is None or len(column_names) == 0:
            logger.error("Problem retrieving list of column names for {}".format(dataset_name))
            continue
        data = read_csv(logger, path_of_output + '/' + dataset_name)
        dataset_id, new_timestamp = unify_client.update_dataset(dataset_name, primary_key_column, column_names, data)
        if dataset_id is not None:
            dataset_ids.append(dataset_id)
            dataset_names.append(dataset_name)
    unify_client.add_datasets_to_project_if_new(dataset_ids, dataset_names, project_id, project_name)

def bootstrap_new_project(logger, unify_client, project_config, path_of_output):
    """
    Bootstrap a new project if not exists yet
    :param logger: logging.logger
    :param unify_client: Tamr Unify client
    :param project_config: Project config
    :param path_of_output: Path of output/ folder which the metadata input file lives
    :return: True if successful. Otherwise False.
    """
    # Create a new project
    new_project_id = unify_client.create_new_mastering_project(project_config["name"])
    if new_project_id:
        logger.info("Project '{}' created with ID {}".format(project_config["name"], new_project_id))
    else:
        return False
    # Add input datasets
    update_input_datasets_for_project(logger, unify_client, project_config, new_project_id, path_of_output)
    return True


def process_existing_project(logger, unify_client, project_config, project_id, path_of_output, does_reload=False):
    """
    Process existing project
    :param logger: logging.logger
    :param unify_client: Tamr Unify client
    :param project_config: Project config
    :param project_id: Project ID
    :param path_of_output: Absolute path to output/ folder
    :return: True if successful. Otherwise False.
    """
    # Update input datasets
    if does_reload:
        update_input_datasets_for_project(logger, unify_client, project_config, project_id, path_of_output)
    # run mastering
    if not myUnify.run_mastering(project_id, project_config["name"]):
        return False
    return True


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", help="help for subcommands")
    subparsers.required = True
    parser_sampling = subparsers.add_parser("sample", help="sample help")
    parser_sampling.add_argument("-m", "--mode", dest="mode", type=str, choices=["local", "connect"],
                                 required=True, default=None, help="sample data directly to local or via df-connect")
    parser_sampling.add_argument("-s", "--source", dest="source", type=str, choices=["hive", "oracle"],
                                 required=True, default=None, help="if sample from df-connect, specify the type of the "
                                                                   "source, oracle or hive (currently only supports "
                                                                   "oracle)")
    parser_sampling.add_argument("-n", "--name", dest="name", type=str, default=None, required=True,
                                 help="name of data source")
    parser_sampling.add_argument("-r", "--reload", dest="does_reload", help="reload all tables", action="store_true")
    parser_metadata = subparsers.add_parser("metadata", help="metadata help")
    parser_metadata.add_argument("-m", "--mode", dest="mode", type=str, choices=['local', 'connect'],
                                 required=True, default=None, help="generate metadata for local raw data or dataset "
                                                                   "on Unify sampled by df-connect")
    parser_metadata.add_argument("-s", "--source", dest="source", type=str, choices=["hive", "oracle"],
                                 default=None, help="if generate metadata for df-connect sampled dataset, "
                                 "specify the source, oracle or hive (currently only supports oracle)")
    parser_metadata.add_argument("-n", "--name", dest="name", help="name of data source", type=str, default=None)
    parser_metadata.add_argument("-d", "--dictionary", dest="dict", type=str, default=None,
                                 help="absolute path to token dictionary")
    parser_unify = subparsers.add_parser("unify", help="unify help")
    parser_unify.add_argument("-r", "--reload", dest="does_reload", action="store_true", help="reload input datasets")
    args = parser.parse_args()

    # get locations
    path_of_src = os.path.dirname(os.path.realpath(__file__))
    path_of_conf = os.path.abspath(path_of_src + "/../conf/")
    path_of_data = os.path.abspath(path_of_src + "/../data/")
    path_of_output = os.path.abspath(path_of_src + "/../output/")

    # mkdir if not exists
    if not os.path.exists(path_of_data):
        os.makedirs(path_of_data)
    if not os.path.exists(path_of_output):
        os.makedirs(path_of_output)

    # logger for main
    logger = CustomLogger("main")

    # parse credentials
    myCreds = Creds(path_of_conf)
    if myCreds.creds is None:
        exit(0)

    # parse config files
    myConfig = ProjectConfig(path_of_conf)
    if len(myConfig.project_configs) == 0:
        logger.error("No valid project configurations have been found")
        exit(0)

    # Sample data
    if args.command == 'sample':
        print("sampling ... ")
        if args.mode == 'local' and args.source == 'hive':
            source_config = [item for item in myCreds.creds['hive'] if item['name'] == args.name]
            if len(source_config) != 1:
                logger.error("None or more than one source found with the specified source name '{}'".format(args.name))
                exit(0)
            mySampler = Hive2Sampler(source_config[0])
        if args.mode == 'local' and args.source == 'oracle':
            source_config = [item for item in myCreds.creds['oracle'] if item['name'] == args.name]
            if len(source_config) != 1:
                logger.error("None or more than one source found with the specified source name '{}'".format(args.name))
                exit(0)
            mySampler = OracleSampler(source_config[0])
        if args.mode == 'connect':
            source_type = args.source
            if source_type == 'oracle':
                source_config = [item for item in myCreds.creds['oracle'] if item['name'] == args.name]
                if len(source_config) != 1:
                    logger.error("None or more than one source found with the specified source name '{}'".format(args.name))
                    exit(0)
                source_config = source_config[0]
                source_sampler = OracleSampler(source_config)
                jdbc_url = "jdbc:tamr:oracle://{}:{};ServiceName={}".format(
                    source_config['host'], source_config['port'], source_config['sid']
                )
                mySampler = DfConnectMultiSampler(source_sampler, myCreds.creds['unify'], source_config,
                                             jdbc_url, source_config['user'], source_config['pwd'])            
            if source_type == 'hive':
                source_config = [item for item in myCreds.creds['hive'] if item['name'] == args.name]
                if len(source_config) != 1:
                    logger.error("None or more than one source found with the specified source name '{}'".format(args.name))
                    exit(0)
                source_config = source_config[0]
                source_sampler = Hive2Sampler(source_config)
                jdbc_url = "jdbc:hive2://{}:{}".format(source_config['host'], source_config['port'])
                mySampler = DfConnectMultiSampler(source_sampler, myCreds.creds['unify'], source_config, jdbc_url, source_config['user'], source_config['pwd'])

        if not mySampler.get_sampled_data(path_of_data, args.does_reload):
            logger.info("Data sampling FAILED")
            exit(1)
        else:
            logger.info("Data sampling SUCCEEDED")

    # Generate metadata
    if args.command == 'metadata':        
        logger.info("Generate metadata")
        if args.dict is not None:
            logger.info("A token dictionary has been specified: {}".format(args.dict))
        if args.mode == 'local':
            myPreprocessor = DataPreprocessor(
                path_of_data,
                path_of_output,
                myCreds.creds['unify'],
                None,
                args.dict
            )
            if not myPreprocessor.process_local_files():
                logger.error("FAILED to generate metadata")
                exit(1)
            else:
                logger.info("Metadata was SUCCESSFULLY generated")
        elif args.mode == 'connect':            
            if args.source is None or args.name is None:
                logger.error("Missing required arguments for subcommand 'metadata' with mode 'connect'")
                logger.error(parser_metadata.format_help())
                parser_metadata.print_help()
                exit(0)
            source_config = [item for item in myCreds.creds[args.source] if item['name'] == args.name]            
            if len(source_config) != 1:
                logger.error("None or more than one source found with the specified source name '{}'".format(args.name))
                exit(0)
            source_config = source_config[0]
            myPreprocessor = DataMultiPreprocessor(
                path_of_data,
                path_of_output,
                myCreds.creds['unify'],
                source_config,
                args.dict
            )
            if not myPreprocessor.process_unify_dataset(args.source):
                logger.error("Failed to generate metadata")
                exit(1)
            else:
                logger.info("Metadata was SUCCESSFULLY generated")

    # Run Unify
    # if args.unify or args.all:
    if args.command == "unify":
        # Unify client
        myUnify = Unify(
            myCreds.creds["unify"]["protocol"],
            myCreds.creds["unify"]["hostname"],
            myCreds.creds["unify"]["port"],
            myCreds.creds["unify"]["grPort"],
            myCreds.creds["unify"]["user"],
            myCreds.creds["unify"]["pwd"]
        )

        # Existing projects
        existing_projects = myUnify.get_projects()
        existing_project_names = [project.name for project in existing_projects]
        logger.info("Found existing projects on server {}: {}"
                    .format(myCreds.creds["unify"]["hostname"], existing_project_names)
                    )

        # Process all projects
        for project_config in myConfig.project_configs:
            logger.info("Processing project '{}'".format(project_config["name"]))
            if project_config["name"] not in existing_project_names:
                logger.info(
                    "Bootstrap new project '{}' since it does not exist yet".format(project_config["name"]))
                if not bootstrap_new_project(logger, myUnify, project_config, path_of_output):
                    logger.error("ERROR: Failed to create new project '{}'".format(project_config["name"]))
                else:
                    logger.info("Project '{}' has been initialized with input datasets added. "
                                "Please go to Unify UI to finish the rest of the project setup.".format(
                        project_config["name"]))
            else:
                logger.info("Update existing project '{}'".format(project_config["name"]))
                project_ids = \
                    [project.relative_id.split("/")[1] for project in existing_projects
                     if project.name == project_config["name"]]
                if not process_existing_project(logger, myUnify, project_config, project_ids[0], path_of_output, args.does_reload):
                    logger.error("ERROR: Failed to update existing project '{}'".format(project_config["name"]))
                else:
                    logger.info("Project '{}' has been updated".format(project_config["name"]))

