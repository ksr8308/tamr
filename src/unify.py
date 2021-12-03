#!/bin/python
import requests
import json
import base64
import datetime
import time
from dateutil.tz import tzlocal
import tamr_unify_client as api
from tamr_unify_client.auth import UsernamePasswordAuth
from custom_logger import CustomLogger


class Unify:
    """
    This is a class for handling all operations to Tamr Unify
    """
    def __init__(self, protocol, hostname, port, gr_port, user, pwd):
        self._protocol = protocol
        self._hostname = hostname
        self._port = port
        self._gr_port = gr_port
        self._baseUrl = self._protocol + "://" + self._hostname + ":" + self._port
        self._basicCreds = self._basic_auth_str(user, pwd)
        auth = UsernamePasswordAuth(user, pwd)
        self.unify = api.Client(auth, host=self._hostname, protocol=self._protocol, port=self._port)
        self.logger = CustomLogger("unify")

    @staticmethod
    def _basic_auth_str(username, password):
        """Constructs a 'BasicCreds' string for an HTTP request header."""
        auth = '{}:{}'.format(username, password)
        encoded = base64.b64encode(auth.encode('latin1'))
        return 'BasicCreds ' + requests.utils.to_native_string(encoded.strip())

    def _get_job_status(self, job_id):
        """
        Get job status
        :param job_id: Job ID
        :return: Status
        """
        url = self._baseUrl + "/api/job/jobs/{}".format(job_id)
        headers = {"Accept": "application/json", 'Authorization': self._basicCreds}
        try:
            response = requests.get(url, headers=headers)
            if response.status_code not in [200, 201, 202]:
                self.logger.error(response.text)
                return 'FAILED'
            else:
                return json.loads(response.text)['data']['status']['state']
        except Exception as e:
            self.logger.error(e)
            return 'FAILED'

    def _job_finishes_ok(self, job_id):
        """
        Check if job finished successfully
        :param job_id: Job ID
        :return: True if job finished successfully. Otherwise False.
        """
        while True:
            job_status = self._get_job_status(job_id)
            if job_status == 'PENDING' or job_status == 'RUNNING':
                time.sleep(5)
                continue
            elif job_status == 'SUCCEEDED':
                return True
            elif job_status == 'CANCELED':
                self.logger.error("Job '{}' failed".format(job_id))
                return False
            elif job_status == 'FAILED':
                self.logger.error("Job '{}' failed.".format(job_id))
                return False

    def get_projects(self):
        """
        Get all existing project names and IDs on the server
        :return: list of json objects each with project name and ID
        """
        return self.unify.projects

    def get_datasets(self):
        """
        Get all dataset present on the instance
        :return: List of dataset objects if successful. Otherwise None.
        """
        url = self._baseUrl + "/api/versioned/v1/datasets"
        headers = {"Accept": "application/json", "Authorization": self._basicCreds}
        try:
            response = requests.get(url, headers=headers)            
            if response.status_code == 200:                
                return json.loads(response.text)
            else:
                self.logger.error("Problem getting datasets")
                return None
        except Exception as e:            
            self.logger.error(e)
            self.logger.error("Problem getting datasets")
            return None

    def get_dataset_last_modified_time(self, dataset_name):
        """
        Get the last modified time for dataset
        :param dataset_name: Dataset name
        :return: Datetime if successful. Otherwise None
        """
        all_datasets = self.get_datasets()
        queried_dataset = [dataset for dataset in all_datasets if dataset["name"] == dataset_name]
        if len(queried_dataset) == 0:
            self.logger.error("Dataset '{}' can not be found".format(dataset_name))
            return None
        else:
            return datetime.datetime.strptime(
                queried_dataset[0]["lastModified"]["time"],
                "%Y-%m-%dT%H:%M:%S.%fZ"
            ).replace(tzinfo=tzlocal())

    def get_dataset_by_id(self, dataset_id):
        """
        Get dataset details by ID
        :param dataset_id: Dataset ID
        :return: Dataset details in JSON
        """
        url = self._baseUrl + "/api/dataset/datasets/{}".format(dataset_id)
        headers = {"Accept": "application/json", "Authorization": self._basicCreds}
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                return json.loads(response.text)
            else:
                self.logger.error("Problem getting dataset {}".format(dataset_id))
                return None
        except Exception as e:
            self.logger.error(e)
            self.logger.error("Problem getting dataset {}".format(dataset_id))
            return None

    def get_recipes(self):
        """
        Get all recipes on the instance
        :return: List of recipes if successful. Otherwise None.
        """
        url = self._baseUrl + "/api/recipe/recipes/all"
        headers = {"Accept": "application/json", "Authorization": self._basicCreds}
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                return json.loads(response.text)
            else:
                self.logger.error("Problem getting all recipes")
                return None
        except Exception as e:
            self.logger.error(e)
            self.logger.error("Problem getting all recipes")
            return None

    def get_project_by_id(self, project_id):
        """
        Get project info by ID
        :param project_id: ID of project
        :return: Project info json object if successful. Otherwise None.
        """
        url = self._baseUrl + "/api/recipe/projects/{}".format(project_id)
        headers = {"Accept": "application/json", "Authorization": self._basicCreds}
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                return json.loads(response.text)
            else:
                self.logger.error("Problem getting project by ID {}".format(project_id))
                return None
        except Exception as e:
            self.logger.error(e)
            self.logger.error("Problem getting project by ID {}".format(project_id))
            return None

    def enable_transformations(self, project_id):
        """
        Enable transformations for mastering project
        :param project_id: Project ID
        :return: True if successful. Otherwise False.
        """
        project_obj = self.get_project_by_id(project_id)
        if project_id:
            version_num = project_obj["lastModified"]["version"]
            project_data = project_obj["data"]
            project_data["metadata"]["enableTransformations"] = True
            url = self._baseUrl + "/api/recipe/projects/{}?version={}".format(project_id, version_num)
            headers = {"Content-Type": "application/json", "Accept": "application/json", "Authorization": self._basicCreds}
            try:
                response = requests.put(url, headers=headers, data=json.dumps(project_data))
                if response.status_code == 200:
                    return True
                else:
                    self.logger.error("Failed to enable transformations for project '{}'".format(project_id))
                    return False
            except Exception as e:
                self.logger.error(e)
                self.logger.error("Failed to enable transformations for project '{}'".format(project_id))
                return False

    def add_attribute_configuration(self, project_id, attr_name, function, is_for_ml, tokenizer):
        """
        Add Unified Attribute to project
        :param project_id: Project ID
        :param attr_name: Unified Attribute name
        :param function: Similarity function
        :param is_for_ml: Boolean to indicate whether to be included for machine learning
        :param tokenizer: Tokenizer
        :return: True if successful. Otherwise False.
        """
        url = self._baseUrl + "/api/versioned/v1/projects/{}/attributeConfigurations".format(project_id)
        headers = {"Content-Type": "application/json", "Accept": "application/json", "Authorization": self._basicCreds}
        data = {"attributeName": attr_name,
                "similarityFunction": function,
                "enabledForMl": is_for_ml,
                "tokenizer": tokenizer
                }
        try:
            response = requests.post(url, headers=headers, data=json.dumps(data))
            if response.status_code == 201:
                return True
            else:
                self.logger.error("Problem adding attribute configuration '{}' for project '{}'".format(
                    data, project_id
                ))
                return False
        except Exception as e:
            self.logger.error(e)
            self.logger.error("Problem adding attribute configuration '{}' for project '{}'".format(
                data, project_id
            ))
            return False

    def create_new_mastering_project(self, project_name):
        """
        Create a new mastering project with the specified project name
        :param project_name: Name of new project
        :return: New project Id
        """
        # Create the project
        url = self._baseUrl + "/api/versioned/v1/projects"
        headers = {"Content-Type": "application/json", "Accept": "application/json", "Authorization": self._basicCreds}
        project_config = [{
            "name": project_name,
            "description": project_name,
            "type": "DEDUP",
            "unifiedDatasetName": project_name+"_unified_dataset"
        }]
        relative_project_id = None
        try:
            response = requests.post(url, headers=headers, data=json.dumps(project_config))
            if response.status_code not in [200, 201, 202]:
                self.logger.error("Problem creating new project {}: {}".format(project_name, response.text))
            else:
                response_obj = json.loads(response.text)
                relative_project_id = response_obj["relativeId"]
        except Exception as e:
            self.logger.error(e)
            self.logger.error("ERROR: Problem creating new project")
        if relative_project_id is None:
            return None
        project_id = relative_project_id.split("/")[1]
        # Enable transformations
        self.enable_transformations(project_id)
        # Create required Unified Attributes:
        #   table_name, column_name, column_name_tokenized, column_name_tokenized_standardized,
        #   business_type, keys, top_n_values
        self.add_attribute_configuration(project_id, "table_name", "COSINE", False, "DEFAULT")
        self.add_attribute_configuration(project_id, "column_name", "COSINE", True, "BIGRAM")
        self.add_attribute_configuration(project_id, "column_name_tokenized", "COSINE", True, "DEFAULT")
        self.add_attribute_configuration(project_id, "column_name_tokenized_std", "COSINE", True, "DEFAULT")
        self.add_attribute_configuration(project_id, "business_type", "COSINE", True, "DEFAULT")
        self.add_attribute_configuration(project_id, "keys", "COSINE", True, "DEFAULT")
        self.add_attribute_configuration(project_id, "top_n_values", "COSINE", True, "DEFAULT")
        return project_id

    def dataset_exists(self, dataset_name):
        """
        Checks if dataset exists
        :param dataset_name: Name of dataset
        :return: Dataset ID if exists, otherwise -1. None if query fails.
        """
        url = self._baseUrl + "/api/dataset/datasets/named/{}".format(dataset_name)
        headers = {"Accept": "application/json", "Authorization": self._basicCreds}
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                response_obj = json.loads(response.text)
                return response_obj["documentId"]["id"]
            else:
                return -1
        except Exception as e:
            self.logger.error(e)
            return None

    def create_dataset(self, dataset_name, primary_key_field, column_names):
        """
        Create new dataset with the specified properties
        :param dataset_name: Name
        :param primary_key_field: Primary key field
        :param column_names: Names of fields
        :return: Dataset relative ID of the format 'datasets/*' if successful. Otherwise None.
        """
        url = self._baseUrl + "/api/versioned/v1/datasets"
        headers = {"Content-Type": "application/json", "Accept": "application/json", "Authorization": self._basicCreds}
        dataset_config = {"name": dataset_name, "keyAttributeNames": [primary_key_field], "description": "column metadata"}
        try:
            response = requests.post(url, headers=headers, data=json.dumps(dataset_config))
            if response.status_code in [200, 201, 202]:
                response_obj = json.loads(response.text)
                dataset_relative_id = response_obj["relativeId"]
                self.logger.info("Dataset '{}' created with ID {}".format(dataset_name, dataset_relative_id))
            else:
                self.logger.info("Problem creating dataset {}: {}".format(dataset_name, response.text))
                return None
        except Exception as e:
            self.logger.error(e)
            self.logger.error("Problem creating dataset {}".format(dataset_name))
            return None
        # add attributes
        dataset_id = dataset_relative_id.split("/")[1]
        url = self._baseUrl + "/api/versioned/v1/datasets/{}/attributes".format(dataset_id)
        for column_name in column_names:
            if column_name == primary_key_field:
                continue
            attr_config = {"name": column_name, "description": "", "type": {"baseType": "STRING"}}
            try:
                response = requests.post(url, headers=headers, data=json.dumps(attr_config))
                if response.status_code not in [200, 201, 202]:
                    self.logger.error("Problem creating attribute {} for dataset {}".format(column_name, dataset_name))
                    return None
            except Exception as e:
                self.logger.error(e)
                self.logger.error("Problem creating attribute {} for dataset {}".format(column_name, dataset_name))
                return None
        return dataset_relative_id

    def update_dataset_schema(self, dataset_name, column_names):
        """
        Update dataset schema if any changes. New columns will be added. Existing columns will be all preserved. No
        column deletion will be done.
        :param dataset_name: Dataset name
        :param column_names: Column names as in Snowflake
        :return: True if any changes have been made. Otherwise False.
        """
        dataset_id = self.dataset_exists(dataset_name)
        if dataset_id == -1:
            return False
        dataset_details = self.get_dataset_by_id(dataset_id)
        existing_columns = dataset_details["data"]["fields"]
        new_columns = []
        for column_name in column_names:
            if column_name not in existing_columns:
                new_columns.append(column_name)
        if len(new_columns) > 0:
            for new_column_name in new_columns:
                url = self._baseUrl + "/api/versioned/v1/datasets/{}/attributes".format(dataset_id)
                headers = {"Content-Type": "application/json", "Accept": "application/json", "Authorization": self._basicCreds}
                data = {"name": new_column_name, "description": "", "type": {"baseType": "STRING"}}
                response = requests.post(url, headers=headers, data=json.dumps(data))
                if response.status_code != 201:
                    self.logger.error("Problem adding new attribute '{}' for dataset '{}'".format(
                        new_column_name, dataset_name
                    ))
                    self.logger.error(response.text)
                    return False
                else:
                    self.logger.info("Added new attribute '{}' for dataset '{}'".format(
                        new_column_name, dataset_name
                    ))
            return True
        return True

    def update_dataset(self, dataset_name, primary_key_column, column_names, data):
        """
        Update dataset with data from Snowflake. If dataset does not exist yet, create it.
        :param dataset_name: Name of dataset
        :param primary_key_column: Column to be used as the unique ID
        :param column_names: Names of fields
        :param data: Data to be updated or created. List of dictionary objects each as one row of data.
        :return: Dataset ID if successful. Otherwise None
        """
        # check if dataset already exists
        if primary_key_column not in column_names:
            self.logger.error("Primary key column '{}' not found in dataset '{}'"
                              .format(primary_key_column, dataset_name))
            return None, None
        dataset_id = self.dataset_exists(dataset_name)
        if dataset_id == -1:
            self.logger.info("Dataset '{}' does not exist yet. Creating new dataset ...".format(dataset_name))
            dataset_relative_id = self.create_dataset(dataset_name, primary_key_column, column_names)
            if dataset_relative_id:
                return self.update_dataset(dataset_name, primary_key_column, column_names, data)
            else:
                self.logger.error("Failed to create dataset {}".format(dataset_name))
                return None, None
        elif dataset_id:
            self.logger.info("Updating dataset '{}' ...".format(dataset_name))
            if not self.update_dataset_schema(dataset_name, column_names):
                self.logger.error("Failed to update schema of {}".format(dataset_name))
                return None, None
            url = self._baseUrl + "/api/versioned/v1/datasets/{}:updateRecords?header=false".format(dataset_id)
            headers = {"Content-Type": "application/json", "Accept": "application/json", "Authorization": self._basicCreds}
            payload = []
            for record in data:
                if record is None:
                    continue
                payload.append({"action": "CREATE", "record": record, "recordId": record[primary_key_column]})
                if len(payload) == 10000:
                    try:
                        response = requests.post(
                            url,
                            headers=headers,
                            data="\n".join([json.dumps(command) for command in payload])
                        )
                        if response.status_code not in [200, 201, 202]:
                            self.logger.error("Problem updating dataset {}: {}".format(dataset_name, response.text))
                            return None, None
                        self.logger.info("{} records updated".format(len(payload)))
                    except Exception as e:
                        self.logger.error(e)
                        self.logger.error("Problem updating dataset {}".format(dataset_name))
                        return None, None
                    payload.clear()
            if len(payload) > 0:
                try:
                    response = requests.post(
                        url,
                        headers=headers,
                        data="\n".join([json.dumps(command) for command in payload])
                    )
                    if response.status_code not in [200, 201, 202]:
                        self.logger.error("Problem updating dataset {}: {}".format(dataset_name, response.text))
                        return None, None
                    self.logger.info("{} records updated".format(len(payload)))
                except Exception as e:
                    self.logger.error(e)
                    self.logger.error("Problem updating dataset {}".format(dataset_name))
                    return None, None
            self.logger.info("Dataset '{}' with ID {} updated".format(dataset_name, dataset_id))
            return dataset_id, None
        else:
            return None, None

    def get_input_datasets_for_project(self, project_id):
        """
        Get list of input datasets for project
        :param project_id: Project ID
        :return: List of input dataset if successful. Otherwise None.
        """
        url = self._baseUrl + "/api/versioned/v1/projects/{}/inputDatasets".format(project_id)
        headers = {"Accept": "application/json", "Authorization": self._basicCreds}
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                return json.loads(response.text)
            else:
                self.logger.error("Problem getting input datasets for project with ID {}".format(project_id))
                return None
        except Exception as e:
            self.logger.error(e)
            return None

    def add_dataset_to_project(self, dataset_id, project_id):
        """
        Add dataset as input for project
        :param dataset_id: Dataset ID
        :param project_id: Project ID
        :return: True if successful. Otherwise False.
        """
        url = self._baseUrl + "/api/versioned/v1/projects/{}/inputDatasets".format(project_id)
        headers = {"Content-Type": "application/json", "Accept": "application/json", "Authorization": self._basicCreds}
        try:
            response = requests.post(url+"?id={}".format(dataset_id), headers=headers)
            if response.status_code != 204:
                self.logger.error(response.text)
                self.logger.error("Problem adding dataset {} to project {}".format(dataset_id, project_id))
                return False
        except Exception as e:
            self.logger.error(e)
            self.logger.error("Problem adding dataset {} to project {}".format(dataset_id, project_id))
            return False
        return True

    def map_required_attributes(self, dataset_name, project_id, project_name):
        """
        Map required Unified Attributes
        :param dataset_name: Dataset name
        :param project_id: Project ID
        :param project_name: Project name
        :return: None
        """
        url = self._baseUrl + "/api/versioned/v1/projects/{}/attributeMappings".format(project_id)
        headers = {"Content-Type": "application/json", "Accept": "application/json", "Authorization": self._basicCreds}
        attributes = [
            "table_name",
            "column_name",
            "column_name_tokenized",
            "column_name_tokenized_std",
            "business_type",
            "keys",
            "top_n_values"
        ]
        # data2 = {"inputDatasetName": dataset_name,
        #          "inputAttributeName": "ODS_SRC_SYSTEM_ID",
        #          "unifiedDatasetName": project_name+"_unified_dataset",
        #          "unifiedAttributeName": "ODS_SRC_SYSTEM_ID"}
        for attribute in attributes:
            try:
                response = requests.post(url, headers=headers, data=json.dumps(
                    {
                        "inputDatasetName": dataset_name,
                        "inputAttributeName": attribute,
                        "unifiedDatasetName": project_name+"_unified_dataset",
                        "unifiedAttributeName": attribute
                    }
                ))
                if response.status_code not in [201, 204]:
                    self.logger.error(
                        "Problem adding mapping for attribute {} in project {}".format(attribute, project_name)
                    )
            except Exception as e:
                self.logger.error(e)

    def add_datasets_to_project_if_new(self, dataset_ids, dataset_names, project_id, project_name):
        """
        Add datasets to project if not associated with project yet
        :param dataset_ids: The IDs of datasets to be added
        :param project_id: Project ID
        :return: None
        """
        input_datasets = self.get_input_datasets_for_project(project_id)
        existing_dataset_ids = [dataset["relativeId"].split("/")[1] for dataset in input_datasets]
        for i in range(0, len(dataset_ids)):
            dataset_id = dataset_ids[i]
            dataset_name = dataset_names[i]
            if str(dataset_id) not in existing_dataset_ids:
                self.logger.info("Adding dataset {} to project {}".format(dataset_id, project_id))
                if not self.add_dataset_to_project(dataset_id, project_id):
                    self.logger.error("Problem adding dataset")
                else:
                    self.map_required_attributes(dataset_name, project_id, project_name)

    def refresh_golden_record_dataset(self, gr_project_name):
        """
        Refresh the golden records dataset
        :param gr_project_name: GR Project name
        :return: True if successful. Otherwise False.
        """
        all_recipes = self.get_recipes()
        if all_recipes is not None:
            gr_recipe = [recipe for recipe in all_recipes if recipe["data"]["name"] == gr_project_name+"-GOLDEN_RECORDS"]
            if len(gr_recipe) == 1:
                gr_module_id = gr_recipe[0]["data"]["metadata"]["resultingFromModule"]
                url = self._protocol + "://" + self._hostname + ":" + self._port + \
                      "/api/recipe/modules/{}/job/indexDraft".format(gr_module_id)
                headers = {"Content-Type": "application/json", "Accept": "application/json", "Authorization": self._basicCreds}
                try:
                    response = requests.post(url, headers=headers)
                    if response.status_code == 200:
                        response_obj = json.loads(response.text)
                        if response_obj["documentId"] is None:
                            return True
                        else:
                            job_id = response_obj["documentId"]["id"]
                            return self._job_finishes_ok(job_id)
                except Exception as e:
                    self.logger.error(e)
                    self.logger.error("Problem refreshing golden records for project '{}'".format(gr_project_name))
                    return False
            else:
                self.logger.error("Problem getting the GR recipe for project '{}'".format(gr_project_name))
                return False

    def publish_golden_records(self, gr_project_name):
        """
        Refresh the golden records dataset
        :param gr_project_name: GR Project name
        :return: True if successful. Otherwise False.
        """
        all_recipes = self.get_recipes()
        if all_recipes is not None:
            gr_recipe = [recipe for recipe in all_recipes if recipe["data"]["name"] == gr_project_name+"-GOLDEN_RECORDS"]
            if len(gr_recipe) == 1:
                gr_module_id = gr_recipe[0]["data"]["metadata"]["resultingFromModule"]
                url = self._protocol + "://" + self._hostname + ":" + self._port + \
                      "/api/recipe/modules/{}/publish".format(gr_module_id)
                headers = {"Content-Type": "application/json", "Accept": "application/json", "Authorization": self._basicCreds}
                try:
                    response = requests.post(url, headers=headers)
                    if response.status_code == 200:
                        return True
                    else:
                        return False
                except Exception as e:
                    self.logger.error(e)
                    self.logger.error("Problem publishing golden records for project '{}'".format(gr_project_name))
                    return False
            else:
                self.logger.error("Problem getting the GR recipe for project '{}'".format(gr_project_name))
                return False

    def generate_golden_records_export(self, gr_project_name):
        """
        Generate export for golden records dataset
        :param gr_project_name: GR Project name
        :return: True if successful. Otherwise False.
        """
        all_datasets = self.get_datasets()
        gr_dataset = [dataset for dataset in all_datasets if dataset["name"] == gr_project_name + "_golden_records"]
        if len(gr_dataset) == 1:
            gr_dataset_id = gr_dataset[0]["id"].split("/")[-1]
            gr_dataset_details = self.get_dataset_by_id(gr_dataset_id)
            export_config = {}
            export_config["columns"] = gr_dataset_details["data"]["fields"]
            export_config["datasetId"] = int(gr_dataset_id)
            export_config["formatConfiguration"] = {
                "@class": "com.tamr.procurify.models.export.CsvFormat$Configuration",
                "delimiterCharacter": ",",
                "quoteCharacter": "\"",
                "nullValue": "",
                "writeHeader": True
            }
            export_config["revision"] = gr_dataset_details["data"]["version"]
            url = self._baseUrl + "/api/export"
            headers = {"Content-Type": "application/json", "Accept": "application/json", "Authorization": self._basicCreds}
            try:
                response = requests.post(url, headers=headers, data=json.dumps(export_config))
                if response.status_code == 200:
                    response_obj = json.loads(response.text)
                    job_id = response_obj["data"]["jobId"]
                    if self._job_finishes_ok(job_id):
                        return True
                    else:
                        return False
            except Exception as e:
                self.logger.error(e)
                return False
        else:
            return False

    def stream_golden_records(self, gr_project_name):
        """
        Stream golden records data from golden records dataset
        :param project_name: Project name
        :return: Stream of records in JSON string
        """
        self.logger.info("Streaming golden records from Unify for project '{}'".format(gr_project_name))
        all_datasets = self.get_datasets()
        gr_dataset = [dataset for dataset in all_datasets if dataset["name"] == gr_project_name + "_golden_records"]
        if len(gr_dataset) == 1:
            gr_dataset_id = gr_dataset[0]["id"].split("/")[-1]
            url = self._baseUrl + "/api/versioned/v1/datasets/{}/records".format(gr_dataset_id)
            headers = {"Accept": "application/json", "Authorization": self._basicCreds}
            try:
                response = requests.get(url, headers=headers, stream=True)
                if response.status_code == 200:
                    for line in response.iter_lines(decode_unicode=True):
                        if line:
                            yield json.loads(line.decode("utf-8"))
                else:
                    self.logger.error(response.text)
                    self.logger.error("Problem streaming dataset")
                    yield None
            except requests.exceptions.ChunkedEncodingError as e:
                self.logger.error("IncompleteRead error when streaming golden records for project '{}'".format(gr_project_name))
                self.logger.error(e)
                yield None
            except Exception as e:
                self.logger.error("Problem streaming golden records for project '{}'".format(gr_project_name))
                self.logger.error(e)
                yield None
        else:
            self.logger.error("Golden Record dataset does not exist for project '{}'".format(gr_project_name))
            yield None

    def stream_dataset(self, dataset_name):
        """
        Stream records data from dataset
        :param dataset_name: Dataset name
        :return: Stream of records in JSON string
        """
        self.logger.info("Streaming records from Unify for dataset '{}'".format(dataset_name))
        all_datasets = self.get_datasets()
        dataset = [dataset for dataset in all_datasets if dataset["name"] == dataset_name]
        if len(dataset) == 1:
            dataset_id = dataset[0]["id"].split("/")[-1]
            url = self._baseUrl + "/api/versioned/v1/datasets/{}/records".format(dataset_id)
            headers = {"Accept": "application/json", "Authorization": self._basicCreds}
            try:
                idx = 0
                response = requests.get(url, headers=headers, stream=True)                
                if response.status_code == 200:
                    for line in response.iter_lines(decode_unicode=True):
                        if line:                            
                            yield json.loads(line.decode("utf-8"))

                        if idx != 0 and idx % 5000 == 0 :
                            self.logger.info("{}({} rows) loaded...".format(dataset_name, idx))
                        idx += 1
                    self.logger.info("{}({} rows) loaded...".format(dataset_name, idx))
                else:
                    self.logger.error(response.text)
                    self.logger.error("Problem streaming dataset")
                    yield None
            except requests.exceptions.ChunkedEncodingError as e:
                self.logger.error("IncompleteRead error when streaming records for dataset '{}'".format(dataset_name))
                self.logger.error(e)
                yield None
            except Exception as e:
                self.logger.error("Problem streaming records for dataset '{}'".format(dataset_name))
                self.logger.error(e)
                yield None
        else:
            self.logger.error("Dataset '{}' does not exist".format(dataset_name))
            yield None

    def stream_golden_records(self, project_name):
        """
        Stream golden records data from golden records dataset
        :param project_name: Project name
        :return: Stream of records in JSON string
        """
        self.logger.info("Streaming golden records from Unify for project '{}'".format(project_name))
        all_datasets = self.get_datasets()
        gr_dataset = [dataset for dataset in all_datasets if dataset["name"] == project_name + " - Golden Record"]
        if len(gr_dataset) == 1:
            gr_dataset_id = gr_dataset[0]["id"].split("/")[-1]
            url = self._baseUrl + "/api/versioned/v1/datasets/{}/records".format(gr_dataset_id)
            headers = {"Accept": "application/json", "Authorization": self._basicCreds}
            try:
                response = requests.get(url, headers=headers, stream=True)
                if response.status_code == 200:
                    for line in response.iter_lines(decode_unicode=True):
                        if line:
                            yield json.loads(line.decode("utf-8"))
                else:
                    self.logger.error(response.text)
                    self.logger.error("Problem streaming dataset")
                    yield None
            except requests.exceptions.ChunkedEncodingError as e:
                self.logger.error("IncompleteRead error when streaming golden records for project '{}'".format(project_name))
                self.logger.error(e)
                yield None
            except Exception as e:
                self.logger.error("Problem streaming golden records for project '{}'".format(project_name))
                self.logger.error(e)
                yield None
        else:
            self.logger.error("Golden Record dataset does not exist for project '{}'".format(project_name))
            yield None

    def train_predict_clusters(self, project_name):
        """
        Refresh the golden records dataset
        :param project_name: Project name
        :return: True if successful. Otherwise False.
        """
        all_recipes = self.get_recipes()
        if all_recipes is not None:
            dedup_recipe = [recipe for recipe in all_recipes if recipe["data"]["name"] == project_name+"-DEDUP"]
            if len(dedup_recipe) == 1:
                dedup_recipe_id = dedup_recipe[0]["documentId"]["id"]
                url = self._baseUrl + "/api/recipe/recipes/{}/run/trainPredictCluster".format(dedup_recipe_id)
                headers = {"Content-Type": "application/json", "Accept": "application/json", "Authorization": self._basicCreds}
                try:
                    response = requests.post(url, headers=headers)
                    if response.status_code == 201:
                        response_obj = json.loads(response.text)
                        job_id = response_obj["documentId"]["id"]
                        return self._job_finishes_ok(job_id)
                except Exception as e:
                    self.logger.error(e)
                    self.logger.error("Problem train and predict clusters for project '{}'".format(project_name))
                    return False
            else:
                self.logger.error("Problem getting the DEDUP recipe for project '{}'".format(project_name))
                return False

    def run_mastering(self, project_id, project_name):
        """
        Run the whole mastering pipeline
        :param project_id: Project ID
        :return: True if successful. Otherwise False.
        """
        project = self.unify.projects.by_resource_id(project_id)
        project = project.as_mastering()

        self.logger.info("Updating unified dataset for project '{}'".format(project_id))
        try:
            op = project.unified_dataset().refresh()
            if not op.succeeded():
                self.logger.error("Problem refreshing unified dataset for project '{}'".format(project_id))
                return False
        except Exception as e:
            self.logger.error(e)
            self.logger.error("Problem refreshing unified dataset for project '{}'".format(project_id))
            return False

        self.logger.info("Updating pairs for project '{}'".format(project_id))
        try:
            op = project.pairs().refresh()
            if not op.succeeded():
                self.logger.error("Problem updating the pairs for project '{}'".format(project_id))
                return False
        except Exception as e:
            self.logger.error(e)
            self.logger.error("Problem updating the pairs for project '{}'".format(project_id))
            return False

        self.logger.info("Updating clusters for project '{}'".format(project_id))
        if not self.train_predict_clusters(project_name):
            self.logger.error("Problem updating clusters")
            return False

        # self.logger.info("Updating dedup model for project '{}'".format(project_id))
        # try:
        #     model = project.pair_matching_model()
        #     op = model.train()
        #     if not op.succeeded():
        #         self.logger.error("Problem training the model for project '{}'".format(project_id))
        #         return False
        # except Exception as e:
        #     self.logger.error(e)
        #     self.logger.error("Problem training the model for project '{}'".format(project_id))
        #     return False

        # self.logger.info("Updating model predictions for project '{}'".format(project_id))
        # try:
        #     op = model.predict()
        #     if not op.succeeded():
        #         self.logger.error("Problem generating model predictions for project '{}'".format(project_id))
        #         return False
        # except Exception as e:
        #     self.logger.error(e)
        #     self.logger.error("Problem generating model predictions for project '{}'".format(project_id))
        #     return False

        # self.logger.info("Updating high impact pairs for project '{}'".format(project_id))
        # try:
        #     op = project.high_impact_pairs().refresh()
        #     if not op.succeeded():
        #         self.logger.error("Problem updating high impact pairs for project '{}'".format(project_id))
        #         return False
        # except Exception as e:
        #     self.logger.error(e)
        #     self.logger.error("Problem updating high impact pairs for project '{}'".format(project_id))
        #     return False

        self.logger.info("Updating published clusters for project '{}'".format(project_id))
        try:
            op = project.published_clusters().refresh()
            if not op.succeeded():
                self.logger.error("Problem publishing clusters for project '{}'".format(project_id))
                return False
        except Exception as e:
            self.logger.error(e)
            self.logger.error("Problem publishing clusters for project '{}'".format(project_id))
            return False

        return True

        # self.logger.info("Updating Golden Records")
        # if not self.refresh_golden_record_dataset(project_name):
        #     self.logger.error("Problem updating golden records dataset")

        # self.logger.info("Generating Golden Records export")
        # if not self.generate_golden_records_export(project_name):
        #     self.logger.error("Problem generating the export for golden records for project '{}'".format(project_name))

        # return True

    def get_published_clusters_versions(self, project_id, persistent_cluster_ids):
        """
        Get versions of the published clusters
        :param project_id: The ID of the project
        :param persistent_cluster_ids: list of persistent cluster IDs
        :return: list of json objects containing the persistent IDs and corresponding latest versions and modified dates
        """
        self.logger.info("Getting published cluster versions for project '{}'".format(project_id))
        url = self._baseUrl + "/api/versioned/v1/projects/{}/publishedClusterVersions".format(project_id)
        headers = {"Accept": "application/json", "Authorization": self._basicCreds}
        BATCH_SIZE = 200000
        lower_index = 0
        result = []
        NUM_TRIALS = 5
        while lower_index < len(persistent_cluster_ids):
            trial = 1
            while trial <= NUM_TRIALS:
                try:
                    response = requests.get(url, headers=headers, stream=True,
                                            data='\n'.join(persistent_cluster_ids[lower_index:lower_index+BATCH_SIZE])
                                            )
                    if response.status_code not in [200, 201, 202]:
                        self.logger.error(
                            "ERROR: Problem Getting published clusters versions for project '{}' in trial {}: {}".format(
                                project_id, trial, response.text
                            ))
                        if trial >= NUM_TRIALS:
                            return None
                        trial += 1
                        time.sleep(60)
                    else:
                        break
                except Exception as e:
                    self.logger.error(e)
                    self.logger.error("Problem getting published clusters versions for project '{}' in trial {}".format(project_id, trial))
                    if trial >= NUM_TRIALS:
                        return None
                    trial += 1
                    time.sleep(60)
            for line in response.iter_lines(decode_unicode=True):
                if line:
                    version = json.loads(line.decode('utf-8'))
                    if len(version["versions"]) == 0:
                        continue
                    result.append({"persistentId": version["id"],
                                   "materializationDate": version["versions"][0]["timestamp"]})
            lower_index += BATCH_SIZE
        return result

    def get_published_clusters_versions_internal(self, unified_dataset_name, persistent_cluster_ids):
        """
        Get versions of the published clusters
        :param unified_dataset_name: unified dataset name
        :param persistent_cluster_ids: list of persistent cluster IDs
        :return: list of json objects containing the persistent IDs and corresponding latest versions and modified dates
        """
        self.logger.info("Getting published cluster versions for unified dataset '{}'".format(unified_dataset_name))
        url = self._baseUrl + "/api/dedup/supplier-mastering/published-cluster-versions/{}".format(unified_dataset_name)
        headers = {"Accept": "application/json", "Authorization": self._basicCreds}
        BATCH_SIZE = 1000
        lower_index = 0
        result = []
        NUM_TRIALS = 5
        while lower_index < len(persistent_cluster_ids):
            trial = 1
            while trial <= NUM_TRIALS:
                try:
                    response = requests.post(url, headers=headers, stream=True,
                                             data='"' + '"\n"'.join(persistent_cluster_ids[lower_index:lower_index+BATCH_SIZE]) + '"'
                                             )                    
                    if response.status_code not in [200, 201, 202]:
                        self.logger.error(
                            "ERROR: Problem Getting published clusters versions for project '{}' in trial {}: {}".format(
                                unified_dataset_name, trial, response.text
                            ))
                        if trial >= NUM_TRIALS:
                            return None
                        trial += 1
                        time.sleep(60)
                    else:
                        break
                except Exception as e:
                    self.logger.error(e)
                    self.logger.error("Problem getting published clusters versions for project '{}' in trial {}".format(unified_dataset_name, trial))
                    if trial >= NUM_TRIALS:
                        return None
                    trial += 1
                    time.sleep(60)
            idx = 0
            for line in response.iter_lines(decode_unicode=True):
                if line:
                    version = json.loads(line.decode('utf-8'))
                    if len(version["versions"]) == 0:
                        continue
                    result.append({"persistentId": version["id"]["persistentId"],"materializationDate": version["versions"][0]["materializationDate"]})
                if idx != 0 and idx % 5000 == 0 :                     
                    self.logger.info("{}({} rows) loaded...".format(dataset_name, idx))
                idx += 1
            self.logger.info("{}({} rows) loaded...".format(dataset_name, idx))
            lower_index += BATCH_SIZE
        return result
