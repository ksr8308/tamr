import os
import sys
import logging
import json
import requests
import tamr_unify_client as api
from tamr_unify_client.auth import UsernamePasswordAuth
from urllib.parse import quote 

current_path = os.path.dirname(os.path.realpath(__file__))

sys.path.append(current_path + '/../src/')
from unify import Unify

sys.path.append(current_path + '/../config/')
from config_manager import ConfigManager
from pnd_logger import PndLogger

class CustomUnify:

    def __init__(self, conf_manager, logger_name=None):
        self.__logger = PndLogger("Custom Unify Class", logger_name)
        self.__cm = conf_manager

        self._creds = None
        self.__set_creds()

        self.unify = None
        self.__set_unify()

        self.versioned = Versioned(logger_name, self.unify)
        self.dedup = Dedup(logger_name, self.unify)
        self.dataset = Dataset(logger_name, self.unify)
        self.persistence = Persistence(logger_name, self.unify)

    def __set_creds(self) :
        try:
            self._creds = self.__cm.creds            
        except Exception as e:
            self.__logger.error("Failed to configure creds. - {}".format(e))
        return

    def __set_unify(self) :
        try:
            self.unify = Unify(self._creds["protocol"], self._creds["hostname"], self._creds["port"], self._creds["grPort"], self._creds["user"], self._creds["pwd"])            
        except Exception as e:
            self.__logger.info("Failed to configure Unify. - {}".format(e))
        return

class Versioned:
    def __init__(self, logger_name, unify):
        self.__logger = PndLogger("Unify-Versioned", logger_name)
        self.__unify = unify
        self.__all_datasets = unify.get_datasets()
    
    def is_exist_dataset(self, dataset_name) :        
        dataset = [dataset for dataset in self.__all_datasets if dataset["name"] == dataset_name]
        return True if len(dataset) > 0 else False

    def get_stream_dataset(self, dataset_name) :
        dataset_list = []
        for row in self.__unify.stream_dataset(dataset_name) :
            dataset_list.append(row)        
        return  dataset_list

    def get_unified_metadata_dataset(self, source) :
        dataset_name = "{}_column_metadata".format(source)
        return self.__unify.stream_dataset(dataset_name)

    def get_unified_metadata_profiled_dataset(self, source) :
        dataset_name = "{}_column_metadata_profiled.csv".format(source)
        return self.__unify.stream_dataset(dataset_name)        

    def get_unified_dataset(self, proj_nm) :
        dataset_name = "{}_unified_dataset".format(proj_nm)        
        return self.__unify.stream_dataset(dataset_name)
            
    def get_unified_dataset_dedup_published_clusters_with_data(self, proj_nm) :
        dataset_name = "{}_unified_dataset_dedup_published_clusters_with_data".format(proj_nm)
        return self.__unify.stream_dataset(dataset_name)
        
    def get_golden_records_overrides(self, proj_nm) :
        dataset_name = "{}_GR_golden_records_overrides".format(proj_nm)        
        return self.__unify.stream_dataset(dataset_name)
    
    def get_golden_records_draft(self, proj_nm) :
        dataset_name = "{}_GR_golden_records_draft".format(proj_nm)        
        return self.__unify.stream_dataset(dataset_name)

    def get_golden_records(self, proj_nm) :
        dataset_name = "{}_GR_golden_records".format(proj_nm)        
        return self.__unify.stream_dataset(dataset_name)

    def get_clusters_published_date(self, proj_nm) :
        dataset_name = "{}_unified_dataset".format(proj_nm)
        persistent_ids = [row['persistentId'] for row in self.get_unified_dataset_dedup_published_clusters_with_data(proj_nm)]
        return self.__unify.get_published_clusters_versions_internal(dataset_name, persistent_ids)

    def get_dataset_metadata(self) :
        dataset_list = []
        for dataset in self.__all_datasets :
            if "_column_metadata" in dataset["name"] and "sample" not in dataset["name"] and ".csv" not in dataset["name"]:
                dataset_list.append(dataset["name"])
        return dataset_list

    def get_dataset_profiled(self) :
        dataset_list = []
        for dataset in self.__all_datasets :
            if "_column_metadata_profiled" in dataset["name"] and "sample" not in dataset["name"] :
                dataset_list.append(dataset["name"])
        return dataset_list

    def get_projects(self):
        url = self.__unify._baseUrl + "/api/pubapi/v1/projects"
        headers = {"Content-Type": "application/json", "Accept": "application/json", "Authorization": self.__unify._basicCreds}

        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                return json.loads(response.text)
            else:
                self.logger.error("Failed to retrieve project information from Tamr API. Try replacing pubapi with versioned.")
                return None
        except Exception as e:
            self.__logger.error(e)
            self.__logger.error("Failed to retrieve project information from Tamr API. Try replacing pubapi with versioned.")
        return None    

class Dedup:
    def __init__(self, logger_name, unify):
        self.__logger = PndLogger("Unify-Dedup", logger_name)
        self.__unify = unify        

    def get_pairs(self, projects):
        dataset_list = []
        try:
            for dataset in projects:        
                unified_dataset = quote(dataset)                 
                params = {
                    'dataset': unified_dataset,
                    'commentStatus': True
                } 
                url = self.__unify._baseUrl + "/api/dedup/pairs/{}".format(unified_dataset)
                headers = {"Content-Type": "application/json", "Accept": "application/json", "Authorization": self.__unify._basicCreds}
                response = requests.get(url, params = params, headers=headers)
                if response.status_code == 200:
                    dataset_list.append(json.loads(response.text))

            return dataset_list
        except Exception as e:            
            self.__logger.error('Failed to retrieve pairs with comments from Tamr API. - {}'.format(e))
        return
       
    def get_pair_comments_query(self, pairs):
        dataset_list = []
        json_list = []
        url = self.__unify._baseUrl + "/api/dedup/pairs/comment/query"
        headers = {"Content-Type": "application/json", "Accept": "application/json", "Authorization": self.__unify._basicCreds}

        try:
            for pair in pairs : 
                for item in pair['items'] :
                    json_list.append("{}|{}|{}|{}".format(str(item['datasetName1']), str(item['transactionId1']), str(item['datasetName2']), str(item['transactionId2'])))

            response = requests.post(url, json = json_list, headers=headers)
            if response.status_code == 200:
                dataset_list.append(json.loads(response.text))

            return dataset_list
        except Exception as e:            
            self.__logger.error('Failed to retrieve pairs with comments from Tamr API. - {}'.format(e))
        return

    def set_lock_clusters(self, dataset_name, persistent_ids):
        dataset_list = []
        json_list = []
        url = self.__unify._baseUrl + "/api/dedup/clusters/{}/lockClusters".format(dataset_name)
        headers = {"Content-Type": "application/json", "Accept": "application/json", "Authorization": self.__unify._basicCreds}

        try:
            self.__logger.info(json.dumps(persistent_ids))
            response = requests.post(url, headers=headers, data=json.dumps(persistent_ids))
            if response.status_code == 200:
                return True
            else:
                self.__logger.error("Failed to lock clusters. - {}".format(response.status_code))
                return False
        except Exception as e:            
            self.__logger.error("Failed to lock clusters. - {}".format(e))
        return 

    def refresh_golden_records(self, project_name) :
        try:
            self.__unify.refresh_golden_record_dataset(project_name) 
        except Exception as e:
            self.__logger.error("Failed to udpate golden record. - {}".format(e))

    def publish_golden_records(self, project_name) :
        try:
            self.__unify.publish_golden_records(project_name) 
        except Exception as e:
            self.__logger.error("Failed to publish golden record. - {}".format(e))

    def generate_golden_records(self, project_name) :
        try:
            self.__unify.generate_golden_records_export(project_name) 
        except Exception as e:
            self.__logger.error("Failed to generate golden record. - {}".format(e))

    def get_mastering_labels(self, datasetName) :        
        dataset_list = []
        url = self.__unify._baseUrl + "/api/dedup/pairs/labels/{}?".format(datasetName)
        headers = {"Content-Type": "application/json", "Accept": "application/json", "Authorization": self.__unify._basicCreds}
        try:            
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                for item in response.iter_lines() :                    
                    dataset_list.append(json.loads(item))
            self.__logger.info("Successed get labels from {}".format(datasetName))
            return dataset_list
        except Exception as e:            
            self.__logger.error(e)
        return 

    def upload_mastering_labels(self, datasetName, datasets) :
        url = self.__unify._baseUrl + "/api/dedup/pairs/labels/{}?includeUserResponse=false".format(datasetName)        
        headers = {"Content-Type": "application/json", "Accept": "application/json", "Authorization": self.__unify._basicCreds}
        try:            
            response = requests.post(url, headers=headers, data=datasets)
            if 200 <= int(response.status_code) and int(response.status_code) < 200 :
                self.__logger.error("Failed to uploading mastering labels. - {}".format(response.status_code))                
            else:
                self.__logger.info("Successed to uploading mastering labels.")
        except Exception as e:
            self.__logger.error(e)
        return 

class Dataset :
    def __init__(self, logger_name, unify):
        self.__logger = PndLogger("Unify-Dataset", logger_name)
        self.__unify = unify
        self.__all_datasets = unify.get_datasets()

    def delete_dataset(self, dataset_name) :
        dataset = [dataset for dataset in self.__all_datasets if dataset["name"] == dataset_name]
        if len(dataset) == 1:
            dataset_id = dataset[0]["id"].split("/")[-1]
            url = self.__unify._baseUrl + "/api/dataset/datasets/{}".format(dataset_id)
            headers = {"Content-Type": "application/json", "Accept": "application/json", "Authorization": self.__unify._basicCreds}            
            try:
                response = requests.post(url, headers=headers)
                if response.status_code == 200:
                    self.__logger.info("Delete {} dataset SUCCESSED.".format(dataset_name))
                else:
                    self.__logger.error(response.text)
                    self.__logger.error("Problem streaming dataset")
                    yield None            
            except Exception as e:
                self.__logger.error("Problem deleted dataset '{}'".format(dataset_name))
                self.__logger.error(e)
                yield None
        return
    
    def truncate_dataset(self, dataset_name) :
        dataset = [dataset for dataset in self.__all_datasets if dataset["name"] == dataset_name]
        if len(dataset) == 1:            
            url = self.__unify._baseUrl + "/api/dataset/datasets/{}/truncate".format(dataset_name)
            headers = {"Content-Type": "application/json", "Accept": "application/json", "Authorization": self.__unify._basicCreds}            
            try:
                response = requests.post(url, headers=headers)
                if response.status_code == 200:
                    self.__logger.info("Truncated {} dataset SUCCESSED.".format(dataset_name))                    
                else:
                    self.__logger.error(response.text)
                    self.__logger.error("Problem truncate dataset")                                    
            except Exception as e:
                self.__logger.error("Problem truncate dataset '{}'".format(dataset_name))
                self.__logger.error(e)                
        return

class Persistence :
    def __init__(self, logger_name, unify):
        self.__logger = PndLogger("Unify-Persistence", logger_name)
        self.__unify = unify

    def get_stream_query(self, namespace, data) :
        result_sets = []
        try:            
            url = self.__unify._baseUrl + "/api/persistence/ns/{}/streaming-query".format(namespace)
            headers = {"Content-Type": "application/json", "Accept": "application/json", "Authorization": self.__unify._basicCreds}                        
            response = requests.post(url, headers=headers, data=json.dumps(data))
            
            if response.status_code == 200:
                for row in json.loads(json.dumps(response.text)).split("\n") :
                    result_sets.append(json.loads(row))

            return result_sets
        except Exception as e:            
            self.__logger.error('Failed to retrieve pairs with comments from Tamr API. - {}'.format(e))
        return