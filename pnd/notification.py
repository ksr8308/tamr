import os
import sys
import string
import urllib

current_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(current_path + '/config')
from pnd_logger import PndLogger
from config_manager import ConfigManager

sys.path.append(current_path + '/data')
from data_manager import DataManager
from custom_unify import CustomUnify

if __name__ == "__main__":
    process_name = "notification"
    logger = PndLogger("Notification", process_name)
    cm = ConfigManager("unify", process_name)
    unify = CustomUnify(cm, process_name)
    dm = DataManager("tamr", "unify", process_name)

    logger.info("start notification.")

    data = { "filters":  [ { "@class": "com.tamr.persist.models.querying.Filter", "type": "NOT_EQUALS", "test": "null", "path": { "fields": ["data", "assignmentInfo"] } }, { "@class": "com.tamr.persist.models.querying.Filter", "type": "EQUALS", "test": "Schema_Discovery_unified_dataset", "path": { "fields": ["data", "unifiedDatasetName"] } } ], "limit": 1000, "offset": 0 }
    dataset = dm.get_record_pair_feedback(data)
    except_users = ["mdm_master", "admin"]
    receips = [row["data"]["username"] for row in dataset if row["data"]["username"] not in except_users]
    receips = list(set(receips))
    receips_str = ";".join(receips)    
    bindvars = []     
    msg_subject = "Master Data Matching Training"
    msg_content = "Master Data Matching 학습을 위한 Feedback 요청이 할당되었으니\r\n아래링크로 접속하셔서 진행바랍니다. (Tamr Pairing)\r\nhttp://{}:9100".format(unify._creds["hostname"])    
    chnl_id = "X0111968;X0111967;2038884;{}".format(receips_str)
    row = {"msg_seq": 0, "msg_title": msg_subject, "msg_cont": msg_content, "chnl_type": "3", "chnl_id": chnl_id, "msg_type": "M"}
    bindvars.append(row)
    dm.insert_feedback_notification(bindvars)
    logger.info("Receive user or channel = {}".format(chnl_id))
    logger.info("Receive user or channel = {}".format(chnl_id))
    logger.info("finish.")