import configparser
from typing import *

def get_key_file_name() -> Tuple[str, str]:
    config = configparser.ConfigParser()
    config.read('./project_config.cfg')
    bq_cfg = config['bigquery']
    key_file_name = bq_cfg['key_file_name']
    return 