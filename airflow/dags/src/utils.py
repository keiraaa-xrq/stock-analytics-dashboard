import configparser
from typing import *

def get_bigquery_key() -> str:
    config = configparser.ConfigParser()
    config.read('./project_config.cfg')
    bq_cfg = config['bigquery']
    key_file_name = bq_cfg['key_file_name']
    return key_file_name


def get_reddit_key() -> str:
    config = configparser.ConfigParser()
    config.read('./project_config.cfg')
    bq_cfg = config['reddit']
    key_file_name = bq_cfg['key_file_name']
    return key_file_name
