import configparser
from typing import *
import os
import sys, os


def get_key_file_name() -> str:
    config = configparser.ConfigParser()
    config.read('../../project_config.cfg')
    bq_cfg = config['bigquery']
    key_file_name = bq_cfg['key_file_name']
    return key_file_name
