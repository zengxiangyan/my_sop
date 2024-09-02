#coding=utf-8
import sys
from os.path import abspath, join, dirname
import pandas as pd
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
import application as app
import csv
import json
import time
import copy
import requests
from datetime import datetime
from extensions import utils
from models.analyze.analyze_tool import used_time
from models.tiktok.table_name import *
from models.tiktok.video_mention import get_mention
from models.tiktok.video_mention import update_mention
from models.tiktok.video_manual import VideoManual


class PropPanel(object):
    def __init__(self, category, user_id=0):
        self.category = category
        self.user_id = user_id
        self.tables_config = {
            'category': 'douyin2_cleaner.douyin_video_prop_metrics_category',
            'pnid': 'douyin2_cleaner.douyin_video_prop_metrics_pnid',
            'pnvid': 'douyin2_cleaner.douyin_video_prop_metrics_pnvid'
        }
        pass

    def get_score(self, field):

        pass

