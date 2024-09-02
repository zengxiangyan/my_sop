import arrow
import copy
import csv
import logging
logger = logging.getLogger('spider')
import base64
import json
import os
import requests
import time

import application as app
from extensions import utils

client_id_default = ''
client_secret_default = ''
url_token = 'https://aip.baidubce.com/oauth/2.0/token?grant_type=client_credentials&client_id={client_id}&client_secret={client_secret}'
url_ocr = 'https://aip.baidubce.com/rest/2.0/ocr/v1/general?access_token={access_token}'

class Base:

    def __init__(self, client_id=client_id_default, client_secret=client_secret_default):
        self.client_id = client_id
        self.client_secret = client_secret

    def get_token(self):
        url = url_token.format(client_id=self.client_id, client_secret=self.client_secret)
        response = requests.get(url)
        if not response:
            return
        j = response.json()
        access_token = j['access_token']
        print('access_token:', access_token)
        return access_token

    def get_result(self, image_file):
        access_token = self.get_token()
        url = 'https://aip.baidubce.com/rest/2.0/ocr/v1/general?access_token={access_token}'.format(access_token=access_token)
        # url = 'https://aip.baidubce.com/rest/2.0/ocr/v1/general_basic?access_token={access_token}'.format(access_token=access_token)
        head = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        with open(image_file, "rb") as f:  # 转为二进制格式
            base64_data = base64.b64encode(f.read())
        data = {
            'image': base64_data
        }
        r = requests.post(url, data, headers=head)
        content = r.json()
        return content



