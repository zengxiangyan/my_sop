# -*- coding: utf-8 -*-
import requests
import pandas as pd
import json
import yaml
from os.path import abspath, join, dirname
import sys
sys.path.insert(0, join(abspath(dirname(__file__))))
from TestWeworklogin import TestWeworkLogin
# print(sys.path)


def set_headers(url, data, csrf):
    eid = data['eid']
    headers = {
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Connection': 'keep-alive',
        'Content-Length': '2004',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        #'Cookie': '_csrf=767fe6f02e48a6e791d00bfb2379fa71010e380b41afea8c5b013362f295ff39a%3A2%3A%7Bi%3A0%3Bs%3A5%3A%22_csrf%22%3Bi%3A1%3Bs%3A32%3A%22E%B4%EE%7B%0B%2B%5C%1E_4s%25RB%E2%87%2C%F1%9F%5C%7D%F3w%22%F3%C8%84%8E%18%1Dc%B0%22%3B%7D; Hm_lvt_1a4dd5d642bb6d7388fcf26174a25044=1698226047; Hm_lpvt_1a4dd5d642bb6d7388fcf26174a25044=1698226047; PHPSESSID=7c3d9512c9d8995655c255c7fb05af62',
        'Host': 'sop.ecdataway.com',
        'Origin': 'https://sop.ecdataway.com',
        'Referer': f'https://sop.ecdataway.com/bi/reportform/getreportnew2?&eid={eid}',
        'Sec-Ch-Ua': '"Chromium";v="118", "Google Chrome";v="118", "Not=A?Brand";v="99"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': '"Windows"',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
        'X-Csrf-Token': csrf,
        'X-Requested-With': 'XMLHttpRequest'
    }
    return headers


def post_search_dict(url, data):
    ###  自动使用已登录的cookies登录，如果过期则重新输入用户密码登录获取新的身份信息  （目前测试只需要修改时间戳即可，后期不行可换验证登录）###
    try:
        with open(sys.path[0]+"/cookies.yaml", "r") as f1:
            cookies = yaml.safe_load(f1)
        cookies = {cookie['name']: cookie['value'] for cookie in cookies}
        with open(sys.path[0]+"/csrf_token.yaml", "r") as f2:
            csrf = yaml.safe_load(f2)
            csrf = csrf['csrf_token']
        headers = set_headers(url, data, csrf)
        response = requests.post(url, headers=headers, cookies=cookies, data=data)
        # print(response.json(),response.status_code)
        if (response.status_code == 400) or (response.json().get('msg') == '登陆超时，请重新登陆！'):
            print(response)
            a = TestWeworkLogin()
            a.setup_class()
            a.test_save_cookies()
            with open(sys.path[0]+"/cookies.yaml", "r") as f:
                cookies = yaml.safe_load(f)
            cookies = {cookie['name']: cookie['value'] for cookie in cookies}
            response = requests.post(url, headers=headers, cookies=cookies, data=data)
            print(response.text)
            return response.text
        else:
            return response.text
    except Exception as e:
        a = TestWeworkLogin()
        a.setup_class()
        a.test_save_cookies()
        return e


def run(starttime,endtime,eid,table,cid):
    url = 'https://sop.ecdataway.com/bi/reportform/prodclicknew2'  ###sop查询接口
    data = {
            'select_model': '1',
            'toptype': 'all',
            'starttime': starttime,
            'endtime': endtime,
            'eid': eid,
            'stable': table,
            'cat': '1',
            'allcatewhere': cid,
        }
    print(data)
    try:
        data = json.loads(post_search_dict(url, data))
        print(data)
        # 提取 content 字段中的数据
        content = data['data']['content']

        # 将字典列表转换为 DataFrame
        df = pd.DataFrame(content).fillna('')
        df['平台'] = df['source2']
        df['Full_path'] = df['lv1name'].fillna('') + \
                          df['lv2name'].apply(lambda x: '>>' + x if x else '') + \
                          df['lv3name'].apply(lambda x: '>>' + x if x else '') + \
                          df['lv4name'].apply(lambda x: '>>' + x if x else '') + \
                          df['lv5name'].apply(lambda x: '>>' + x if x else '')
        return df[['平台', 'cid', 'lv1cid', 'lv2cid', 'lv3cid','lv4cid', 'lv5cid', 'lv1name', 'lv2name', 'lv3name', 'lv4name','lv5name', 'Full_path']]
    except Exception as e:
        return e

if __name__ == "__main__":
    df = run(starttime='2020-01-01',endtime='2023-10-01',eid=92080,table='entity_prod_92080_E',cid=[])
    print(df)
    # print(sys.path[0]+"/report/")