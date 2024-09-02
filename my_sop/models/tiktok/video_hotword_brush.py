# coding=utf-8
import os
import sys
from os.path import abspath, join, dirname
from pathlib import Path
import numpy as np
import pandas as pd
import csv
import json
import time
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
import argparse
import arrow
from dateutil.parser import parse
import argparse
import copy
import re
import requests
import hashlib
from itertools import chain
import asyncio
import nest_asyncio
from tqdm import tqdm
from typing import List
import logging

sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
import application as app
from extensions import utils
from chatgpt import gpt_utils
from scripts.cdf_lvgou.tools import Fc

from models.analyze.analyze_tool import used_time
from models.nlp.common import text_normalize


class words_judge(list):
    def __init__(self,words,model='gpt4', temperature=0.5):
        self.words=words
        self.model=model
        self.temperature=temperature
        # self.presence_penalty=presence_penalty
        # self.frequency_penalty=frequency_penalty
        self._main()

    def _filter_message_quailifer(self,keywords):
        keywords='\n'.join(list(keywords))
        message=[
            {'role':'system',"content":f'你是一个大师级的关键词运营分析师,我会给你一些关键词，请你挑选一些有创意的词给我，'},
            {'role':'user',"content":f'我需要你过滤掉的词类似于\n["他们家","不知道","我知道"]\n以及["佳洁士","#佳洁士凉感锁白牙膏","白牙膏"]还有["23ml","40天","6666"]\n\n我需要保留的词包括但不限于["我又又氟了","不辣口"]请你分析下这些词的特点是什么。'},
            {'role':'user','content':f'{keywords}\n,请你进行过滤并以list格式返回给我'},
            {'role':'user','content':f'请你将认为合适的词组成一个列表，以jsonlist格式返回给我'}

        ]
        return message
    def _find_list(self,x):
        try:
            return re.findall(r'(\[[\s\S]*?\])',x)[0]
        except:
            print(x)
    def _main(self):
        messages=list(chain.from_iterable([[self._filter_message_quailifer(self.words[i:i+50])] for i in range(0,len(self.words),50)]))
        nest_asyncio.apply()
        _, ans_res_dict = asyncio.run(gpt_utils.get_chat_answers_nint(messages, mode='nintjp', model=self.model, temperature=self.temperature))
        words_after = list()
        for _, x in ans_res_dict.items():
            x = x["choices"][0]['message']['content']
            try:
                words_after.append(eval(self._find_list(str(x))))
            except:
                print(self._find_list(str(x)))
        words_after=list(chain.from_iterable(words_after))
        list_out=list()
        for i in self.words:
            if i in words_after:
                list_out.append(1)
            else:
                list_out.append(0)
        self += list_out

class words_fetch(list):
    def __init__(self,categorys:List=[],type_ids:List=[1,2,3,4],volume_condition:int=5):
        self.dy2 = app.get_db("dy2")
        self.dy2.connect()
        self.type_ids=type_ids
        self.volume_condition=volume_condition
        self.types = {
            1:'hot_value',
            2:'volume',
            3:'new_value',
            4:'speed_value',
        }
        self.categorys=self._get_categorys(categorys)
        self._main()
    
    def _main(self):
        self._get_words()

    def _get_categorys(self, categorys:List)->List:
        categorys_str = ('and category in ('+','.join(categorys)+')') if categorys else ""
        sql = f"select category, id, `desc` from douyin2_cleaner.hotword_cfg where category != 9920109 {categorys_str}"
        categorys_res = self.dy2.query_all(sql)
        return categorys_res

    def _get_words(self)->List:
        keyword_ids = []
        for category, id, desc in tqdm(self.categorys, desc=f"Processing fetch kids", unit="desc", leave=True):
            keyword_ids += self._get_kids_bycategory(category, id, desc)
        # 根据kids获取words
        keyword_id_sets = [keyword_ids[i:i+1000] for i in range(0,len(keyword_ids),1000)]
        for keyword_id_set in tqdm(keyword_id_sets, desc=f"Processing fetch words", unit="keyword_id_set", leave=True):
            sql = f'''select name
            from douyin2_cleaner.keyword
            where kid in ({','.join(keyword_id_set)})
                and gpt_flag > 1
            '''
            words = self.dy2.query_all(sql)
            words = [tmp[0] for tmp in words]
            res = words_judge(words)
            words_out = [(k,v) for k,v in zip(words, res)]
            self._insert_words_out(words_out)
    
    def _get_kids_bycategory(self, category:int, id:int, desc:str)->List:
        current_date = datetime.now()
        month_s, month_e = get_last_month_day(current_date)
        keyword_ids = []
        for type_id in self.type_ids:
            try:
                sql = f'''
                select distinct kid
                from douyin2_cleaner.douyin_video_hotword_info_{category} 
                where word_type = {type_id}
                    and hotword_id = {id}
                    and volume >= {self.volume_condition}
                    and kid_time between UNIX_TIMESTAMP('{month_s}') and UNIX_TIMESTAMP('{month_e}')
                order by {self.types[type_id]} desc
                limit 400
                '''
                kids = self.dy2.query_all(sql)
                keyword_ids += kids
            except Exception as e:
                print(category, id, desc)
                print(e)
                return False
        keyword_ids = list(set(keyword_ids))
        keyword_ids = [str(tmp[0]) for tmp in keyword_ids]
        return keyword_ids

    def _insert_words_out(self, words_out:List):
        if not words_out:
            return False
        sql = f'''insert into douyin2_cleaner.keyword (name, gpt_flag) values 
        (%s, %s)
        on duplicate key update gpt_flag = values(gpt_flag)
        '''
        self.dy2.execute_many(sql, words_out)
        return True

def get_last_month_day(date:datetime):
    if date.month == 1:
        first_day_of_last_month = datetime(date.year - 1, 12, 1)
    else:
        first_day_of_last_month = datetime(date.year, date.month - 1, 1)
    last_day_of_last_month = date.replace(day=1) - timedelta(days=1)
    return first_day_of_last_month.strftime('%Y-%m-%d'), last_day_of_last_month.strftime('%Y-%m-%d')

def run(category):
    words_fetch(category)

def auto_run_monthly(category):
    print("start")
    try:
        run(category)
    except Exception as e:
        logging.info(str(e))
        Fc.vxMessage(to="zheng.jiatao", title=f"内容力热词筛选报错", text=str(e))

def main(args):
    if args.category:
        eval(args.action)([args.category])
    else:
        eval(args.action)([])
    return

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='run video_hotword_brush')
    parser.add_argument('-category', type=str, default='', help="category")
    parser.add_argument('-action', type=str, default='run',help="run:指定项目跑新词GPT")
    args = parser.parse_args()

    start_time = time.time()
    main(args)
    end_time = time.time()
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print('[{}] all done used:{:.2f}'.format(current_time, end_time - start_time))

'''
python models/tiktok/video_hotword_brush.py -action run
python models/tiktok/video_hotword_brush.py -action auto_run_monthly

#每周末用gpt刷关键词flag
0 0 * * 0 /usr/local/miniconda3/bin/python /data/www/DataCleaner/src/models/tiktok/video_hotword_brush.py -action auto_run_monthly > /data/www/DataCleaner/src/video_hotword_brush.log 2>&1
nohup /usr/local/miniconda3/bin/python /data/www/DataCleaner/src/models/tiktok/video_hotword_brush.py -action auto_run_monthly > /data/www/DataCleaner/src/video_hotword_brush.log 2>&1
'''