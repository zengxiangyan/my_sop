# coding=utf-8
import arrow
import argparse
import csv
import collections
import datetime
from datetime import date
import arrow
import json
import logging
logger = logging.getLogger('st_table')
import random
import sys
import time
from os.path import abspath, join, dirname
import os
from PIL import Image
import codecs

sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')
import application as app
from extensions import utils
from models.ocr.base import Base as ocr_model

table_ali_item_detail_log = 'cleaner.ali_item_detail_log'
table_ali_detail_image = 'cleaner.ali_detail_image'
dir_ali_detail_image = 'ali_detail_image'

def get_ocr_result(db, url, item_id=0):
    sql = "select id,content,is_done from {table} where url='{url}'".format(table=table_ali_detail_image, url=url)
    logger.info(sql)
    row = db.query_one(sql)
    #不存在时，需要插入一条url记录
    if row is not None and int(row[2]) == 1:
        return json.loads(row[1]) if row[1] is not None else {}

    if url == '':
        if row is not None:
            id = row[0]
            sql = "update {table} set is_done=4 where id={id}".format(table=table_ali_detail_image, id=id)
            db.execute(sql)
            db.commit()
        return {}

    now = time.time()
    image_filename = '{now}_{rand}.jpg'.format(now=now, rand=random.randint(100000, 999999))
    image_file = app.output_path(dir_ali_detail_image, image_filename)
    flag = utils.download_image(url, image_file)
    #没有下载成功
    if not flag:
        if row is not None:
            id = row[0]
            sql = "update {table} set is_done=2 where id={id}".format(table=table_ali_detail_image, id=id)
            db.execute(sql)
            db.commit()
        return {}

    size = os.path.getsize(image_file)

    img = Image.open(image_file)
    img_size = img.size  # 图片的长和宽
    if size < 10000 or img_size[0] < 100 or img_size[1] < 100:
        if row is not None:
            id = row[0]
            sql = "update {table} set is_done=3 where id={id}".format(table=table_ali_detail_image, id=id)
            db.execute(sql)
            db.commit()
        return {}

    obj = ocr_model()
    content = obj.get_result(image_file)
    print(content)
    if 'error_code' in content:
        #{'log_id': 1394095417670500352, 'error_msg': 'image format error', 'error_code': 216201}
        if content['error_code'] == 216201:
            if row is not None:
                id = row[0]
                sql = "update {table} set is_done=1,file=%s,error_code=%s,error_msg=%s where id={id}".format(table=table_ali_detail_image, id=id)
                db.execute(sql, (image_filename, str(content['error_code']), content['error_msg']))
                db.commit()
            else:
                item_vals = []
                item_vals.append((item_id, url, now, image_filename, str(content['error_code']), content['error_msg']))
                utils.easy_batch(db, table_ali_detail_image,
                                 ['item_id', 'url', 'createTime', 'file', 'error_code', 'error_msg'],
                                 item_vals, '')

        return content

    content2 = json.dumps(content, ensure_ascii=False)
    if row is not None:
        id = row[0]
        sql = "update {table} set content=%s,is_done=1,file=%s where id={id}".format(table=table_ali_detail_image, id=id)
        db.execute(sql, (content2,image_filename))
        db.commit()
    else:
        item_vals = []
        item_vals.append((item_id, url, now, content2, image_filename, 1))
        utils.easy_batch(db, table_ali_detail_image,
                         ['item_id', 'url', 'createTime', 'content', 'file', 'is_done'],
                         item_vals, '')
    return content

def get_item_data(db, item_id):
    a = arrow.get(time.time())
    end_time = utils.get_month_date_str(a.format('YYYY-MM-DD'), True)
    a = a.shift(months=-1)
    start_time = utils.get_month_date_str(a.format('YYYY-MM-DD'))
    table = 'ali.trade_all'
    sql = "select date,price/100 from {table} prewhere item_id={item_id} where pkey>='{start_time}' and pkey<'{end_time}' limit 1 by date".format(table=table, item_id=item_id, start_time=start_time, end_time=end_time)
    data = db.query_all(sql)
    x_data, y_data = [], []
    for row in data:
        x, y = list(row)
        x_data.append(x)
        y_data.append(y)
    return [x_data, y_data]

def get_detail_by_item_id(db, item_ids, ocr_direct=False):
    h = {}
    if len(item_ids) == 0:
        return h

    sql = 'select id, item_id,content,is_done from {table} where item_id in ({ids})'.format(table=table_ali_detail_image, ids=','.join([str(x) for x in item_ids]))
    data = db.query_all(sql)

    for row in data:
        id, item_id, content, is_done = list(row)
        item_id = str(item_id)
        if content == '' or content is None:
            continue
        print(row)
        # content = re.sub(r'\\"', '', content)
        try:
            content = json.loads(content)
        except:
            content = codecs.decode(content, 'unicode_escape')
            content = json.loads(content)
        if content is None or not content or 'words_result' not in content:
            continue
        content = content['words_result']
        l = []
        for v in content:
            l.append(v['words'])
        s = ' '.join(l)
        if item_id not in h:
            h[item_id] = s
        else:
            h[item_id] += ' ' + s
    return h

def get_content(content, str_split=' '):
    try:
        content = json.loads(content)
    except:
        content = codecs.decode(content, 'unicode_escape')
        content = json.loads(content)
    if content is None or not content or 'words_result' not in content:
        return ''
    content = content['words_result']
    l = []
    for v in content:
        l.append(v['words'])
    s = str_split.join(l)
    return s






