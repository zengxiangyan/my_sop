#coding=utf-8
import sys
from os.path import abspath, join, dirname
import pandas as pd
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
import application as app
import arrow
import csv
import json
import time
import copy
import requests
from datetime import datetime
from extensions import utils
from pathlib import Path
from models.tiktok import config as tconfig
from models.tiktok.table_name import *

class Item(object):
    def __init__(self, dy2, chsop, category, with_split_sales=2, debug=False):
        self.dy2 = dy2
        self.chsop = chsop
        self.category = category
        self.with_split_sales = with_split_sales
        self.debug = debug
        if category not in tconfig.h_eid_ref:
            self.eid = 0
            return
        eid_info = tconfig.h_eid_ref[category]
        self.eid = eid_info[0]
        self.table_e = '{}.{}'.format('sop_e',eid_info[2])
        self.table_spu = '{}_spu'.format(self.table_e)

    def get_item_sales(self, data, h_aweme_id_pid1, h_pnvid_aweme_id2, mode=0):
        '''
        data:[aweme_id,create_time,digg_count], h_aweme_id_pid: {aweme_id: {pid: 1}} h_pnvid_aweme_id2: {pnvid: [aweme_id, pid]}
        mode 0: 照视频id看 1： 照属性看
        :return:
        '''
        if self.eid == 0:
            return {}
        h_aweme_id = {x[0]:x[1:] for x in data}
        h_aweme_id_day, h_day_aweme_id = self.get_aweme_id_time(data)
        h_pid1 = {}
        for aweme_id in h_aweme_id_pid1:
            for x in h_aweme_id_pid1[aweme_id]:
                h_pid1[x] = 1
        h_pid1_pid2, h_pid2_pid1, h_pid2_pid, h_pid_pid2 = self.get_pid_ref(h_pid1)
        h_aweme_id_pid, h_pid_aweme_id = self.get_aweme_id_pid(h_aweme_id_pid1, h_pid1_pid2, h_pid2_pid)
        h_pid_day_sales = self.get_sku_sales(h_pid_aweme_id.keys())
        h_pid_day_rate = get_sku_sales_rate(self.with_split_sales, h_pid_day_sales, h_day_aweme_id, h_pid_aweme_id, h_aweme_id)
        if mode == 0:
            h = self.get_aweme_id_sales(h_aweme_id, h_aweme_id_pid, h_pid_day_rate, h_aweme_id_day, h_pid_day_sales)
        else:
            h = self.get_prop_sales(h_aweme_id, h_aweme_id_pid, h_pid_day_rate, h_aweme_id_day, h_pid_day_sales, h_pnvid_aweme_id2, h_pid1_pid2, h_pid2_pid)
        return h

    def get_aweme_id_time(self, data, with_split_sales=2, days_sales=14):
        h_aweme_id_day = {}
        h_day_aweme_id = {} #每一天的视频，用于拆分销额的时候
        for row in data:
            aweme_id, create_time, *else_args = list(row)
            start = arrow.get(create_time)
            # end = start.shift(days=8)
            end = start.shift(days=days_sales)
            h_aweme_id_day[aweme_id] = []

            for r in arrow.Arrow.span_range('day', start, end):
                ms = r[0].format('YYYY-MM-DD')
                h_aweme_id_day[aweme_id].append(ms)
                if with_split_sales > 0:
                    utils.get_or_new(h_day_aweme_id, [ms, aweme_id], 1)
        return h_aweme_id_day, h_day_aweme_id

    def get_pid_ref(self, h_pid1):
        dy2 = self.dy2
        chsop = self.chsop
        category = self.category
        eid_info = tconfig.h_eid_ref[category]
        eid = eid_info[0]
        table_pid = get_pid_table(category)
        sql = "select pid,alias_pid,name from {table} where alias_pid!='' and pid in ({pid_str})".format(
            table=table_pid,
            pid_str=','.join([str(x) for x in h_pid1])
        )
        data = dy2.query_all(sql)
        # h_sku_name = {x[0]:x[2] for x in data}
        h_pid1_pid2, h_pid2_pid1 = utils.get_two_ref(data, split=True)
        # pid1_str = ','.join([str(x) for x in h_pid1_pid2.keys()])
        pid2_str = ','.join([str(x) for x in h_pid2_pid1.keys()])
        print('pid2_str:', pid2_str)
        if len(pid2_str) == 0:
            data = {}
        else:
            sql = 'select pid,name,alias_pid from artificial.product_{eid} where pid in ({pid_str})'.format(
                eid=eid,
                pid_str=pid2_str
            )
            data = chsop.query_all(sql)
        h_pid2_pid, h_pid_pid2 = utils.get_two_ref(data, idx1=0, idx2=2)
        return h_pid1_pid2, h_pid2_pid1, h_pid2_pid, h_pid_pid2

    def get_sku_sales(self, l_pid):
        chsop = self.chsop
        table = self.table_spu
        pid_str = ','.join([str(x) for x in l_pid])
        if len(pid_str) == 0:
            data = {}
        else:
            sql = "select alias_pid, date, num, sales from {table} where alias_pid in ({pid_str})".format(table=table, pid_str=pid_str)
            data = chsop.query_all(sql)
        h = {}
        for row in data:
            alias_pid, day, num, sales = list(row)
            h_sub = utils.get_or_new(h, [alias_pid, str(day)], [num, sales])
        return h

    def get_aweme_id_pid(self, h_aweme_id_pid1, h_pid1_pid2, h_pid2_pid):
        # 获取aweme_id和pid的关系，后续方便使用
        h_aweme_id_pid, h_pid_aweme_id = {}, {}
        for aweme_id in h_aweme_id_pid1:
            for pid1 in h_aweme_id_pid1[aweme_id]:
                if pid1 not in h_pid1_pid2:
                    continue
                for pid2 in h_pid1_pid2[pid1]:
                    for pid in h_pid2_pid[pid2]:
                        utils.get_or_new(h_aweme_id_pid, [aweme_id, pid], 1)
                        utils.get_or_new(h_pid_aweme_id, [pid, aweme_id], 1)
        return h_aweme_id_pid, h_pid_aweme_id

    def get_aweme_id_sales(self, h_aweme_id, h_aweme_id_pid, h_pid_day_rate, h_aweme_id_day, h_pid_day_sales):
        with_split_sales = self.with_split_sales
        h_aweme_id_sales = {}
        for aweme_id in h_aweme_id_day:
            c1 = h_aweme_id[aweme_id][-1] #todo: 之后变更为index
            if c1 == 0:
                continue
            num, sales = 0, 0
            for day in h_aweme_id_day[aweme_id]:
                if aweme_id not in h_aweme_id_pid:
                    continue
                for pid in h_aweme_id_pid[aweme_id]:
                    c2 = h_pid_day_rate[pid][day] if with_split_sales > 0 and pid in h_pid_day_rate and day in h_pid_day_rate[pid] else 1
                    if pid not in h_pid_day_sales or day not in h_pid_day_sales[pid]:
                        continue
                    row2 = h_pid_day_sales[pid][day]
                    if c1 == 0 and c2 == 0:
                        continue
                    print('pid:', pid, 'day:', day, 'c1:', c1, 'c2:', c2)
                    num += row2[0] * c1 / c2
                    sales += row2[1] * c1 / c2
            h_aweme_id_sales[aweme_id] = [int(num), int(sales/100)]
        return h_aweme_id_sales

    def get_prop_sales(self, h_aweme_id, h_aweme_id_pid, h_pid_day_rate, h_aweme_id_day, h_pid_day_sales, h_pnvid_aweme_id2, h_pid1_pid2, h_pid2_pid):
        with_split_sales = self.with_split_sales
        l = []
        h_pnvid_sales = {}
        for pnvid in h_pnvid_aweme_id2:
            h_pid_day = {}  # 最终时间段
            h_pid1_count = {}
            h_pid_day_aweme_id = {}  # 该属性下面的统计
            for aweme_id, pid1 in h_pnvid_aweme_id2[pnvid]:
                h_pid1_count[pid1] = 1
                if pid1 not in h_pid1_pid2:
                    continue
                for pid2 in h_pid1_pid2[pid1]:
                    for pid in h_pid2_pid[pid2]:
                        if pid not in h_pid_day:
                            h_pid_day[pid] = {}
                        for day in h_aweme_id_day[aweme_id]:
                            h_pid_day[pid][day] = 1
                            utils.get_or_new(h_pid_day_aweme_id, [pid, day, aweme_id], 1)

            num, sales = 0, 0
            for pid in h_pid_day:
                for day in h_pid_day[pid]:
                    if pid not in h_pid_day_sales or day not in h_pid_day_sales[pid]:
                        continue
                    row2 = h_pid_day_sales[pid][day]
                    c2 = h_pid_day_rate[pid][day] if with_split_sales > 0 and pid in h_pid_day_rate and day in h_pid_day_rate[pid] else 1
                    if with_split_sales == 0:
                        c1 = 1
                    else:
                        c1 = 0
                        for aweme_id in h_pid_day_aweme_id[pid][day]:
                            if with_split_sales == 1:
                                c1 += 1
                            else:
                                c1 += h_aweme_id[aweme_id][-1]

                    if c1 == 0 and c2 == 0:
                        continue
                    print('pid:', pid, 'day:', day, 'c1:', c1, 'c2:', c2)
                    num += row2[0] * c1 / c2
                    sales += row2[1] * c1 / c2
            h_pnvid_sales[pnvid] = [num, sales]
        return h_pnvid_sales


def get_sku_sales_rate(with_split_sales, h_pid_day_sales, h_day_aweme_id, h_pid_aweme_id, h_aweme_id):
    '''
    拆分spu每一天销额时，用到的分母
    :param with_split_sales: 1:视频数 2：视频转评赞数 3: 粉丝数 4:播放量
    :param h_pid_day_sales:
    :param h_day_aweme_id:
    :param h_pid_aweme_id:
    :param h_aweme_id:
    :return:
    '''
    h = {}
    for pid in h_pid_day_sales:
        if pid not in h_pid_aweme_id:
            continue
        for day in h_pid_day_sales[pid]:
            if day not in h_day_aweme_id:   #该天的视频数没有，直接跳出
                continue
            c = 0
            for aweme_id in h_day_aweme_id[day]:
                if aweme_id not in h_pid_aweme_id[pid]:
                    continue
                if with_split_sales == 1:
                    c += 1
                else:
                    c += h_aweme_id[aweme_id][-1]
            utils.get_or_new(h, [pid, day], c)
    return h

if __name__ == "__main__":
    from models.tiktok.item import Item
    dy2 = app.connect_db('dy2')
    chsop = app.connect_clickhouse('chsop')
    category = 26396
    table = get_douyin_video_table(category)
    table_brand = get_douyin_video_brand_table(category)
    table_sub_cid = get_douyin_video_sub_cid_table(category)
    obj = Item(dy2, chsop, category, with_split_sales=4)
    sql = "select aweme_id from {table} where create_time>=unix_timestamp('2022-07-01') and aweme_id in (select aweme_id from {table_brand} where bid in (52734,52746,5536723,6198802,5638768,51972,5452406,59539,1110309,52747,3041542,474001,52049,94852,3197123))  and aweme_id in (select aweme_id from {table_sub_cid} where sub_cid not in (99,999))".format(
        table=table,
        table_brand=table_brand,
        table_sub_cid=table_sub_cid
    )
    data = dy2.query_all(sql)
    l_aweme_id = [7149114148322921766, 7149131807861099807]
    l_aweme_id = [x[0] for x in data]
    aweme_id_str = ','.join([str(x) for x in l_aweme_id])
    #digg_count+share_count+comment_count
    #play_count
    sql = 'select aweme_id,create_time,play_count from {table} where aweme_id in ({aweme_id_str})'.format(table=table, aweme_id_str=aweme_id_str)
    data = dy2.query_all(sql)

    table_pid = get_douyin_video_pid_table(category)
    sql = 'select aweme_id, pid from {table} where aweme_id in ({aweme_id_str})'.format(table=table_pid, aweme_id_str=aweme_id_str)
    data2 = dy2.query_all(sql)
    h_aweme_id_pid1, h_pid1_aweme_id = utils.get_two_ref(data2)

    table_prop = get_douyin_video_prop_table(category)
    sql = 'select aweme_id,pid,pnvid from {table_prop} a where aweme_id in ({aweme_id_str})'.format(table_prop=table_prop, aweme_id_str=aweme_id_str)
    data3 = dy2.query_all(sql)
    h_pnvid_aweme_id2 = {}
    for row in data3:
        aweme_id, pid, pnvid = list(row)
        if pnvid not in h_pnvid_aweme_id2:
            h_pnvid_aweme_id2[pnvid] = []
        h_pnvid_aweme_id2[pnvid].append([aweme_id, pid])

    print(data)
    print(h_aweme_id_pid1)
    print(h_pnvid_aweme_id2)
    r = obj.get_item_sales(data, h_aweme_id_pid1, h_pnvid_aweme_id2, mode=0)
    print(r)
    base_dir = Path(app.output_path('dy2'))
    filename = base_dir / 'dy_item.csv'
    l = []
    for aweme_id in r:
        l.append([aweme_id] + list(r[aweme_id]))
    utils.easy_csv_write(filename, l, ['aweme_id', 'num', 'sales'])
    print('output filename:', filename)