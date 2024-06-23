#coding=utf-8
import os
import gc
import sys
import time
import ujson
import math
import traceback
import datetime
import random
import signal
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

from multiprocessing import Pool, Process, Queue

from models.cleaner import Cleaner
from models.clean_batch import CleanBatch
from models.batch_task import BatchTask
import application as app


class AllClean(Cleaner):
    def __init__(self, bid, eid=None, skip=False):
        super().__init__(bid, eid)


    def get_tbl(self):
        return 'chmaster', 'sop_c.entity_prod_1_C'

            # batch_now = CleanBatch(self.bid, print_out=False)
            # batch_now.get_config()
            # batch_now.check_lack_fields()
            # self.cache['batch_now'] = batch_now


    @staticmethod
    def import_batch_partation(where, params, prate):
        bid, rbid, atbl, ctbl, spn, sps1, sps2, logId, = params

        cln = AllClean(bid)
        name, tbl = cln.get_tbl()
        dba = cln.get_db(name)

        sql = '''
            INSERT INTO {} (
                batch_id,source,date,item_id,trade_props.name,trade_props.value,shop_type,cid,all_bid,alias_all_bid,clean_ver,clean_type,all_bid_sp,alias_all_bid_sp,pid,mid_props.name,mid_props.value,cln_props.name,cln_props.value,created)
            SELECT
                {},source,date,item_id,trade_props.name,trade_props.value,shop_type,cid,all_bid,alias_all_bid,clean_ver,clean_type,all_bid_sp,alias_all_bid_sp,pid,mid_props.name,mid_props.value,cln_props.name,cln_props.value,created
            FROM (
                SELECT uuid,source,date,item_id,cid,all_bid,alias_all_bid,clean_ver,clean_type,all_bid_sp,alias_all_bid_sp,pid,[{spn}] AS `mid_props.name`,[{}] AS `mid_props.value`,[{spn}] AS `cln_props.name`,[{}] AS `cln_props.value`,created
                FROM {} WHERE ({where})
            ) a JOIN (
                SELECT uuid,shop_type,trade_props.name,trade_props.value
                FROM {} WHERE ({where}) AND sign = 1
            ) b USING uuid
        '''.format(tbl, rbid, sps1, sps2, ctbl, atbl, spn=spn, where=where)
        dba.execute(sql)


    def import_batch(self, bid, include_sp=[]):
        cln = Cleaner(bid)
        aname, atbl = cln.get_plugin().get_a_tbl()
        cname, ctbl = cln.get_plugin().get_c_tbl()
        cdba = self.get_db(cname)

        poslist = cln.get_poslist()

        if bid == 0:
            # test
            nmap = {'SKU-出数专用': 'SKU'}
        else:
            nmap = {}

        spn, sps = [], []
        for pos in poslist:
            if len(include_sp) > 0 and pos not in include_sp:
                continue
            n = poslist[pos]['name']
            spn.append(nmap[n] if n in nmap else n)
            sps.append(pos)

        spn, sps1, sps2 = ','.join(['\'{}\''.format(n) for n in spn]), ','.join(['mp{}'.format(p) for p in sps]), ','.join(['sp{}'.format(p) for p in sps])
        params = [self.bid, bid, atbl, ctbl, spn, sps1, sps2, 0]

        sql = 'SELECT min(date), addDays(max(date), 1) FROM {}'.format(ctbl)
        ret = cdba.query_all(sql)

        cln.foreach_partation_new(ret[0][0], ret[0][1], AllClean.import_batch_partation, params, multi=1, limit=1000000)
