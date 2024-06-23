#coding=utf-8
import sys
import json
import math
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))

from models.cleaner import Cleaner
from models.clean import Clean
from models.brush import Brush
from models.market import Market
from models.clean_sop import CleanSop
from models.brush_spu import BrushSpu
from models.market_spu import MarketSpu

import application as app

def use_all(bid, eid=0):
    db26 = app.get_db('26_apollo')
    db26.connect()

    if eid is not None and eid > 0:
        sql = 'SELECT use_all_table FROM cleaner.clean_batch WHERE eid = {}'.format(eid)
        ret = db26.query_all(sql)
    else:
        sql = 'SELECT use_all_table FROM cleaner.clean_batch WHERE batch_id = {}'.format(bid)
        ret = db26.query_all(sql)

    return ret[0][0] == 1

def get_clean(bid, eid=None, force=False):
    if bid == 270:
        return CleanSop(bid)
    return Clean(bid, eid)

def get_entity(bid):
    return Cleaner(bid)

def get_brush(bid=None, eid=None, skip=False, force=False):
    return Brush(bid, eid, skip)

def get_market(bid=None, eid=None, force=False):
    if bid == 210:
        return MarketSpu(bid, eid)
    return Market(bid, eid)