import sys
import time
import json
import datetime
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app

class main(Batch.main):

    # def pre_brush_modify(self, v, products, prefix):
    #     if v['visible'] != 3 or v['flag'] != 2:
    #         v['flag'] = 0


    def brush_modify(self, v, bru_items):
        # sp25（总颗数_新）人工答的式子（例如：5*2）
        for vv in v['split_pids']:
            if vv['sp25'] != '':
                vv['sp25'] = str(eval(vv['sp25'].replace('颗','').strip()))
            if vv['sp27'] != '':
                vv['sp27'] = str(eval(vv['sp27'].replace('颗','').replace('g','').strip()))


    def hotfix_new(self, tbl, dba, prefix):
        # sp27（总克数）=sp25（总颗数_新）*sp26（单颗克数）
        sql = '''
            ALTER TABLE {} UPDATE `sp总克数` = toString(round(toUInt32OrZero(`sp总颗数_新`) * toFloat32OrZero(`sp单颗克数`),2))
            WHERE `sp总颗数_新` != '' AND `sp单颗克数` != '' AND `sp总克数` = ''
            SETTINGS mutations_sync = 1
        '''.format(tbl)
        dba.execute(sql)