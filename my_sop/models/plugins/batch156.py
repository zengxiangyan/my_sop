import os
import sys
import csv
import time
import traceback
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import random

class main(Batch.main):

    def clean(self, smonth, emonth, logId = -1, force=False):
        if logId == -1:
            status, process = self.cleaner.get_status('clean')
            if force == False and status not in ['error', 'completed', '']:
                raise Exception('clean {} {}%'.format(status, process))
                return

            logId = self.cleaner.add_log('clean', 'process ...')
            try:
                self.clean(smonth, emonth, logId=logId)
            except Exception as e:
                error_msg = traceback.format_exc()
                self.cleaner.set_log(logId, {'status':'error', 'msg':error_msg})
                raise e
            return

        aname, atbl = self.get_a_tbl()
        cname, ctbl = self.get_c_tbl()
        cdba = self.cleaner.get_db(cname)

        acols = self.cleaner.get_cols(atbl, cdba)
        ccols = self.cleaner.get_cols(ctbl, cdba, ['created'])

        cols, f_cols = [], []
        for col in ccols:
            cols.append(col)
            if col not in acols and col not in ('sp1', 'sp4', 'sp5', 'sp6', 'sp7', 'sp8'):
                col = self.cleaner.safe_insert(ccols[col], '', insert_mod=True)
                f_cols.append(col)
            else:
                f_cols.append(col)

        sql = 'DROP TABLE IF EXISTS {}_tmp'.format(ctbl)
        cdba.execute(sql)

        sql = 'CREATE TABLE {t}_tmp AS {t}'.format(t=ctbl)
        cdba.execute(sql)

        sql = '''
            INSERT INTO {}_tmp ({})
            WITH '女鞋' AS sp1,
                 arrayFilter((v, x) -> x = '靴款品名' AND v != '', `trade_props.value`, `trade_props.name`) AS a_sp4_1,
                 arrayFilter((v, x) -> x = '款式' AND v != '', `trade_props.value`, `trade_props.name`) AS a_sp4_2,
                 arrayFilter((v, x) -> x = '靴筒面材质' AND v != '', `trade_props.value`, `trade_props.name`) AS a_sp5_1,
                 arrayFilter((v, x) -> x = '帮面材质' AND v != '', `trade_props.value`, `trade_props.name`) AS a_sp5_2,
                 arrayFilter((v, x) -> x = '后跟高' AND v != '', `trade_props.value`, `trade_props.name`) AS a_sp6,
                 arrayFilter((v, x) -> x = '场合' AND v != '', `trade_props.value`, `trade_props.name`) AS a_sp7_1,
                 arrayFilter((v, x) -> x = '适合场合' AND v != '', `trade_props.value`, `trade_props.name`) AS a_sp7_2,
                 arrayFilter((v, x) -> x = '适用场景' AND v != '', `trade_props.value`, `trade_props.name`) AS a_sp7_3,
                 arrayFilter((v, x) -> x = '鞋头款式' AND v != '', `trade_props.value`, `trade_props.name`) AS a_sp8,
                 arrayFilter((v, x) -> x = '靴款品名' AND v != '', `props.value`, `props.name`) AS b_sp4_1,
                 arrayFilter((v, x) -> x = '款式' AND v != '', `props.value`, `props.name`) AS b_sp4_2,
                 arrayFilter((v, x) -> x = '靴筒面材质' AND v != '', `props.value`, `props.name`) AS b_sp5_1,
                 arrayFilter((v, x) -> x = '帮面材质' AND v != '', `props.value`, `props.name`) AS b_sp5_2,
                 arrayFilter((v, x) -> x = '后跟高' AND v != '', `props.value`, `props.name`) AS b_sp6,
                 arrayFilter((v, x) -> x = '场合' AND v != '', `props.value`, `props.name`) AS b_sp7_1,
                 arrayFilter((v, x) -> x = '适合场合' AND v != '', `props.value`, `props.name`) AS b_sp7_2,
                 arrayFilter((v, x) -> x = '适用场景' AND v != '', `props.value`, `props.name`) AS b_sp7_3,
                 arrayFilter((v, x) -> x = '鞋头款式' AND v != '', `props.value`, `props.name`) AS b_sp8,
                 IF(LENGTH(a_sp4_1)=0,b_sp4_1,a_sp4_1) AS c_sp4_1,
                 IF(LENGTH(a_sp4_2)=0,b_sp4_2,a_sp4_2) AS c_sp4_2,
                 IF(LENGTH(a_sp5_1)=0,b_sp5_1,a_sp5_1) AS c_sp5_1,
                 IF(LENGTH(a_sp5_2)=0,b_sp5_2,a_sp5_2) AS c_sp5_2,
                 IF(LENGTH(a_sp6)=0,b_sp6,a_sp6) AS c_sp6,
                 IF(LENGTH(a_sp7_1)=0,b_sp7_1,a_sp7_1) AS c_sp7_1,
                 IF(LENGTH(a_sp7_2)=0,b_sp7_2,a_sp7_2) AS c_sp7_2,
                 IF(LENGTH(a_sp7_3)=0,b_sp7_3,a_sp7_3) AS c_sp7_3,
                 IF(LENGTH(a_sp8)=0,b_sp8,a_sp8) AS c_sp8,
                 IF(cid IN (50012047,50012028,201312504,201312704,201315801,201323801), c_sp4_1, c_sp4_2) AS d_sp4,
                 IF(cid IN (50012028,201312504,201312704,201315801,201323801) AND LENGTH(c_sp5_1) > 0, c_sp5_1, c_sp5_2) AS d_sp5,
                 multiIf(cid IN (50012042), b_sp7_1, cid IN (50012032,201273400,201304907,50012033,201273891,201276457,201284013,201315801), b_sp7_2, b_sp7_3) AS d_sp7,
                 arrayStringConcat(arraySort(arrayDistinct(d_sp4)), ' ') AS sp4,
                 arrayStringConcat(arraySort(arrayDistinct(d_sp5)), ' ') AS sp5,
                 arrayStringConcat(arraySort(arrayDistinct(c_sp6)), ' ') AS sp6,
                 arrayStringConcat(arraySort(arrayDistinct(d_sp7)), ' ') AS sp7,
                 arrayStringConcat(arraySort(arrayDistinct(c_sp8)), ' ') AS sp8
            SELECT {} FROM {t} WHERE {w} AND uuid2 NOT IN (SELECT uuid2 FROM {t} WHERE {w} AND sign = -1)
        '''.format(ctbl, ','.join(cols), ','.join(f_cols), t=atbl, w='pkey>=\'{}\' AND pkey<\'{}\''.format(smonth, emonth))
        cdba.execute(sql)

        # replace
        sql = 'SELECT source, pkey FROM {}_tmp GROUP BY source, pkey'.format(ctbl)
        ret = cdba.query_all(sql)

        for source, pkey, in ret:
            sql = 'ALTER TABLE {t} REPLACE PARTITION ({},\'{}\') FROM {t}_tmp'.format(source, pkey, t=ctbl)
            cdba.execute(sql)

        sql = 'DROP TABLE {}_tmp'.format(ctbl)
        cdba.execute(sql)

        self.cleaner.add_log('clean', 'completed', '', process=100, logId=logId)