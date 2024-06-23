import sys
import time
import datetime
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

    def brush_1212(self, smonth, emonth, logId=-1):
        uuids = []
        sps = [['瞬感', 800], ['美奇', 500], ['美预安', 100]]
        c = []

        for sp1,limit in sps:
            d = 0
            where = 'uuid2 in (select uuid2 from {} where sp1 = \'{}\')'.format(self.get_clean_tbl(), sp1)
            ret, sales_by_uuid = self.cleaner.process_top(smonth, emonth, limit=limit, where=where)
            for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
                # if self.skip_brush(source, tb_item_id, p1, if_flag=False):
                #     continue
                uuids.append(uuid2)
                d = d+ 1
            c.append(d)
        print(c)
        clean_flag = self.cleaner.last_clean_flag() + 1
        # self.cleaner.add_brush(uuids, clean_flag, 1)
        return True

    def brush(self, smonth, emonth, logId=-1):
        cname, ctbl = self.get_c_tbl()
        cc = {}

        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        # sp5s = ['安耐糖Anytime', '美预安', '瞬感', '微探', '颐健安美奇']
        # sp5s = ['安耐糖Anytime', '美预安', '微探', '颐健安美奇']
        # sp5s = ['瞬感']
        sp5s = ['安耐糖Anytime', '美预安', '微探', '颐健安美奇', '硅基动感', '微泰动泰']
        # for ssmonth, eemonth in [['2019-01-01', '2020-01-01'], ['2020-01-01','2021-01-01'], ['2021-01-01','2022-01-01']]:
        for ssmonth, eemonth in [['2022-07-01', '2022-12-01']]:
            for each_source in ['source = 1 and (shop_type > 20 or shop_type < 10 )', 'source=2']:
                for each_sp5 in sp5s:
                    uuids = []
                    where = '''
                    uuid2 in (select uuid2 from {ctbl} where pkey>='{ssmonth}' and pkey<'{eemonth}' and sp5='{each_sp5}') and {each_source}
                    '''.format(ctbl=ctbl, ssmonth=ssmonth, eemonth=eemonth, each_source=each_source, each_sp5=each_sp5)
                    ret, sales_by_uuid1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=10000)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            continue
                        else:
                            uuids.append(uuid2)
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

                    cc['{}~{}@{}@{}'.format(ssmonth, eemonth, each_sp5, str(each_source))] = [len(uuids)]
                    self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=1, sales_by_uuid=sales_by_uuid1)

        sp5s = ['瞬感']
        # for ssmonth, eemonth in [['2019-01-01', '2020-01-01'], ['2020-01-01','2021-01-01'], ['2021-01-01','2022-01-01']]:
        for ssmonth, eemonth in [['2022-07-01', '2022-12-01']]:
            for each_source in ['source = 1 and (shop_type > 20 or shop_type < 10 )', 'source=2']:
                for each_sp5 in sp5s:
                    uuids = []
                    where = '''
                            uuid2 in (select uuid2 from {ctbl} where pkey>='{ssmonth}' and pkey<'{eemonth}' and sp5='{each_sp5}') and {each_source}
                            '''.format(ctbl=ctbl, ssmonth=ssmonth, eemonth=eemonth, each_source=each_source,
                                       each_sp5=each_sp5)
                    ret, sales_by_uuid1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=10000,
                                                                        rate=0.8)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            continue
                        else:
                            uuids.append(uuid2)
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

                    cc['{}~{}@{}@{}'.format(ssmonth, eemonth, each_sp5, str(each_source))] = [len(uuids)]
                    self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=1, sales_by_uuid=sales_by_uuid1)

        for i in cc:
            print(i, cc[i])

        return True


    def brush_modify(self, v, bru_items):
        if v['flag'] == 2 and v['split'] > 0:
            v['split'] = 0

        vvv, sp13, sp14, sp15 = None, 0, 0, 0
        for vv in v['split_pids']:
            if vv['sp6'] == '传感器':
                sp13 += vv['number']
            if vv['sp6'] == '扫描仪':
                sp14 += vv['number']
            if vv['sp6'] == '发射器':
                sp15 += vv['number']
            vvv = vv

        if vvv is not None:
            vvv['sp13'] = sp13
            vvv['sp14'] = sp14
            vvv['sp15'] = sp15
            vvv['split_rate'] = 1
            v['split_pids'] = [vvv]


    def hotfix_new(self, tbl, dba, prefix):
        sql = '''
            ALTER TABLE {} UPDATE
                `spSKU名` = '机洗无法识别', `sp传感器` = '0', `sp扫描仪` = '0', `sp发射器` = '0'
            WHERE clean_sales/clean_num/(toInt32OrZero(`sp传感器`)+toInt32OrZero(`sp扫描仪`)+toInt32OrZero(`sp发射器`)) < 30000
              AND `spSKU`='瞬感'
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)


    def finish_new(self, tbl, dba, prefix):
        sql = '''
            ALTER TABLE {} UPDATE `sp子品类` = '其它'
            WHERE clean_sales/clean_num < 30000 AND `spSKU`='瞬感'
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)