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
    # 拆套包 默认按照number均分 返回 [[rate, id], ...]
    def calc_splitrate_new(self, item, data):
        total_brand = len(list(set([v['alias_all_bid'] for v in data])))
        total_sku = len(list(set([v['pid'] for v in data])))

        if len(data) > 1:
            for v in data:
                sp6 = v['sp6'].lower().replace('ml', '')
                sp6 = 0 if sp6 == '' else float(sp6)
                sp6 = 0 if sp6  < 50 else sp6
                v['split_rate'] = sp6 * v['number']

        total = sum([v['split_rate'] for v in data])
        for v in data:
            split_rate = 0 if total == 0 else v['split_rate'] / total
            v['split_rate'] = split_rate
            v['sp15'] = 'Multi Cross Brand' if total_brand > 1 else ('Multi Same Brand' if total_sku > 1 else 'Multi Same Sku')
        return data


    def hotfix_new(self, tbl, dba, prefix):
        self.cleaner.add_miss_cols(tbl, {'spBulk Sales':'String','sp出数用sku毫升数':'String'})

        sql = '''
            ALTER TABLE {} UPDATE `spBulk Sales` = '否' WHERE 1
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `sp出数用sku毫升数` = `sp单瓶规格(ml)` WHERE `sp出数用sku毫升数` = ''
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `spBulk Sales` = '是'
            WHERE clean_alias_all_bid in (139066,380545) AND sid in (194641035,189367760,1000140024,188438,192723120,9730213,1000097462,883600)
              AND clean_pid > 0 AND clean_brush_id > 0
              AND `sp人头马_子品类`='干邑白兰地' AND `sp人头马_三厂商干邑级别` != 'LXIII'
              AND clean_price/100/toInt32OrZero(`sp出数用内包装瓶数`) >=20000
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)


    def pre_brush_modify(self, v, products, prefix):
        for pid in products:
            if products[pid]['spid21'] != '':
                break
            v = products[pid]['spid6'].split('*')
            products[pid]['spid21'] = v[0]
            products[pid]['spid22'] = '1' if len(v) == 1 else v[1]


    def brush_modify(self, v, bru_items):
        for vv in v['split_pids']:
            if vv['sp31'] != '':
                v5 = vv['sp5'].replace('瓶','') or 1
                v32 = eval(vv['sp31'].replace('ml',''))
                vv['sp32'] = '{:g}ml'.format(round(v32))
                vv['sp6'] = '{:g}ml'.format(round(v32/int(v5)))
            elif vv['sp6'] != '':
                v6 = eval(vv['sp6'].replace('ml',''))
                vv['sp6'] = '{:g}ml'.format(round(v6))