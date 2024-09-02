import sys
import time
import datetime
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')
import models.plugins.batch as Batch
import application as app


class main(Batch.main):

    def calc_splitrate(self, item, data):
        custom_price = {
            68:35,
            1018:75,
            3155:35,
            3422:35,
            3424:50,
        }
        return self.cleaner.calc_splitrate(item, data, custom_price)


    def brush_modify(self, v, bru_items):
        x1, x2, x3 = [], [], []
        for vv in v['split_pids']:
            sku_name = vv['sp20'] or vv['sku_name']
            if vv['sp1'] == '其它' or vv['sp1'] == '':
                sku_name = '其它'
            x1.append(sku_name+'*'+str(vv['number']))
            x2.append(vv['sp1'])
            x3.append(vv['sp19'])

        sp11 = ''
        if len(v['split_pids']) == 1 and v['split_pids'][0]['number'] == 1:
            sp11 = 'Single'
        elif len(v['split_pids']) == 1:
            sp11 = 'Bundle-same SKU'
        elif len(list(set(x2))) > 1:
            sp11 = 'Bundle-cross category'
        elif len(list(set(x3))) > 1:
            sp11 = 'Bundle-different SPUs'
        else:
            sp11 = 'Bundle-same SPU'

        x1.sort()
        for vv in v['split_pids']:
            vv['sp24'] = '+'.join(x1)
            vv['sp11'] = sp11


    def finish_new(self, tbl, dba, prefix):
        sql = '''
            ALTER TABLE {} UPDATE
                `sp疑似新品时间-报告用属性` = `sp疑似新品`
            WHERE 1
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)
