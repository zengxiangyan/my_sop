import sys
import time
import datetime
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')
import models.plugins.batch as Batch
import application as app


class main(Batch.main):
    # 答题结果特殊修改
    def brush_modify(self, v, bru_items):
        for vv in v['split_pids']:
            if v['flag'] == 1:
                vv['sp15'] = vv['sp12']
            elif vv['sp13']:
                vv['sp15'] = str(int(vv['sp13']) * vv['number'])


    def hotfix_new(self, tbl, dba, prefix):
        self.cleaner.add_miss_cols(tbl, {'spsku最早出现时间':'String'})

        dba.execute('DROP TABLE IF EXISTS {}_join'.format(tbl))

        dba.execute('''
            CREATE TABLE {t}_join ENGINE = Join(ANY, LEFT, clean_sku_id) AS
            SELECT clean_sku_id, IF(toYear(min(date))%100=17,'17年及以前',concat(toString(toYear(min(date))%100),'年')) m
            FROM {t} WHERE clean_sku_id > 0 GROUP BY clean_sku_id
        '''.format(t=tbl))

        sql = '''
            ALTER TABLE {t} UPDATE `spsku最早出现时间` = ifNull(joinGet('{t}_join', 'm', clean_sku_id),'')
            WHERE 1
        '''.format(t=tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        dba.execute('DROP TABLE IF EXISTS {}_join'.format(tbl))