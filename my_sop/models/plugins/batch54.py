import sys
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import time
import models.plugins.batch as Batch
import application as app

class main(Batch.main):
    def finish(self, tbl, dba, prefix):
        # cpu，显卡的型号全部导入的
        # 颜色，厚度导入top80%，其他都归成 ‘其他’
        sql = 'SELECT min(pkey), max(pkey), sum(c_sales) * 0.8 FROM {} GROUP BY toYYYYMM(pkey)'.format(tbl)
        ret = dba.query_all(sql)

        for smonth, emonth, ss, in ret:
            sql = '''
                WITH arrayReduce('BIT_XOR',arrayMap(x->crc32(x),arrayFilter(y->trim(y)<>'',arrayReduce('groupUniqArray',trade_props.value)))) AS p1
                SELECT groupArray(uuid2), SUM(c_sales) ss FROM {} WHERE pkey >= '{}' AND pkey <= '{}' GROUP BY source, item_id, p1 ORDER BY ss DESC
            '''.format(tbl, smonth, emonth)
            rrr = dba.query_all(sql)

            uuids = []
            for uids, s, in rrr:
                uuids += list(uids)
                ss -= s
                if ss < 0:
                    break

            sql = '''
                ALTER TABLE {} UPDATE `c_props.value` = arrayMap((k, v) -> IF(k='厚度' OR k='颜色', '其它', v), c_props.name, c_props.value)
                WHERE pkey >= '{}' AND pkey <= '{}' AND (uuid2 NOT IN ({}))
            '''.format(tbl, smonth, emonth, ','.join(['\'{}\''.format(v) for v in uuids]))
            dba.execute(sql)

            while not self.cleaner.check_mutations_end(dba, tbl):
                time.sleep(5)


    def market(self, smonth, emonth):
        self.cleaner.market(904, smonth, emonth, 'clean_props.value[1] NOT IN (\'其它\')')