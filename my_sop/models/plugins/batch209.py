import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import pandas as pd

class main(Batch.main):

    def finish_new(self, tbl, dba, prefix):
        sql = '''
            SELECT clean_alias_all_bid a, `sp国别` sp, SUM(clean_sales) ss FROM {}
            WHERE sp != '' AND a > 0 GROUP BY a, sp ORDER BY ss DESC LIMIT 1 BY a
        '''.format(tbl)
        ret = dba.query_all(sql)
        sql = '''
            ALTER TABLE {} UPDATE `sp国别` = transform(clean_alias_all_bid,[{}],['{}'],'中国大陆') WHERE 1
        '''.format(tbl, ','.join([str(a) for a,b,c, in ret]),'\',\''.join([str(b) for a,b,c, in ret]))
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)
