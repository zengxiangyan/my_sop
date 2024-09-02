import sys
import time
import datetime
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app

class main(Batch.main):

    def hotfix_new(self, tbl, dba, prefix):
        self.cleaner.add_miss_cols(tbl, {'sp抖音店铺类型':'String'})

        dba.execute('DROP TABLE IF EXISTS {}_join'.format(tbl))

        dba.execute('''
            CREATE TABLE {}_join ENGINE = Join(ANY, LEFT, i) AS
            SELECT toUInt64(sid) i, if(type_f in ('', '其他'), '集市店', type_f) type_tran FROM mysql('192.168.10.96:3306', 'apollo_douyin', 'shop_channel', 'apollo-rw', 'QBT094bt')
        '''.format(tbl))

        while not self.cleaner.check_mutations_end(dba, '{}_join'.format(tbl)):
            time.sleep(5)

        dba.execute('''
            ALTER TABLE {t} UPDATE
                `sp抖音店铺类型` = ifNull(joinGet('{t}_join', 'type_tran', sid), '集市店')
            WHERE source = 11
        '''.format(t=tbl))

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        dba.execute('DROP TABLE IF EXISTS {}_join'.format(tbl))
