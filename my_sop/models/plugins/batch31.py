import sys
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app

class main(Batch.main):
    def hotfix(self, tbl, dba, prefix, params):
        super().hotfix(tbl, dba, prefix, params)
        pass
        # # set sp5='西药' where sp5='其它'
        # sql = '''
        #     INSERT INTO {tbl} (snum, pkey, uuid2, month, source, step, sp5)
        #     SELECT snum, pkey, uuid2, month, source, step+1, '西药'
        #     FROM {tbl} WHERE step >= {} AND step < {} AND step%100 = 0 AND sp5='其它'
        # '''.format(sver, ever, tbl=tbl)
        # self.cleaner.dbch.execute(sql)

        # # set sp5='其它' where sp5='待确定'
        # sql = '''
        #     INSERT INTO {tbl} (snum, pkey, uuid2, month, source, step, sp5)
        #     SELECT snum, pkey, uuid2, month, source, step+2, '其它'
        #     FROM {tbl} WHERE step >= {} AND step < {} AND step%100 = 0 AND sp5='待确定'
        # '''.format(sver, ever, tbl=tbl)
        # self.cleaner.dbch.execute(sql)