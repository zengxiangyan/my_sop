import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import random

class main(Batch.main):

    def hotfix_new(self, tbl, dba, prefix):
        sql = 'SELECT trim(p) FROM {} ARRAY JOIN splitByString(\'Ծ‸ Ծ\', `sp一级类目`) AS p GROUP BY trim(p)'.format(tbl)
        ret = dba.query_all(sql)

        self.cleaner.add_miss_cols(tbl, {'sp一级类目-{}'.format(v):'String' for v, in ret})

        dba.execute('DROP TABLE IF EXISTS {}_set'.format(tbl))

        sql = '''
            CREATE TABLE {t}_set ENGINE = Set AS
            SELECT concat(toString(uuid2), trim(p)) FROM {t} ARRAY JOIN splitByString('Ծ‸ Ծ', `sp一级类目`) AS p
        '''.format(t=tbl)
        dba.execute(sql)

        cols = ['`sp一级类目-{c}`=IF(concat(toString(uuid2), \'{c}\') IN {}_set, \'{c}\', \'否\')'.format(tbl, c=v) for v, in ret]
        sql = 'ALTER TABLE {t} UPDATE {} WHERE 1'.format(','.join(cols), t=tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        dba.execute('DROP TABLE IF EXISTS {}_set'.format(tbl))

        sql = 'SELECT trim(p) FROM {} ARRAY JOIN splitByString(\'Ծ‸ Ծ\', `sp二级类目`) AS p GROUP BY trim(p)'.format(tbl)
        ret = dba.query_all(sql)

        self.cleaner.add_miss_cols(tbl, {'sp二级类目-{}'.format(v):'String' for v, in ret})

        dba.execute('DROP TABLE IF EXISTS {}_set'.format(tbl))

        sql = '''
            CREATE TABLE {t}_set ENGINE = Set AS
            SELECT concat(toString(uuid2), trim(p)) FROM {t} ARRAY JOIN splitByString('Ծ‸ Ծ', `sp二级类目`) AS p
        '''.format(t=tbl)
        dba.execute(sql)

        cols = ['`sp二级类目-{c}`=IF(concat(toString(uuid2), \'{c}\') IN {}_set, \'{c}\', \'否\')'.format(tbl, c=v) for v, in ret]
        sql = 'ALTER TABLE {t} UPDATE {} WHERE 1'.format(','.join(cols), t=tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)
        
        dba.execute('DROP TABLE IF EXISTS {}_set'.format(tbl))

        sql = 'SELECT trim(p) FROM {} ARRAY JOIN splitByString(\'Ծ‸ Ծ\', `sp三级类目`) AS p GROUP BY trim(p)'.format(tbl)
        ret = dba.query_all(sql)

        self.cleaner.add_miss_cols(tbl, {'sp三级类目-{}'.format(v):'String' for v, in ret})

        dba.execute('DROP TABLE IF EXISTS {}_set'.format(tbl))

        sql = '''
            CREATE TABLE {t}_set ENGINE = Set AS
            SELECT concat(toString(uuid2), trim(p)) FROM {t} ARRAY JOIN splitByString('Ծ‸ Ծ', `sp三级类目`) AS p
        '''.format(t=tbl)
        dba.execute(sql)

        cols = ['`sp三级类目-{c}`=IF(concat(toString(uuid2), \'{c}\') IN {}_set, \'{c}\', \'否\')'.format(tbl, c=v) for v, in ret]
        sql = 'ALTER TABLE {t} UPDATE {} WHERE 1'.format(','.join(cols), t=tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        dba.execute('DROP TABLE IF EXISTS {}_set'.format(tbl))