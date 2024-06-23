import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app

class main(Batch.main):

    def brush_modify(self, v, bru_items):
        arr = []
        for vv in v['split_pids']:
            if vv['sp5'] != '' and vv['sp5'] != '否':
                arr.append(vv['sp5'])
            if vv['sp6'] != '' and vv['sp6'] != '否':
                arr.append(vv['sp6'])
            if vv['sp7'] != '' and vv['sp7'] != '否':
                arr.append(vv['sp7'])
            if vv['sp8'] != '' and vv['sp8'] != '否':
                arr.append(vv['sp8'])
            if vv['sp9'] != '' and vv['sp9'] != '否':
                arr.append(vv['sp9'])
            if vv['sp10'] != '' and vv['sp10'] != '否':
                arr.append(vv['sp10'])
            if vv['sp11'] != '' and vv['sp11'] != '否':
                arr.append(vv['sp11'])
            if vv['sp12'] != '' and vv['sp12'] != '否':
                arr.append(vv['sp12'])
            if vv['sp13'] != '' and vv['sp13'] != '否':
                arr.append(vv['sp13'])

        arr = list(set(arr))
        arr.sort()

        for vv in v['split_pids']:
            vv['sp16'] = 'Ծ‸ Ծ'.join(arr)


    def hotfix_new(self, tbl, dba, prefix):
        sql = 'SELECT trim(p) FROM {} ARRAY JOIN splitByString(\'Ծ‸ Ծ\', `sp功效`) AS p GROUP BY trim(p)'.format(tbl)
        ret = dba.query_all(sql)

        self.cleaner.add_miss_cols(tbl, {'sp功效-{}'.format(v):'String' for v, in ret})

        dba.execute('DROP TABLE IF EXISTS {}_set'.format(tbl))

        sql = '''
            CREATE TABLE {t}_set ENGINE = Set AS
            SELECT concat(toString(uuid2), trim(p)) FROM {t} ARRAY JOIN splitByString('Ծ‸ Ծ', `sp功效`) AS p
        '''.format(t=tbl)
        dba.execute(sql)

        cols = ['`sp功效-{c}`=IF(concat(toString(uuid2), \'{c}\') IN {}_set, \'{c}\', \'否\')'.format(tbl, c=v) for v, in ret]
        sql = 'ALTER TABLE {t} UPDATE {} WHERE 1'.format(','.join(cols), t=tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = 'DROP TABLE IF EXISTS {}_set'.format(tbl)
        dba.execute(sql)