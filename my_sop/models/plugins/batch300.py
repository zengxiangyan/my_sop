import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app

class main(Batch.main):

    def hotfix_new(self, tbl, dba, prefix):
        sql = '''
            SELECT max(LENGTH(splitByChar('+',`sp包装规格`))) FROM {} WHERE `sp包装规格` != ''
        '''.format(tbl)
        ccc = dba.query_all(sql)[0][0]

        if ccc == 0:
            return

        cols, fcols, fixcols = {}, [], []
        for i in range(ccc):
            cols['sp单包抽数{}'.format(i+1)] = 'String'
            cols['sp规格{}'.format(i+1)] = 'String'
            fcols.append('''`sp单包抽数{i}`=arrayMap(x -> splitByChar('*',x), splitByChar('+',`sp包装规格`))[{i}][1]'''.format(i=i+1))
            fcols.append('''`sp规格{i}`=arrayMap(x -> splitByChar('*',x), splitByChar('+',`sp包装规格`))[{i}][2]'''.format(i=i+1))
            fixcols.append('''`sp规格{i}`=IF(`sp单包抽数{i}`!='' AND `sp规格{i}`='', '1', `sp规格{i}`)'''.format(i=i+1))

        self.cleaner.add_miss_cols(tbl, cols)

        sql = 'ALTER TABLE {t} UPDATE {} WHERE 1'.format(','.join(fcols), t=tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = 'ALTER TABLE {t} UPDATE {} WHERE 1'.format(','.join(fixcols), t=tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)