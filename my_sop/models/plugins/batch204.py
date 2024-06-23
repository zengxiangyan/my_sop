import sys
import time
import datetime
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')
import models.plugins.batch as Batch
import application as app


class main(Batch.main):

    def brush_modify(self, v, bru_items):
        for vv in v['split_pids']:
            if v['flag'] == 2 and vv['sp10'] != '':
                vv['sp6'] = vv['sp10'].replace('g','').replace('ml','')+'*'+str(vv['number'])
            vv['sp6'] = '{:g}'.format(round(eval(vv['sp6']),2)) if vv['sp6'] != '' else ''


    def hotfix_new(self, tbl, dba, prefix):
        bidsql, bidtbl = self.cleaner.get_aliasbid_sql()

        sql = '''
            ALTER TABLE {} UPDATE `spSKU（不出数）` = ''
            WHERE `spSKU（不出数）` != '' AND b_id > 0 AND b_pid = 0 AND (b_sp1 != c_sp1 OR {} != {})
        '''.format(tbl, bidsql.format(v='IF(c_all_bid=0,alias_all_bid,c_all_bid)'), bidsql.format(v='b_all_bid'))
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        self.cleaner.add_miss_cols(tbl, {'sp功效单一值':'String'})

        sql = '''
            ALTER TABLE {} UPDATE `sp功效单一值` = multiIf(
                `sp功效` LIKE '%防脱生发%', '防脱生发',
                `sp功效` LIKE '%蓬松丰盈%', '蓬松丰盈',
                `sp功效` LIKE '%清爽控油%', '清爽控油',
                `sp功效` LIKE '%修复修护%', '修复修护',
                `sp功效` LIKE '%柔顺保湿%', '柔顺保湿/柔顺滋润',
                `sp功效` LIKE '%去屑止痒%', '去屑止痒',
                `sp功效` LIKE '%强韧头发%', '强韧头发',
                `sp功效` LIKE '%深层清洁%', '深层清洁',
                `sp功效` LIKE '%舒缓抗炎%', '舒缓抗炎',
                `sp功效` LIKE '%固色护色%', '固色护色',
                `sp功效` LIKE '%留香去异味%', '留香去异味',
                '其它'
            ) WHERE 1
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

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