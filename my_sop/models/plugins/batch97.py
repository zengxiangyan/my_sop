import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app

class main(Batch.main):
    def finish_new(self, tbl, dba, prefix):
        # 均价小于20，sp11 刷成 ”删除“
        sql = '''
            ALTER TABLE {} UPDATE `sp是否删除` = '删除'
            WHERE clean_brush_id = 0 AND clean_sales/clean_num < 2000
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        # 当sp11 = 删除，宝贝均价>=100时，sp11 刷成 ”不删除“
        sql = '''
            ALTER TABLE {} UPDATE `sp是否删除` = '不删除'
            WHERE clean_brush_id = 0 AND clean_alias_all_bid = 112506 AND clean_sales/clean_num >= 10000 AND `sp是否删除` = '删除'
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        # 当 alias_all_bid = 61046 , sp6 = 电动，均价小于300时，sp11 刷成 ”删除“；
        # 当 alias_all_bid = 112485 , sp6 = 电动，均价小于600时，sp11 刷成 ”删除“；
        # 当 alias_all_bid = 63983 , sp6 = 电动，均价小于150时，sp11 刷成 ”删除“；
        # 当 alias_all_bid = 60824 , sp6 = 电动，均价小于130时，sp11 刷成 ”删除“；
        sql = '''
            ALTER TABLE {} UPDATE `sp是否删除` = '删除'
            WHERE clean_brush_id = 0 AND `sptype` = '电动' AND (
                   (clean_alias_all_bid = 61046 AND clean_sales/clean_num < 30000)
                OR (clean_alias_all_bid = 112485 AND clean_sales/clean_num < 60000)
                OR (clean_alias_all_bid = 63983 AND clean_sales/clean_num < 15000)
                OR (clean_alias_all_bid = 60824 AND clean_sales/clean_num < 13000)
            )
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        # 当sp7=和韵，均价小于120时，sp11 刷成 ”删除“；
        # 当sp7=飞韵，均价小于1000时，sp11 刷成 ”删除“；
        # 当sp7=丝韵，均价小于500时，sp11 刷成 ”删除“；
        # 当sp7=丝韵翼，均价小于700时，sp11 刷成 ”删除“；
        # 当sp7=新风韵，均价小于900时，sp11 刷成 ”删除“；
        # 当sp7=致韵，均价小于1500时，sp11 刷成 ”删除“；
        sql = '''
            ALTER TABLE {} UPDATE `sp是否删除` = '删除'
            WHERE clean_brush_id = 0 AND (
                   (`sp型号` = '和韵' AND clean_sales/clean_num < 12000)
                OR (`sp型号` = '飞韵' AND clean_sales/clean_num < 100000)
                OR (`sp型号` = '丝韵' AND clean_sales/clean_num < 50000)
                OR (`sp型号` = '丝韵翼' AND clean_sales/clean_num < 70000)
                OR (`sp型号` = '新风韵' AND clean_sales/clean_num < 90000)
                OR (`sp型号` = '致韵' AND clean_sales/clean_num < 150000)
            )
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        self.cleaner.add_miss_cols(tbl, {'spsingle（出数）':'String','sptype（出数）':'String'})

        sql = '''
            ALTER TABLE {} UPDATE `spsingle（出数）` = `spsingle`, `sptype（出数）` = `sptype`
            WHERE 1
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)


    def brush(self, smonth, emonth, logId=-1):
        where = 'alias_all_bid = 112485'
        uuids = []
        ret, sales_by_uuid = self.cleaner.process_top_anew(smonth, emonth, where=where)
        for uuid2, source, tb_item_id, p1, cid, bid in ret:
            if self.skip_brush(source, tb_item_id, p1, remove=True):
                continue
            uuids.append(uuid2)
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1)
        print('add new brush {}'.format(len(uuids)))
        return True