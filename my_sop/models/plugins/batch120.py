import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import pandas as pd

class main(Batch.main):
    def brush(self, smonth, emonth):
        sql = 'SELECT source, tb_item_id, real_p1 FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0],v[1],v[2]):True for v in ret}

        tb_item_ids = []
        file_sp = [
            ['batch120_刷品牌字段.xlsx', 2],
            ['batch120_刷是否出数.xlsx', 5],
            ['batch120_刷出数PriceSeg.xlsx', 11]]
        for filename, sp_pos in file_sp:
            print(filename, sp_pos)
            if sp_pos == 11:
                continue
            table = self.read_sp(filename)
            tb_item_ids = tb_item_ids + ['\'{}\''.format(v[0]) for v in table]

        uuids = []

        sql = '''
            SELECT argMax(uuid2, month), source, tb_item_id, p1 FROM {}_parts
            WHERE tb_item_id IN ({})
            GROUP BY source, tb_item_id, p1
        '''.format(self.get_entity_tbl(), ','.join(tb_item_ids))
        ret = self.cleaner.dbch.query_all(sql)
        for uuid2, source, tb_item_id, p1, in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            uuids.append(uuid2)
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1)

        for filename, sp_pos in file_sp:
            print(filename, sp_pos)
            table = self.read_sp(filename)
            for tb_item_id, sp_value in table:
                sp_value = sp_value.replace("'", "''")
                if sp_value == '其他':
                    sp_value = '其它'
                if sp_pos == 11:
                    # where sp2=品牌
                    tb_item_id = tb_item_id.replace("'", "''")
                    sql = 'update {} set spid{}=\'{}\', flag=1, uid=227 where spid2 = \'{}\''.format(self.cleaner.get_tbl(), sp_pos, sp_value, tb_item_id)
                else:
                    sql = 'update {} set spid{}=\'{}\', flag=1, uid=227 where tb_item_id = {}'.format(self.cleaner.get_tbl(), sp_pos, sp_value, tb_item_id)
                print(sql)
                self.cleaner.db26.execute(sql)

        print('add new brush {}'.format(len(uuids)))

        return True

    def read_sp(self, filename):
        file = app.output_path(filename)
        # table = pd.read_excel(file, 0, header=None, dtype=str)
        table = pd.read_excel(file, 0, dtype=str)
        table = table.fillna('')
        table = table.values.tolist()
        return table