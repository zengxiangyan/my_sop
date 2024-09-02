import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app

class main(Batch.main):
    def brush_2(self, smonth, emonth):
        # 默认规则
        sql = 'SELECT source, tb_item_id, real_p1 FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0],v[1],v[2]):True for v in ret}

        xxx = self.xxx()
        tb_item_ids = ['\'{}\''.format(v[0]) for v in xxx]

        uuids = []

        sql = '''
            SELECT argMax(uuid2, month), source, tb_item_id, p1 FROM {}_parts
            WHERE tb_item_id IN ({})
            GROUP BY source, tb_item_id, p1
        '''.format(self.get_entity_tbl(), ','.join(tb_item_ids))
        # 按tb_item_id
        ret = self.cleaner.dbch.query_all(sql)
        for uuid2, source, tb_item_id, p1, in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            uuids.append(uuid2)
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1)

        for tb_item_id, bid, in xxx:
            sql = 'UPDATE {} SET all_bid = \'{}\', alias_all_bid = \'{}\', flag=1,uid=227 WHERE tb_item_id = {}'.format(self.cleaner.get_tbl(), bid,
                                                                                                                       bid, tb_item_id)
            self.cleaner.db26.execute(sql)

        print('add new brush {}'.format(len(uuids)))

        return True

    def brush_bak0120(self, smonth, emonth):
        # 默认规则
        sql = 'SELECT source, tb_item_id, real_p1 FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0],v[1],v[2]):True for v in ret}
        uuids = []
        sql = '''
            SELECT argMax(uuid2, month), source, tb_item_id, p1 FROM {}_parts
            WHERE tb_item_id IN ({})
            GROUP BY source, tb_item_id, p1
        '''.format(self.get_entity_tbl(), '\'8476783\'')
        # 按tb_item_id
        ret = self.cleaner.dbch.query_all(sql)
        for uuid2, source, tb_item_id, p1, in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            uuids.append(uuid2)
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1)
        print('add new brush {}'.format(len(uuids)))
        return True

    def brush(self, smonth, emonth):
        # 默认规则
        uuids = []
        where = 'tb_item_id = \'100006402719\' and source = \'jd\''
        ret, sales_by_uuid = self.cleaner.process_top(smonth, emonth, where=where)
        for uuid2, source, tb_item_id, p1, bid in ret:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids.append(uuid2)
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1,sales_by_uuid=sales_by_uuid)
        print('add new brush {}'.format(len(uuids)))
        return True



    def xxx(self):
        return [
            [32168496027, 5833208],
            [39869147359, 5833208],
            [39869147360, 5833208],
            [39869147361, 5833208],
            [39879620248, 5833208],
            [1142573967, 5833208],
            [41243899847, 5833208],
            [39879620249, 5833208],
            [41243899847, 5833208],
            [41243899847, 5833208],
            [23477121241, 4807618]
        ]

    def process_exx(self, tbl, prefix, logId=0):
        super().process_exx(tbl)
        self.update_e_alias_bid(tbl)