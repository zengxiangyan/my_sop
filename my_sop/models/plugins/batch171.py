import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import random

class main(Batch.main):

    def brush0510(self, smonth, emonth, logId=-1):
        remove = True
        uuids = []
        sales_by_uuid = {}
        cname, ctbl = self.get_c_tbl()
        top50sp5 = ['妮维雅630精华',
                    'Olay 光感小白瓶',
                    'Olay ProX 淡斑小白瓶',
                    'L\\\'Oreal 光子瓶',
                    '欧诗漫小白灯',
                    '乐敦rohtoCC精华液',
                    '韩束银胶囊美白精华液',
                    'Noreva iklen肌底美白精华',
                    '珀莱雅烟酰胺精华液',
                    '丸美美白精华液钻光瓶',
                    '自然堂雪润皙白淡斑精华液',]
        for each_sp5 in top50sp5:
            where = '''
            uuid2 in (select uuid2 from {} where sp3='{}' and sp4 = '{}' and sp5 = '{}')
            '''.format(ctbl,'有效链接', '单品', each_sp5)
            ret, sbs = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=50)
            sales_by_uuid.update(sbs)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1, remove=remove):
                    continue
                uuids.append(uuid2)
        where = '''
        uuid2 in (select uuid2 from {} where sp3='{}' and sp4 = '{}' and sp5 = '{}')
        '''.format(ctbl, '有效链接', '单品', 'The Ordinary10%烟酰胺+1%锌精华原液')
        ret, sbs = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=100)
        sales_by_uuid.update(sbs)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1, remove=remove):
                continue
            uuids.append(uuid2)

        where = '''
        uuid2 in (select uuid2 from {} where sp3='{}' and sp4 = '{}' and sp5 = '{}') and alias_all_bid = 44785 and name like '%{}%'
        '''.format(ctbl, '有效链接', '单品', '其它', '三重')
        ret, sbs = self.cleaner.process_top_anew(smonth, emonth, where=where)
        sales_by_uuid.update(sbs)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1, remove=remove):
                continue
            uuids.append(uuid2)

        where = '''
                uuid2 in (select uuid2 from {} where sp3='{}' and sp4 = '{}' and sp5 = '{}') and alias_all_bid = 380535 and name like '%{}%'
                '''.format(ctbl, '有效链接', '单品', '其它', 'cc')
        ret, sbs = self.cleaner.process_top_anew(smonth, emonth, where=where)
        sales_by_uuid.update(sbs)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1, remove=remove) or sbs[uuid2] < 5000000:
                continue
            uuids.append(uuid2)

        where = '''
                        uuid2 in (select uuid2 from {} where sp3='{}' and sp4 = '{}' and sp5 = '{}') and alias_all_bid = 3756670
                        '''.format(ctbl, '有效链接', '单品', '其它')
        ret, sbs = self.cleaner.process_top_anew(smonth, emonth, where=where)
        sales_by_uuid.update(sbs)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1, remove=remove) or sbs[uuid2] < 5000000:
                continue
            uuids.append(uuid2)

        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, visible=1, sales_by_uuid=sales_by_uuid)
        return True

    def brush(self, smonth, emonth, logId=-1):
        remove = True
        uuids = []
        sales_by_uuid = {}
        cname, ctbl = self.get_c_tbl()

        for each_name, top in [['精华水',100],['洗面奶',20]]:
            where = '''
            uuid2 in (select uuid2 from {} where sp3='{}' and sp4 = '{}' and sp5 = '{}' and sp1 = '{}') and name like '%{}%'
            '''.format(ctbl, '有效链接', '单品', '其它', '美白精华', each_name)
            ret, sbs = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=top)
            sales_by_uuid.update(sbs)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1, remove=remove):
                    continue
                uuids.append(uuid2)

        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, visible=1, sales_by_uuid=sales_by_uuid)
        return True


    def hotfix(self, tbl, dba, prefix, params):
        super().hotfix(tbl, dba, prefix, params)
        self.hotfix_isbrush(tbl, dba, '辅助_是否人工确认')

        sql = '''
            ALTER TABLE {} UPDATE `c_props.value` = arrayMap((k, v) -> IF(k='Sku Name' OR k='子品类', '其它', v), c_props.name, c_props.value)
            WHERE c_sales/c_num < 1500 AND c_alias_all_bid NOT IN (380535,4711571)
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)
