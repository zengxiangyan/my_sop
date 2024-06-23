import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app

class main(Batch.main):
    def brush(self, smonth, emonth):
        remove = True
        bids = [89329]
        # where = 'uuid2 in (select uuid2 from {} where sp1=\'益生菌\') and source != \'tb\''.format(self.get_clean_tbl())
        where = 'alias_all_bid in ({}) and uuid2 in ' \
                '(select uuid2 from {} where sp30=\'OTC肠胃用药\')'.format(','.join([str(v) for v in bids]),self.get_clean_tbl())
        counter = []
        uuids = []
        sales_by_uuid = {}
        for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            c = 0
            #
            # sales_by_uuid = {}
            # ret, sales_by_uuid1 = self.cleaner.process_top(ssmonth, eemonth,rate=0.95, where=where)
            # for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
            #     if self.skip_brush(source, tb_item_id, p1, remove=False, flag=False):
            #         continue
            #     # uuids.append(uuid2)
            #     c = c + 1
            # ret, sales_by_uuid1 = self.cleaner.process_top(ssmonth, eemonth, rate=1, where=where)
            # for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
            #     if self.skip_brush(source, tb_item_id, p1, remove=False, flag=False):
            #         continue
            #     uuids.append(uuid2)
            #     c = c + 1
            #     sales_by_uuid[uuid2] = sales_by_uuid1[uuid2]

            ret, sales_by_uuid1 = self.cleaner.process_top(ssmonth, eemonth, rate=0.95,where=where)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1, remove=remove, flag=False):
                    continue
                uuids.append(uuid2)
                c = c+1
            # c = max(list(sales_by_uuid.values()))
            counter.append([ssmonth,eemonth,c])
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid)
        print('147 add new brush {}'.format(len(set(uuids))))
        for i in counter:
            print(i)
        print(len(uuids))
        return True


    def hotfix(self, tbl, dba, prefix, params):
        super().hotfix(tbl, dba, prefix, params)
        sql = '''
            SELECT toString(groupArray(concat(toString(`source`),'_', toString(sid)))), toString(groupArray(shop_type))
            FROM sop_e.entity_prod_{}_ALL_ECSHOP
        '''.format(self.cleaner.eid)
        rr1 = dba.query_all(sql)

        sql = '''
            ALTER TABLE {} UPDATE `c_props.value` = arrayMap((k, v) -> IF(
                k='店铺分类', transform(concat(toString(`source`),'_', toString(sid)), {}, {}, v),
            v), c_props.name, c_props.value)
            WHERE 1
        '''.format(tbl, rr1[0][0], rr1[0][1])
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `c_props.value` = arrayMap((k, v) -> multiIf(
                k='店铺分类', multiIf(`source` = 1 AND (shop_type < 20 and shop_type > 10 ), 'C2C', v = 'EKA_FSS', 'EKA', v),
                k='人群（江中）', IF(v = '男士', '通用', v),
            v), c_props.name, c_props.value)
            WHERE 1
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)