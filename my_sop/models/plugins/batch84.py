import sys
import time
import datetime
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app

class main(Batch.main):

    def brush_2(self, smonth, emonth):
        sql = 'SELECT source, tb_item_id, real_p1 FROM {} where flag > 0'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0],v[1],v[2]):True for v in ret}

        sql = '''
        SELECT uniq_k FROM artificial.entity_{}_clean WHERE month >= '{smonth}' and month < '{emonth}' and  sp4 = '绿茶蜜滴身体霜500ml'
        '''.format(self.cleaner.eid, smonth=smonth, emonth=emonth)
        uniq_k = [str(v[0]) for v in self.cleaner.dbch.query_all(sql)]
        uniq_k = ','.join(uniq_k)

        ret, sales_by_uuid = self.cleaner.process_top(smonth, emonth, rate=0.8, where='uniq_k in ({})'.format(uniq_k))
        uuids = []
        for uuid2, source, tb_item_id, p1 in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            uuids.append(uuid2)

        clean_flag = self.cleaner.last_clean_flag() + 1
        # self.cleaner.add_brush(uuids, clean_flag)
        print('add new brush {}'.format(len(set(uuids))))


    def brush_3(self, smonth, emonth):
        # 默认出题规则
        sql = 'SELECT source, tb_item_id, real_p1 FROM {} where flag > 0'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0],v[1],v[2]):True for v in ret}
        tb_item_ids = ['523796103067', '523226871910', '622409443056']
        tb_item_ids = ','.join(['\'{}\''.format(v) for v in tb_item_ids])
        ret, sales_by_uuid = self.cleaner.process_top(smonth, emonth, where='tb_item_id in ({})'.format(tb_item_ids))
        uuids = []
        for uuid2, source, tb_item_id, p1 in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            uuids.append(uuid2)
        clean_flag = self.cleaner.last_clean_flag() + 1
        # self.cleaner.add_brush(uuids, clean_flag)
        print('add new brush {}'.format(len(set(uuids))))

    def brush_bak0119(self, smonth, emonth):
        # batch84
        # tb_item_id：624092172270
        # 的sku麻烦全部改为其它
        sql = 'SELECT source, tb_item_id, real_p1 FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0],v[1],v[2]):True for v in ret}
        # tb_item_ids = ['\'{}\''.format(v[0]) for v in xxx]
        uuids = []
        xxx = [['\'624092172270\'', '其它'],
               ['\'45495763965\'', '时空焕活胶囊精华液60粒']]
        tb_item_ids = [v[0] for v in xxx]
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
        for tb_item_id, sp in xxx:
            sql = 'UPDATE {} SET spid4 = \'{}\', flag=1, uid=227 WHERE tb_item_id = {}'.format(self.cleaner.get_tbl(), sp, tb_item_id)
            print(sql)
            self.cleaner.db26.execute(sql)

        print('add new brush {}'.format(len(uuids)))

        return True

    def brush_default_0323(self, smonth, emonth, logId=-1):
        uuids = []
        sales_by_uuid = {}
        ret, sales_by_uuid1 = self.cleaner.process_top(smonth, emonth, limit=100)
        sales_by_uuid.update(sales_by_uuid1)
        for uuid2, source, tb_item_id, p1, alias in ret:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids.append(uuid2)
        uuids2 = []
        ret2 = self.cleaner.process_rand(smonth, emonth, limit=100)
        for uuid2, source, tb_item_id, p1, in ret2:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids2.append(uuid2)

        uuids3 = []
        where = 'uuid2 in (select uuid2 from {} where sp4 = \'{}\')'.format(self.get_clean_tbl(),'时空焕活夜间多效胶囊精华90粒')
        ret3,sales_by_uuid2 = self.cleaner.process_top(smonth,emonth, where=where, limit=50)
        sales_by_uuid.update(sales_by_uuid2)
        for uuid2, source, tb_item_id, p1, alias in ret3:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids3.append(uuid2)
        where = 'uuid2 in (select uuid2 from {} where sp4 = \'{}\')'.format(self.get_clean_tbl(), '绿茶蜜滴身体霜250ml')
        ret4,sales_by_uuid3 = self.cleaner.process_top(smonth, emonth, where=where, limit=50)
        sales_by_uuid.update(sales_by_uuid3)
        for uuid2, source, tb_item_id, p1, alias in ret4:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids3.append(uuid2)
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=1, sales_by_uuid=sales_by_uuid)
        self.cleaner.add_brush(uuids2, clean_flag, visible_check=2, visible=1)
        self.cleaner.add_brush(uuids3, clean_flag, visible=2, sales_by_uuid=sales_by_uuid)
        return True

    def brush(self, smonth, emonth, logId=-1):
        uuids = []
        sales_by_uuid = {}

        dba, ctbl = self.get_c_tbl()
        uuids3 = []
        where = 'uuid2 in (select uuid2 from {} where sp4 = \'{}\')'.format(ctbl,'白茶舒体霜')
        ret3,sales_by_uuid2 = self.cleaner.process_top_anew(smonth,emonth, where=where, rate=0.9)
        sales_by_uuid.update(sales_by_uuid2)
        for uuid2, source, tb_item_id, p1, cid, alias in ret3:
            if self.skip_brush(source, tb_item_id, p1, remove=True):
                continue
            uuids3.append(uuid2)
        print('add brush: ', len(uuids3))
        clean_flag = self.cleaner.last_clean_flag() + 1
        # self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=1, sales_by_uuid=sales_by_uuid)
        # self.cleaner.add_brush(uuids2, clean_flag, visible_check=2, visible=1)
        self.cleaner.add_brush(uuids3, clean_flag, visible=2, sales_by_uuid=sales_by_uuid)
        return True

    def process_exx(self, tbl, prefix, logId=0):
        ename, etbl = self.get_e_tbl()
        edba = self.cleaner.get_db(ename)

        self.cleaner.new_clone_tbl(edba, tbl, prefix+'_fixsku', dropflag=True)

        sql = '''
            ALTER TABLE {} UPDATE `spSKU-出数专用` = `spSKU` WHERE 1
        '''.format(prefix+'_fixsku')
        edba.execute(sql)

        while not self.cleaner.check_mutations_end(edba, prefix+'_fixsku'):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `spSKU-出数专用` = '其它'
            WHERE `spSKU` IN ('时空焕活透亮润泽胶囊精华液30粒', '时空焕活透亮润泽胶囊精华液60粒', '时空焕活眼部胶囊精华液5.2ml')
        '''.format(prefix+'_fixsku')
        edba.execute(sql)

        while not self.cleaner.check_mutations_end(edba, prefix+'_fixsku'):
            time.sleep(5)

        self.cleaner.update_eprops(prefix+'_fixsku', edba)


    # copy_etbl
    # def update_e(self, dba, tbla, tblb):
    #     cols = self.cleaner.get_cols(tblb, dba, ['c_all_bid','c_alias_all_bid','c_cid','c_pid','c_props.name','c_props.value'])

    #     sql = 'DROP TABLE IF EXISTS {}x'.format(tblb)
    #     dba.execute(sql)

    #     sql = 'CREATE TABLE IF NOT EXISTS {t}x AS {t}'.format(t=tblb)
    #     dba.execute(sql)

    #     sql = '''
    #         INSERT INTO {t}x ({cols},c_all_bid,c_alias_all_bid,c_cid,c_pid,`c_props.name`,`c_props.value`)
    #         SELECT {cols},
    #             ifNull(b.a,c_all_bid), ifNull(b.b,c_alias_all_bid), ifNull(b.c,c_cid), ifNull(b.d,c_pid),
    #             IF(empty(b.e),`c_props.name`,b.e), IF(empty(b.f),`c_props.value`,b.f)
    #         FROM (
    #             SELECT * FROM {t} WHERE `c_props.value`[indexOf(c_props.name, 'SKU')] IN (
    #                 '其它', '时空焕活胶囊精华液90粒'
    #             )
    #         ) a
    #         JOIN (
    #             SELECT item_id, date, `source`, `trade_props.name`, `trade_props.value`,
    #                 any(all_bid) a, any(alias_all_bid) b, any(clean_cid) c, any(clean_pid) d,
    #                 any(`clean_props.name`) e, any(`clean_props.value`) f
    #             FROM {} b WHERE `clean_props.value`[indexOf(clean_props.name, 'SKU')] IN (
    #                 '时空焕活夜间多效胶囊精华90粒', '绿茶蜜滴身体霜250ml'
    #             )
    #             GROUP BY item_id, date, `source`, `trade_props.name`, `trade_props.value`
    #         ) b
    #         USING (item_id, date, `source`, `trade_props.name`, `trade_props.value`)
    #     '''.format(tbla, t=tblb, cols=','.join(cols.keys()))
    #     dba.execute(sql)

    #     sql = 'INSERT INTO {t}x SELECT * FROM {t} WHERE uuid2 NOT IN (SELECT uuid2 FROM {t}x)'.format(t=tblb)
    #     dba.execute(sql)

    #     sql = 'DROP TABLE {}'.format(tblb)
    #     dba.execute(sql)

    #     sql = 'RENAME TABLE {t}x TO {t}'.format(t=tblb)
    #     dba.execute(sql)

    def finish_new(self, tbl, dba, prefix):

        sql = '''
            ALTER TABLE {} UPDATE `spSKU` = '铂粹御肤精华液30ml'
            WHERE item_id = '638069801731'
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `spSKU` = '绿茶蜜滴身体霜500ml', `spSKU-出数专用` = '绿茶蜜滴身体霜500ml'
            WHERE item_id = '100003031794'
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `sp子品类` = '删除'
            WHERE clean_sales/clean_num >= 500000 OR (item_id='646142903111' AND clean_sales/clean_num >= 99900)
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)