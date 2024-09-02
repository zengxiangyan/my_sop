import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import random

class main(Batch.main):

    def brush_0618(self, smonth, emonth, logId=-1):
        sql = 'SELECT source, tb_item_id, real_p1 FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): True for v in ret}
        clean_flag = self.cleaner.last_clean_flag() + 1
        uuids1 = []
        ret1 = self.cleaner.process_top_by_cid_lite(smonth, emonth, limit=2000, random=False, others_ratio=0.05)
        for uuid2, source, tb_item_id, p1 in ret1:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            mpp['{}-{}-{}'.format(source,tb_item_id,p1)] = True
            uuids1.append(uuid2)
        uuids2 = []
        ret2 = self.cleaner.process_top_by_cid_lite(smonth, emonth, limit=1000, random=True, others_ratio=0.05)
        for uuid2, source, tb_item_id, p1 in ret2:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True
            uuids2.append(uuid2)

        self.cleaner.add_brush(uuids1, clean_flag, 1)
        self.cleaner.add_brush(uuids2, clean_flag, 2)
        print(len(uuids1), len(uuids2))

        return True

    def brush(self, smonth, emonth, logId=-1):
        aname, atbl = self.get_a_tbl()
        cname, ctbl = self.get_all_tbl()
        uuids = []
        sales_by_uuid = {}
        rets = []
        db = self.cleaner.get_db(aname)

        sql = 'SELECT snum, tb_item_id, p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        brush_p1, filter_p1 = self.cleaner.get_plugin().filter_brush_props()
        data = []
        for snum, tb_item_id, p1, id, visible in ret:
            p1 = brush_p1(snum, p1)
            data.append([snum, tb_item_id, p1, id, visible])

        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in data}

        source1 = 'source = 1 and (shop_type > 20 or shop_type < 10 )'
        sp1_list = ['面部护理套装', '面膜', '面部精华', '乳液/面霜', '唇部']
        for each_sp1 in sp1_list:
            where = '''
            c_sp1='{each_sp1}'  and {source}
            '''.format(ctbl=ctbl, each_sp1=each_sp1, source=source1, smonth=smonth, emonth=emonth)
            ret, sbs = self.cleaner.process_top_aft200(smonth, emonth, limit=4, where=where)
            sales_by_uuid.update(sbs)
            rets.append(ret)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    continue
                else:
                    uuids.append(uuid2)
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

        alias_bid = [3779,20645,47858,52297,7012]
        for each_bid in alias_bid:
            where = '''
            alias_all_bid='{each_bid}'  and {source}
            '''.format(ctbl=ctbl, each_bid=each_bid, source=source1, smonth=smonth, emonth=emonth)
            ret, sbs = self.cleaner.process_top_aft200(smonth, emonth, limit=4, where=where)
            sales_by_uuid.update(sbs)
            rets.append(ret)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    continue
                else:
                    uuids.append(uuid2)
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

        source2 = '((source =1 and shop_type < 20 and shop_type > 10) or source =2)'
        sp1_list = ['面部护理套装', '面膜', '面部精华', '乳液/面霜', '唇部']
        for each_sp1 in sp1_list:
            where = '''
                    c_sp1='{each_sp1}' and {source}
                    '''.format(ctbl=ctbl, each_sp1=each_sp1, source=source2, smonth=smonth, emonth=emonth)
            ret, sbs = self.cleaner.process_top_aft200(smonth, emonth, limit=3, where=where)
            sales_by_uuid.update(sbs)
            rets.append(ret)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    continue
                else:
                    uuids.append(uuid2)
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

        alias_bid = [3779, 20645, 47858, 52297, 7012]
        for each_bid in alias_bid:
            where = '''
                    alias_all_bid='{each_bid}' and {source}
                    '''.format(ctbl=ctbl, each_bid=each_bid, source=source2, smonth=smonth, emonth=emonth)
            ret, sbs = self.cleaner.process_top_aft200(smonth, emonth, limit=3, where=where)
            sales_by_uuid.update(sbs)
            rets.append(ret)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    continue
                else:
                    uuids.append(uuid2)
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

        print('----------', len(uuids))
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)

        return True


    def hotfix_new(self, tbl, dba, prefix):
        sql = '''
            ALTER TABLE {} UPDATE
                `sp店铺分类` = multiIf(source=1 AND (shop_type<20 and shop_type>10), 'EDT', `sp店铺分类`='EKA_FSS', 'EKA', `sp店铺分类`)
            WHERE 1
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `spcategory` = '其它'
            WHERE item_id IN ('585814080718','586901021774','586234091514','586453471626','578776308772','585148588596','586226443322','587041790245','587061029380','587100470446','587444125677','587361573654','569520637213','585471268254','587846895415','588165399361')
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `spdel_flag` = '1'
            WHERE ( `spcategory` IN ('Makeup', 'Fragrance') AND (clean_price <= 100 OR clean_price >= 400000) )
               OR ( `spcategory` IN ('Skin Care') AND (clean_price <= 100 OR clean_price >= 800000) )
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `spdel_flag` = '0'
            WHERE sid = 193230920
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)