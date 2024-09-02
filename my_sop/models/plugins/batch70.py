import sys
import time
import datetime
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')
import models.plugins.batch as Batch
import application as app


class main(Batch.main):

    def brush_1014(self, smonth, emonth):
        # sql = 'SELECT source, tb_item_id, real_p1 FROM {}'.format(self.cleaner.get_tbl())
        # ret = self.cleaner.db26.query_all(sql)
        # mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): True for v in ret}
        clean_flag = self.cleaner.last_clean_flag() + 1
        uuids1 = []
        ret1, sales_by_uuid = self.cleaner.process_top_by_cid(smonth, emonth, limit=1000, others_ratio=0.05)
        for uuid2, source, tb_item_id, p1, cid in ret1:
            # if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
            #     continue
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids1.append(uuid2)
        uuids1 = list(set(uuids1))
        uuids2 = []
        ret2, ret_sales_by_uuid2 = self.cleaner.process_top_by_cid(smonth, emonth, limit=1000, random=True, others_ratio=0.05)
        for uuid2, source, tb_item_id, p1, cid in ret2:
            # if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
            #     continue
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids2.append(uuid2)
        uuids2 = list(set(uuids2))
        self.cleaner.add_brush(uuids1, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        self.cleaner.add_brush(uuids2, clean_flag, 2)
        print(len(uuids1), len(uuids2))
        # print(len(uuids2))

    def brush_1014bak(self, smonth, emonth, logId=-1):

        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()
        clean_flag = self.cleaner.last_clean_flag() + 1

        sales_by_uuid = {}


        sources = ['source = 1 and (shop_type > 20 or shop_type < 10 )', 'source=2']
        sp1s = ['眼镜片', '眼镜架', '太阳眼镜']

        uuids = []
        for each_source in sources:
            for each_sp1 in sp1s:
                where = '''
                uuid2 in (select uuid2 from {ctbl} where sp1='{each_sp1}' and pkey>='{smonth}' and pkey<'{emonth}') and {each_source}
                '''.format(ctbl=ctbl, each_sp1=each_sp1, smonth=smonth, emonth=emonth, each_source=each_source)
                ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=100)
                sales_by_uuid.update(sales_by_uuid1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if self.skip_brush(source, tb_item_id, p1, if_flag=False):
                        continue
                    uuids.append(uuid2)
        self.cleaner.add_brush(uuids, clean_flag, 1, visible=1, sales_by_uuid=sales_by_uuid)

        uuids = []
        for each_source in sources:
            where = '''
            uuid2 in (select uuid2 from {ctbl} where sp1 not in('眼镜片', '眼镜架', '太阳眼镜') and pkey>='{smonth}' and pkey<'{emonth}') and {each_source}
            '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_source=each_source)
            ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=100)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1, if_flag=False):
                    continue
                uuids.append(uuid2)
        self.cleaner.add_brush(uuids, clean_flag, 1, visible=1, sales_by_uuid=sales_by_uuid)

        return True

    def brush(self, smonth, emonth, logId=-1):

        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()
        clean_flag = self.cleaner.last_clean_flag() + 1
        sales_by_uuid = {}

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        sources = ['source = 1 and (shop_type > 20 or shop_type < 10 )', 'source=2']
        alias_bid_lists = ['18962', '17163', '48139', '243213', '243168']
        sp1s = [['眼镜架', 500], ['眼镜片', 500], ['太阳眼镜', 100]]

        for each_source in sources:
            uuids = []
            where = '''
            uuid2 in (select uuid2 from {ctbl} where pkey>='{smonth}' and pkey<'{emonth}' and sp1 in ('眼镜片','眼镜架')) and alias_all_bid in ('18962', '17163', '48139', '243213', '243168')
            and {each_source}
            '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_source=each_source)
            ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=300)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    continue
                else:
                    uuids.append(uuid2)
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
            self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=10, sales_by_uuid=sales_by_uuid1)

        for each_sp1, each_limits in sp1s:
            for each_source in sources:
                uuids10 = []
                uuids20 = []
                where = '''
                uuid2 in (select uuid2 from {ctbl} where pkey>='{smonth}' and pkey<'{emonth}' and sp1 = '{each_sp1}') and {each_source}
                '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_source=each_source, each_sp1=each_sp1)
                ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=each_limits)
                sales_by_uuid.update(sales_by_uuid1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        continue
                    else:
                        if str(alias_all_bid) in alias_bid_lists:
                            uuids10.append(uuid2)
                        else:
                            uuids20.append(uuid2)
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                self.cleaner.add_brush(uuids10, clean_flag, visible_check=1, visible=10, sales_by_uuid=sales_by_uuid1)
                self.cleaner.add_brush(uuids20, clean_flag, visible_check=1, visible=20, sales_by_uuid=sales_by_uuid1)

        return True

    def finish(self, tbl, dba, prefix):
        pass
        # if test:
        #     # 修数据用的版本不需要做处理
        #     return

        # now = datetime.datetime.now().date().replace(day=1)
        # now = (now - datetime.timedelta(days=1)).replace(day=1)
        # pmonth = (now - datetime.timedelta(days=1)).replace(day=1)
        # ppmonth = (pmonth - datetime.timedelta(days=1)).replace(day=1)
        # pppmonth = (ppmonth - datetime.timedelta(days=1)).replace(day=1)

        # sql = 'SELECT min(date), max(date), max(c_out_ver) FROM {}'.format(tbl)
        # smonth, emonth, ver = dba.query_all(sql)[0]

        # # 更新上月
        # if smonth >= pmonth or pmonth < emonth:
        #     sql = '''
        #         ALTER TABLE {} UPDATE c_num = toInt32(round( num / IF(sid IN (57589,1000014082), 0.83, 0.7) ))
        #         WHERE sid IN (57589, 1000014082, 151019, 644575, 616701) AND cid IN (9790,9789)
        #     '''.format(tbl)
        #     dba.execute(sql)

        #     while not self.cleaner.check_mutations_end(dba, tbl):
        #         time.sleep(5)

        #     sql = '''
        #         ALTER TABLE {} UPDATE c_sales = toInt64(round( sales / num * c_num ))
        #         WHERE sid IN (57589, 1000014082, 151019, 644575, 616701) AND cid IN (9790,9789)
        #     '''.format(tbl)
        #     dba.execute(sql)

        #     while not self.cleaner.check_mutations_end(dba, tbl):
        #         time.sleep(5)

        # _, etbl = self.cleaner.get_plugin().get_e_tbl()
        # _, dtbl = self.cleaner.get_plugin().get_d_tbl()

        # cols = self.cleaner.get_cols(tbl, dba, ['c_out_ver'])
        # cols = ','.join(cols.keys())

        # # 更新上上月
        # if smonth > ppmonth:
        #     sql = 'SELECT max(ver) FROM {} WHERE date >= {} AND date < {}'.format(etbl, ppmonth, pmonth)
        #     old_ver = dba.query_all(sql)[0][0]

        #     sql = 'SELECT max(c_out_ver) FROM {} WHERE date >= {} AND date < {}'.format(dtbl, ppmonth, pmonth)
        #     new_ver = dba.query_all(sql)[0][0]

        #     if old_ver == new_ver:
        #         raise Exception('ver error {}'.format(old_ver))
        #         return

        #     # 导入旧数据
        #     sql = '''
        #         INSERT INTO {} ({cols}, c_out_ver) SELECT {cols}, {} FROM {}
        #         WHERE c_out_ver = {} AND date >= {} AND date < {} AND NOT (sid IN (57589, 1000014082, 151019, 644575, 616701) AND cid IN (9790,9789))
        #     '''.format(tbl, ver, dtbl, old_ver, ppmonth, pmonth, cols=cols)
        #     dba.execute(sql)

        #     # 导入新数据
        #     sql = '''
        #         INSERT INTO {} ({cols}, c_out_ver) SELECT {cols}, {} FROM {}
        #         WHERE c_out_ver = {} AND date >= {} AND date < {} AND sid IN (57589, 1000014082, 151019, 644575, 616701) AND cid IN (9790,9789)
        #     '''.format(tbl, ver, dtbl, new_ver, ppmonth, pmonth, cols=cols)
        #     dba.execute(sql)

        # # 更新上上上月
        # if smonth > pppmonth:
        #     sql = 'SELECT max(c_out_ver) FROM {} WHERE date >= {} AND date < {}'.format(etbl, pppmonth, ppmonth)
        #     old_ver = dba.query_all(sql)[0][0]

        #     sql = 'SELECT max(c_out_ver) FROM {} WHERE date >= {} AND date < {}'.format(dtbl, pppmonth, ppmonth)
        #     new_ver = dba.query_all(sql)[0][0]

        #     if old_ver == new_ver:
        #         raise Exception('ver error {}'.format(old_ver))
        #         return

        #     # 导入旧数据
        #     sql = '''
        #         INSERT INTO {} ({cols}, c_out_ver) SELECT {cols}, {} FROM {}
        #         WHERE c_out_ver = {} AND date >= {} AND date < {} AND NOT (sid IN (57589, 1000014082, 151019, 644575, 616701) AND cid IN (9790,9789))
        #     '''.format(tbl, ver, dtbl, old_ver, pppmonth, ppmonth, cols=cols)
        #     dba.execute(sql)

        #     # 导入新数据
        #     sql = '''
        #         INSERT INTO {} ({cols}, c_out_ver) SELECT {cols}, {} FROM {}
        #         WHERE c_out_ver = {} AND date >= {} AND date < {} AND sid IN (57589, 1000014082, 151019, 644575, 616701) AND cid IN (9790,9789)
        #     '''.format(tbl, ver, dtbl, new_ver, pppmonth, ppmonth, cols=cols)
        #     dba.execute(sql)


    # 出数销售额检查
    def check_sales(self, tbl, dba, logId):
        dba, etbl = self.get_a_tbl()
        dba = self.cleaner.get_db(dba)

        sql = 'SELECT min(pkey), max(pkey), sum(sales), count(*) FROM {}'.format(tbl)
        ret = dba.query_all(sql)
        smonth, emonth, salesa, counta, = ret[0]

        sql = 'SELECT sum(sales*sign), sum(sign) FROM {} WHERE pkey >= \'{}\' AND pkey <= \'{}\' AND sales > 0 AND num > 0'.format(
            etbl, smonth, emonth
        )
        ret = dba.query_all(sql)
        salesb, countb = ret[0][0], ret[0][1]

        if salesa != salesb:
            raise Exception("output failed salesa:{} salesb:{} counta:{} countb:{}".format(salesa, salesb, counta, countb))

        sql = 'SELECT min(pkey), max(pkey), sum(c_sales), count(*) FROM {}'.format(tbl)
        ret = dba.query_all(sql)
        smonth, emonth, salesa, counta, = ret[0]

        if salesa != salesb:
            self.cleaner.add_log(warn="salesa:{} salesb:{}".format(salesa, salesb), logId=logId)