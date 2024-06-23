import sys
import time
import datetime
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app

class main(Batch.main):
    # 每月11号跑 p3 scripts/clean_import_brush_new.py -b 81 --process --start_month='2020-04-01' --end_month='2020-05-01'
    # def brush_2(self, smonth, emonth):
    #     sql = 'SELECT source, tb_item_id FROM {}'.format(self.cleaner.get_tbl())
    #     ret = self.cleaner.db26.query_all(sql)
    #     mpp = {'{}-{}'.format(v[0],v[1]):True for v in ret}

    #     clean_flag = self.cleaner.last_clean_flag() + 1

    #     sql = 'SELECT alias_all_bid FROM {} GROUP BY alias_all_bid'.format(self.get_entity_tbl())
    #     ret = self.cleaner.dbch.query_all(sql)
    #     for bid, in ret:
    #         uuids = []
    #         sql = '''
    #             SELECT argMax(uuid2, month), source, tb_item_id, sum(sign) c
    #             FROM {}_parts WHERE pkey >= '{}' AND pkey < '{}' AND alias_all_bid = {}
    #             GROUP BY source, tb_item_id ORDER BY c DESC LIMIT 300
    #         '''.format(self.get_entity_tbl(), smonth, emonth, bid)
    #         ret = self.cleaner.dbch.query_all(sql)
    #         for uuid2, source, tb_item_id, c, in ret:
    #             if '{}-{}'.format(source, tb_item_id) in mpp:
    #                 continue
    #             uuids.append(uuid2)
    #         self.cleaner.add_brush(uuids, clean_flag)

    #         print('{} add new brush {}'.format(bid, len(uuids)))

    #     return True

    # def brush_old_bak(self, smonth, emonth):
    #     bname, btbl = self.get_brush_product_tbl()
    #     bdba = self.cleaner.get_db(bname)

    #     cname, ctbl = self.get_c_tbl()

    #     sql = '''
    #         SELECT a.source, a.tb_item_id, b.spid7 FROM {} a join {} b using (pid) WHERE a.flag = 2
    #     '''.format(self.cleaner.get_tbl(), btbl)
    #     ret = self.cleaner.db26.query_all(sql)
    #     mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): True for v in ret}

    #     clean_flag = self.cleaner.last_clean_flag() + 1

    #     # sql = 'SELECT alias_all_bid FROM {} GROUP BY alias_all_bid'.format(self.get_entity_tbl())
    #     # ret = self.cleaner.dbch.query_all(sql)
    #     # for bid, in ret:
    #     bids = [[6],
    #             [14],
    #             [40],
    #             [61],
    #             [983],
    #             [990],
    #             [93306],
    #             [2003553],
    #             [6053191, 175319],
    #             [5891186, 1037]]
    #     for bid in bids:
    #         uuids = []
    #         bid_list = ','.join([str(t) for t in bid])
    #         # sql = '''
    #         #     SELECT argMax(a.uuid2, a.month), a.source, a.tb_item_id, sum(a.ss) sumsales
    #         #     FROM (SELECT uuid2, month, source, tb_item_id, uniq_k, sales*sign/num*sign avg_price, sales*sign ss FROM {}_parts WHERE pkey > pkey >= '{smonth}' AND pkey < '{emonth}'
    #         #     AND  alias_all_bid in ({}) AND avg_price > 10000) a
    #         #     JOIN (SELECT source, uniq_k FROM artificial.entity_{}_clean WHERE month > '{smonth}' and month < '{emonth}' and sp8 !='剔除' GROUP BY source, tb_item_id, sp7) b
    #         #     USING (source, uniq_k) GROUP BY a.source, a.tb_item_id
    #         #     ORDER BY sumsales DESC LIMIT 300
    #         # '''.format(self.get_entity_tbl(), bid, self.cleaner.eid, smonth=smonth, emonth=emonth)
    #         sql = '''
    #         select uuid2, source, tb_item_id, avg_price, sumsales from
    #         (SELECT argMax(uuid2,month) uuid2, source, tb_item_id, sum(sales*sign)/sum(num*sign) avg_price, sum(sales*sign) sumsales FROM {}_parts
    #         WHERE pkey >= '{smonth}' AND pkey < '{emonth}' AND alias_all_bid in ({})
    #         and uniq_k in (select uniq_k from artificial.entity_{}_clean WHERE pkey > '{smonth}' and pkey < '{emonth}' and sp8 !='剔除' GROUP BY uniq_k, sp7)
    #         GROUP BY source, tb_item_id)
    #         where avg_price > 20000
    #         ORDER BY sumsales DESC LIMIT 300
    #         '''.format(self.get_entity_tbl(), bid_list, self.cleaner.eid, smonth=smonth, emonth=emonth)
    #         # sql = '''   select uuid2, source, tb_item_id, c, ss, sn, ss/sn from (
    #         #             SELECT argMax(uuid2, month) uuid2, source, tb_item_id, sum(sign) c, sum(sales*sign) ss, sum(num*sign) sn, ss/sn
    #         #             FROM {}_parts WHERE pkey >= '{smonth}' AND pkey < '{emonth}' AND alias_all_bid = {}
    #         #             and uniq_k in (SELECT uniq_k FROM artificial.entity_{}_clean WHERE month >= '{smonth}' and month < '{emonth}' and sp8 !='剔除')
    #         #             GROUP BY source, tb_item_id) where ss > 10000
    #         #             ORDER BY ss DESC LIMIT 300
    #         #         '''.format(self.get_entity_tbl(), bid, self.cleaner.eid, smonth=smonth, emonth=emonth)
    #         ret = self.cleaner.dbch.query_all(sql)
    #         if len(ret) > 0:
    #             tb_item_id_list = ','.join(['\''+str(v[2])+'\'' for v in ret])
    #             sql = '''
    #                 SELECT argMax(uuid2,month), source, tb_item_id FROM {}_parts WHERE uniq_k in (SELECT uniq_k from artificial.entity_{}_clean where sp7 = '5G') AND tb_item_id in ({})
    #                 and pkey >= '{}' AND pkey < '{}' group by tb_item_id,source
    #             '''.format(self.get_entity_tbl(), self.cleaner.eid, tb_item_id_list, smonth, emonth)
    #             ret = self.cleaner.dbch.query_all(sql)
    #             for uuid2, source, tb_item_id in ret:
    #                 if '{}-{}-5G'.format(source, tb_item_id) in mpp:
    #                     continue
    #                 uuids.append(uuid2)

    #             sql = '''
    #                 SELECT argMax(uuid2,month), source, tb_item_id FROM {}_parts WHERE uniq_k in (SELECT uniq_k from artificial.entity_{}_clean where sp7 = '非5G') AND tb_item_id in ({})
    #                 and pkey >= '{}' AND pkey < '{}' group by tb_item_id,source
    #             '''.format(self.get_entity_tbl(), self.cleaner.eid, tb_item_id_list, smonth, emonth)
    #             ret = self.cleaner.dbch.query_all(sql)
    #             for uuid2, source, tb_item_id in ret:
    #                 if '{}-{}-非5G'.format(source, tb_item_id) in mpp:
    #                     continue
    #                 uuids.append(uuid2)

    #             self.cleaner.add_brush(uuids, clean_flag)
    #             print('{} add new brush {}'.format(bid, len(uuids)))

    #     return True

    def brush(self, smonth, emonth, logId=-1):
        # 默认规则
        bdba, btbl = self.get_brush_product_tbl()
        bdba = self.cleaner.get_db(bdba)

        cname, ctbl = self.get_c_tbl()
        cdba = self.cleaner.get_db(cname)

        clean_flag = self.cleaner.last_clean_flag() + 1

        bids = [[6],
                [14],
                [40],
                [61],
                [983],
                [990],
                [93306],
                [2003553],
                [6053191, 175319],
                [5891186, 1037]]


        fsql = 'SELECT a.snum, a.tb_item_id, b.spid7, a.flag, a.id FROM {} a join {} b using (pid)'.format(self.cleaner.get_tbl(), btbl)
        old_ids = []

        sql = '''
            SELECT `source`, item_id FROM (
                SELECT source, item_id, any(sp7) nsp7 FROM {}
                WHERE pkey >= '{}' AND pkey < '{}'
                GROUP BY item_id,source HAVING countDistinct(sp7) = 1
            ) WHERE nsp7 = '非5G'
        '''.format(ctbl, smonth, emonth)
        ret = cdba.query_all(sql)
        only_4g_items = {'{}-{}'.format(v[0], v[1]):True for v in ret}

        for bid in bids:
            uuids = []
            bid_list = ','.join([str(t) for t in bid])

            where = 'sales/num>20000 AND uuid2 IN (SELECT uuid2 FROM {} WHERE sp8 !=\'剔除\' AND alias_all_bid IN ({}))'.format(ctbl, bid_list)
            ret, sales_by_uuid = self.cleaner.process_top_anew(smonth, emonth, limit=300, where=where, ignore_p1=True)

            if len(ret) > 0:
                item_ids = ','.join(['\''+str(v[2])+'\'' for v in ret])

                sql = '''
                    SELECT argMax(uuid2, date), source, item_id, any(sp3), any(sp6) FROM {}
                    WHERE sp7 = '5G' AND item_id in ({}) AND pkey >= '{}' AND pkey < '{}'
                    GROUP BY item_id,source
                '''.format(ctbl, item_ids, smonth, emonth)
                ret = cdba.query_all(sql)
                for uuid2, source, item_id, sp3, sp6 in ret:
                    # print('sp3:', sp3, 'sp6:', sp6)
                    # if self.skip_brush(source, item_id, '5G', sql=fsql) and (sp3 == sp6):
                    if self.skip_brush(source, item_id, '5G', sql=fsql):
                        print('continue')
                        continue
                    print('add')
                    uuids.append(uuid2)

                sql = '''
                    SELECT argMax(uuid2, date), source, item_id, any(sp3), any(sp6) FROM {}
                    WHERE sp7 = '非5G' AND item_id in ({}) AND pkey >= '{}' AND pkey < '{}'
                    GROUP BY item_id,source
                '''.format(ctbl, item_ids, smonth, emonth)
                ret = cdba.query_all(sql)
                for uuid2, source, item_id, sp3, sp6 in ret:
                    if self.skip_brush(source, item_id, '非5G', sql=fsql):
                        continue

                    # 机洗只有4G 但答题都是5G的 不重复出题
                    if '{}-{}'.format(source, item_id) in only_4g_items and self.skip_brush(source, item_id, '5G', add=False, sql=fsql):
                        continue

                    uuids.append(uuid2)

                self.cleaner.add_brush(uuids, clean_flag)
                print('{} add new brush {}'.format(bid, len(uuids)))

        return True


    def brush_0226(self,smonth,emonth):
        where = 'item_id = \'630722241600\''
        ret, sales_by_uuid = self.cleaner.process_top_anew(smonth, emonth, where=where)
        uuids = []
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids.append(uuid2)
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid)

    # def brush_modify2(self, smonth, emonth):
    #     # 对特定几个出题，修改pid
    #     bdba, btbl = self.get_brush_product_tbl()
    #     bdba = self.cleaner.get_db(bdba)
    #     sql = '''
    #     SELECT a.source, a.tb_item_id, b.spid7 FROM {} a join {} b using (pid) WHERE a.flag != 0
    #     '''.format(self.cleaner.get_tbl(), btbl)
    #     ret = self.cleaner.db26.query_all(sql)
    #     mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): True for v in ret}

    #     clean_flag = self.cleaner.last_clean_flag()
    #     where = 'tb_item_id = \'621393332500\' or uuid2 in (select uuid2 from {} where sp3=\'REDMI 9\')'.format(self.get_clean_tbl())
    #     uuids = []
    #     sql = '''
    #     select uuid2, source, tb_item_id, avg_price, sumsales from
    #     (SELECT argMax(uuid2,month) uuid2, source, tb_item_id, sum(sales*sign)/sum(num*sign) avg_price, sum(sales*sign) sumsales FROM {}_parts
    #     WHERE pkey >= '{smonth}' AND pkey < '{emonth}'
    #     GROUP BY source, tb_item_id)
    #     where avg_price > 20000 and uuid2 in (select uuid2 from {} WHERE pkey > '{smonth}' and pkey < '{emonth}' and sp8 !='剔除' and (item_id='621393332500') GROUP BY uuid2, sp7)
    #     ORDER BY sumsales DESC LIMIT 300
    #     '''.format(self.get_entity_tbl(), self.get_clean_tbl(), smonth=smonth, emonth=emonth)
    #     ret = self.cleaner.dbch.query_all(sql)
    #     if len(ret) > 0:
    #         tb_item_id_list = ','.join(['\''+str(v[2])+'\'' for v in ret])
    #         sql = '''
    #         SELECT argMax(uuid2,month), source, tb_item_id FROM {}_parts WHERE uuid2 in (SELECT uuid2 from {} where sp7 = '5G') AND tb_item_id in ({})
    #         and pkey >= '{}' AND pkey < '{}' group by tb_item_id,source
    #         '''.format(self.get_entity_tbl(), self.get_clean_tbl(), tb_item_id_list, smonth, emonth)
    #         ret = self.cleaner.dbch.query_all(sql)
    #         for uuid2, source, tb_item_id in ret:
    #             if '{}-{}-5G'.format(source, tb_item_id) in mpp:
    #                 continue
    #             uuids.append(uuid2)

    #         sql = '''
    #         SELECT argMax(uuid2,month), source, tb_item_id FROM {}_parts WHERE uuid2 in (SELECT uuid2 from {} where sp7 = '非5G') AND tb_item_id in ({})
    #         and pkey >= '{}' AND pkey < '{}' group by tb_item_id,source
    #         '''.format(self.get_entity_tbl(), self.get_clean_tbl(), tb_item_id_list, smonth, emonth)
    #         ret = self.cleaner.dbch.query_all(sql)
    #         for uuid2, source, tb_item_id in ret:
    #             if '{}-{}-非5G'.format(source, tb_item_id) in mpp:
    #                 continue
    #             uuids.append(uuid2)

    #         self.cleaner.add_brush(uuids, clean_flag)
    #         print('add new brush {}'.format(len(uuids)))

    #     return True

    # def brush_3(self, smonth, emonth):
    #     # sql = '''
    #     #             SELECT a.source, a.tb_item_id, b.spid7 FROM {} a join {} b using (pid) WHERE a.flag = 2
    #     #       '''.format(self.cleaner.get_tbl(), self.get_brush_product_tbl())
    #     # ret = self.cleaner.db26.query_all(sql)
    #     # mpp = {'{}-{}-{}'.format(v[0],v[1],v[2]):True for v in ret}
    #     clean_flag = self.cleaner.last_clean_flag() + 1
    #     mpp = {}
    #     uuids = []
    #     tb_item_id_list = " '606379797462', '597343975159' "
    #     sql = '''
    #     SELECT argMax(uuid2,month), source, tb_item_id FROM {}_parts WHERE uniq_k in (SELECT uniq_k from artificial.entity_{}_clean where sp7 = '5G') AND tb_item_id in ({})
    #     and source = 'tmall' and pkey >= '{}' AND pkey < '{}' group by tb_item_id,source
    #     '''.format(self.get_entity_tbl(), self.cleaner.eid, tb_item_id_list, smonth, emonth)
    #     ret = self.cleaner.dbch.query_all(sql)
    #     for uuid2, source, tb_item_id in ret:
    #         if '{}-{}-5G'.format(source, tb_item_id) in mpp:
    #             continue
    #         uuids.append(uuid2)

    #     sql = '''
    #     SELECT argMax(uuid2,month), source, tb_item_id FROM {}_parts WHERE uniq_k in (SELECT uniq_k from artificial.entity_{}_clean where sp7 = '非5G') AND tb_item_id in ({})
    #     and source = 'tmall' and pkey >= '{}' AND pkey < '{}' group by tb_item_id,source
    #     '''.format(self.get_entity_tbl(), self.cleaner.eid, tb_item_id_list, smonth, emonth)
    #     ret = self.cleaner.dbch.query_all(sql)
    #     for uuid2, source, tb_item_id in ret:
    #         if '{}-{}-非5G'.format(source, tb_item_id) in mpp:
    #             continue
    #         uuids.append(uuid2)
    #     # self.cleaner.add_brush(uuids, clean_flag)
    #     print('add new brush {}'.format(len(uuids)))
    #     return True

    # 补充出题
    # def brush_5G(self):
    #     sql = '''
    #         SELECT a.tb_item_id, b.spid7, a.month
    #         FROM product_lib.entity_90745_item a JOIN product_lib.product_90745 b USING(pid)
    #         WHERE a.flag > 0
    #     '''.format(self.cleaner.get_tbl())
    #     ret = self.cleaner.db26.query_all(sql)
    #     smonth = min([v[2] for v in ret])
    #     emonth = max([v[2] for v in ret])

    #     uuids = []
    #     for tb_item_id, spid7, month, in ret:
    #         sql = '''
    #             SELECT argMax(uuid2, month) FROM {tbl}_parts WHERE uniq_k IN (
    #                 SELECT uniq_k FROM artificial.entity_{}_clean WHERE uniq_k IN (
    #                     SELECT uniq_k FROM {tbl}_parts WHERE tb_item_id = '{}'
    #                 ) AND sp7 != '{}' AND sp7 != ''
    #             ) AND month >= '{}' AND month <= '{}'
    #         '''.format(self.cleaner.eid, tb_item_id, spid7, smonth, emonth, tbl=self.get_entity_tbl())
    #         rrr = self.cleaner.dbch.query_all(sql)
    #         if len(rrr) > 0:
    #             uuids.append(rrr[0][0])

    #     clean_flag = self.cleaner.last_clean_flag() + 1
    #     self.cleaner.add_brush(uuids, clean_flag)
    #     print('add new brush {}'.format(len(uuids)))

    def brush_similarity(self, v1, v2, where, item_ids, fix=False):
        k = 'brush_similarity {}'.format(where)
        if k not in self.cleaner.cache:
            self.cleaner.cache[k] = {}

            cname, ctbl = self.get_c_tbl()
            cdba = self.cleaner.get_db(cname)

            sql = 'SELECT uuid2, sp7 FROM {} WHERE ({}) AND item_id IN ({})'.format(ctbl, where, item_ids)
            ret = cdba.query_all(sql)

            for uuid2, sp7, in ret:
                self.cleaner.cache[k][str(uuid2)] = sp7

        if str(v2['uuid2']) in self.cleaner.cache[k]:
            sp7 = self.cleaner.cache[k][str(v2['uuid2'])]
        else:
            sp7 = ''

        if sp7 != v1['split_pids'][0]['sp7']:
            return 1

        if str(v1['uuid2']) == str(v2['uuid2']):
            return 3

        if str(v1['pkey']) == str(v2['pkey']):
            return 2

        if v2['date'] >= v1['month']:
            return 1.1
        return 1


    def finish(self, tbl, dba, prefix):
        # 日单价>5w rmb,或者（宝贝日单价>=8888且日单价>月均价的2倍） 用单价不异常的记录里面date距离异常单价日最近的价格
        sql = '''
            WITH c_props.value[7] AS p1
            SELECT source, item_id, p1, uuid2, pkey, cid, toYYYYMM(date), toUnixTimestamp(CAST(pkey AS DateTime)), c_sales, c_num FROM {}
            WHERE c_sales/c_num >= 888800
        '''.format(tbl)
        rr1 = dba.query_all(sql)

        if len(rr1) == 0:
            return

        # 这个链接有异常高价且无评论，直接拉入黑名单
        sql = '''
            ALTER TABLE {} UPDATE `c_props.value` = arrayMap((k, v) -> IF(k='是否剔除', '剔除', v), c_props.name, c_props.value)
            WHERE item_id = '647701044007'
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `c_props.value` = arrayMap((k, v) -> IF(k='是否剔除', '剔除', v), c_props.name, c_props.value)
            WHERE item_id = '654578360475' and trade_props.value = ['无','新品','SA/NSA双模(5G)','官方标配'] and c_sales/c_num = 9999900
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            WITH c_props.value[7] AS p1
            SELECT source, item_id, p1, uuid2, pkey, cid, toYYYYMM(date), toUnixTimestamp(CAST(pkey AS DateTime)), c_sales, c_num FROM {}
            WHERE (source, item_id, p1) IN ({}) AND c_sales/c_num > 20000 AND c_sales/c_num < 5000000
        '''.format(tbl, ','.join(['({}, \'{}\', \'{}\')'.format(v[0],v[1],v[2]) for v in rr1]))
        rr2 = dba.query_all(sql)

        mpp = {}
        for v in rr2:
            k = '{}-{}-{}'.format(v[0], v[1], v[2])
            if k not in mpp:
                mpp[k] = []
            mpp[k].append(v)

        for source, item_id, p1, uuid2, pkey, cid, m, t, s, n, in rr1:
            k = '{}-{}-{}'.format(source, item_id, p1)
            if k not in mpp:
                continue
            items, avg = mpp[k], []
            for v in items:
                if v[-4] == m:
                    avg.append(v[-2] / v[-1])
            if len(avg) == 0:
                continue
            avg = sum(avg) / len(avg)
            if s / n < avg * 2:
                continue

            near_d, n_sales = sys.maxsize, 0
            for v in items:
                if v[-2] / v[-1] > avg * 2:
                    continue
                tt = abs(t - v[-3])
                if tt < near_d:
                    near_d, n_sales = tt, int(v[-2] / v[-1])

            sql = '''
                ALTER TABLE {} UPDATE c_sales=c_num*{}
                WHERE source={} AND pkey='{}' AND cid={} AND uuid2={}
            '''.format(tbl, n_sales, source, pkey, cid, uuid2)
            dba.execute(sql)

            while not self.cleaner.check_mutations_end(dba, tbl):
                time.sleep(5)

        # 日单价<=200元的商品，sp8改成剔除。
        sql = '''
            ALTER TABLE {} UPDATE `c_props.value` = arrayMap((k, v) -> IF(k='是否剔除', '剔除', v), c_props.name, c_props.value)
            WHERE c_sales/c_num <= 20000
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `c_props.value` = arrayMap((k, v) -> IF(k='是否剔除', '剔除', v), c_props.name, c_props.value)
            WHERE c_sku_id = 448
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `c_props.value` = arrayMap((k, v) -> IF(k='是否剔除', '剔除', v), c_props.name, c_props.value)
            WHERE item_id = '630781621327' and trade_props.value = ['128GB','海蓝色.','官方标配','无需合约版']
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `c_props.value` = arrayMap((k, v) -> IF(k='是否剔除', '剔除', v), c_props.name, c_props.value)
            WHERE item_id in ('629737844658', '629981279447', '633378051991', '633726988408');
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `c_props.value` = arrayMap((k, v) -> IF(k='是否剔除', '剔除', v), c_props.name, c_props.value)
            WHERE item_id in ('630084404726', '638603323752') and pkey >= '2021-05-01' and pkey < '2021-06-01';
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        # 这个链接的有部分价格是9999元，改成3000元（异常高价）
        sql = '''
                WITH c_props.value[7] AS p1
                SELECT source, item_id, p1, uuid2, pkey, cid, toYYYYMM(date), toUnixTimestamp(CAST(pkey AS DateTime)), c_sales, c_num FROM {}
                WHERE c_sales/c_num >= 999900 and item_id = '645279456896'
            '''.format(tbl)
        rr1 = dba.query_all(sql)

        if len(rr1) == 0:
            return

        for source, item_id, p1, uuid2, pkey, cid, m, t, s, n, in rr1:
            k = '{}-{}-{}'.format(source, item_id, p1)
            sql = '''
                ALTER TABLE {} UPDATE c_sales=c_num*{}
                WHERE source={} AND pkey='{}' AND cid={} AND uuid2={}
            '''.format(tbl, 300000, source, pkey, cid, uuid2)
            dba.execute(sql)

            while not self.cleaner.check_mutations_end(dba, tbl):
                time.sleep(5)





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