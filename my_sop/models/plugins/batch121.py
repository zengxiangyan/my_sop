import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import pandas as pd

class main(Batch.main):
    def brush_2(self, smonth, emonth):
        sql = 'SELECT source, tb_item_id, real_p1 FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0],v[1],v[2]):True for v in ret}

        sql = 'select distinct(cid) from {}'.format(self.get_entity_tbl())
        cids = [str(v[0]) for v in self.cleaner.dbch.query_all(sql)]
        uuids = []
        sales_by_uuids = {}
        count = []
        for cid in cids:
            c = 0
            where = 'cid = {}'.format(cid)
            # ret, sales_by_uuids_1 = self.cleaner.process_top(smonth, emonth, limit=100, where=where)
            #
            limit = 100
            tbl = self.get_entity_tbl()+'_parts'
            sql = '''
                        SELECT sum(sales*sign) FROM {} WHERE month >= '{}' AND month < '{}' {}
                    '''.format(tbl, smonth, emonth, '' if where == '' else 'AND ({})'.format(where))
            ret = self.cleaner.dbch.query_all(sql)
            total = ret[0][0]
            sql = '''
                        SELECT tb_item_id, sum(sales*sign) ss FROM {}
                        WHERE month >= '{}' AND month < '{}' AND sales > 0 AND num > 0 {}
                        GROUP BY tb_item_id
                        ORDER BY ss DESC
                        LIMIT {}
                    '''.format(tbl, smonth, emonth, '' if where == '' else 'AND ({})'.format(where), limit)
            ret = self.cleaner.dbch.query_all(sql)
            tb_item_ids = []
            for tb_item_id, ss in ret:
                tb_item_ids.append('\'{}\''.format(tb_item_id))
            tb_item_ids = ','.join(tb_item_ids)
            where = 'tb_item_id in ({})'.format(tb_item_ids)

            ret, sales_by_uuids_1 = self.cleaner.process_top(smonth, emonth, where=where)
            sales_by_uuids.update(sales_by_uuids_1)
            for uuid2, source, tb_item_id, p1, in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    continue
                uuids.append(uuid2)
                c = c + 1
                mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True
            count.append([cid, c])

        clean_flag = self.cleaner.last_clean_flag() + 1
        # self.cleaner.add_brush(uuids, clean_flag, 1)
        print('add new brush {}'.format(len(uuids)))
        for i in count:
            print(i)

        return True

    def brush_3(self, smonth, emonth):
        sql = 'SELECT source, tb_item_id, real_p1 FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0],v[1],v[2]):True for v in ret}
        uuids = []
        # where = 'source=\'tmall\' and uuid2 in (select uuid2 from {} where sp5 in (\'牙膏\',\'漱口水\') and source=1)'.format(self.get_clean_tbl())
        where = 'source=\'tmall\''
        ret, sales_by_uuids = self.cleaner.process_top(smonth, emonth, rate=0.8, where=where)
        for uuid2, source, tb_item_id, p1, in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            uuids.append(uuid2)
            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True

        clean_flag = self.cleaner.last_clean_flag() + 1
        # self.cleaner.add_brush(uuids, clean_flag, 1)
        print('add new brush {}'.format(len(uuids)))

        return True

    def brush_default(self, smonth, emonth):
        sql = 'SELECT source, tb_item_id, real_p1 FROM {} where flag > 0'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): True for v in ret}
        sql = 'select name from cleaner.clean_target where batch_id = 121 and pos_id = 5'
        sp5s = [v[0] for v in self.cleaner.db26.query_all(sql)]
        top = 2000
        rand = 1000

        uuid1 = []
        uuid2 = []
        sales_by_uuids = {}
        ret, sales_by_uuid1 = self.cleaner.process_top_new_default(smonth, emonth, limit=top*0.04, where='uuid2 in (select uuid2 from {} where sp1=\'其它\')'.format(self.get_clean_tbl()))
        sales_by_uuids.update(sales_by_uuid1)
        #
        # print(ret)
        for uuid2, source, tb_item_id, p1 in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            uuid1.append(uuid2)
            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True
        ret = self.cleaner.process_rand_new_default(smonth, emonth, limit=rand*0.04, where='uuid2 in (select uuid2 from {} where sp1=\'其它\')'.format(self.get_clean_tbl()))
        # print('rand ret {}'.format(len(ret)))
        for uuid2, source, tb_item_id, p1, cid in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            uuid2.append(uuid2)
            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True
        #
        for sp5 in sp5s:
            ret, sales_by_uuid1 = self.cleaner.process_top_new_default(smonth, emonth, limit=(top*0.24),
                                                        where='uuid2 in (select uuid2 from {} where sp5=\'{}\')'.format(self.get_clean_tbl(), sp5))
            sales_by_uuids.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1 in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    continue
                uuid1.append(uuid2)
                mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True
            ret = self.cleaner.process_rand_new_default(smonth, emonth,  limit=(rand*0.24),
                                                        where='uuid2 in (select uuid2 from {} where sp5=\'{}\')'.format(self.get_clean_tbl(), sp5))
            print('rand ret {}'.format(len(ret)))
            for uuid2, source, tb_item_id, p1, cid in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    continue
                uuid2.append(uuid2)
                mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True

        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuid1, clean_flag, 1, sales_by_uuids)
        self.cleaner.add_brush(uuid2, clean_flag, 2)
        print('add new brush {}, {}'.format(len(set(uuid1)), len(set(uuid2))))
        return True

    def each_month(self, s, e):
        i = []
        for y in range(s[0], e[0] + 1):
            if y == s[0] and s[0] < s[1]:
                for m in range(s[1], 13):
                    a = ['{}-{}-{}'.format(y, str.zfill(str(m), 2), '01'),
                         '{}-{}-{}'.format((y if m < 12 else y + 1), str.zfill(str(m + 1 if m < 12 else 1), 2), '01')]
                    print(a)
                    i.append(a)
            elif y == e[0]:
                for m in range(1, e[1]):
                    a = ['{}-{}-{}'.format(y, str.zfill(str(m), 2), '01'),
                         '{}-{}-{}'.format((y if m < 12 else y + 1), str.zfill(str(m + 1 if m < 12 else 1), 2), '01')]
                    print(a)
                    i.append(a)
            else:
                for m in range(1, 13):
                    a = ['{}-{}-{}'.format(y, str.zfill(str(m), 2), '01'),
                         '{}-{}-{}'.format((y if m < 12 else y + 1), str.zfill(str(m + 1 if m < 12 else 1), 2), '01')]
                    print(a)
                    i.append(a)
        return i

    def brush_20201113(self, smonth, emonth):
        # sql = 'SELECT source, tb_item_id, real_p1 FROM {} where flag > 1'.format(self.cleaner.get_tbl())
        # ret = self.cleaner.db26.query_all(sql)
        # mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): True for v in ret}

        alias_all_bid = [51972, 52737, 52744, 52732, 52734, 47596, 52746, 52747, 5536723, 180, 185236, 52733, 1230629, 52786, 242293, 52882, 3197123, 52808, 2117468, 52728]
        alias_all_bid = ','.join(str(v) for v in alias_all_bid)
        sql = 'SELECT bid FROM brush.all_brand WHERE alias_bid IN ({bids}) OR bid IN ({bids})'.format(bids=alias_all_bid)
        ret = self.cleaner.db26.query_all(sql)
        bids = [str(v[0]) for v in ret]
        # cids = [213203, 50012453, 50012454, 50024985, 50026453, 50026454, 121408016, 121416025, 121480017, 125076027, 1405, 1407, 11930, 16806, 16807, 16812, 16815, 391, 407, 949, 1803, 1806, 2254, 5440, 5441, 9408, 316563, 316564, 316566, 21675779, 21675780, 35001582]
        cids = [213203,50012453,50012454,50024985,50026453,50026454,121408016,121416025,121480017,125076027,122376001,50015564,50013976]
        where = 'snum = 1 and shop_type >= 20 and all_bid in ({}) and cid in ({})'.format(','.join(bids), ','.join([str(v) for v in cids]))


        s = [int(smonth.split('-')[0]), int(smonth.split('-')[1])]
        e = [int(emonth.split('-')[0]), int(emonth.split('-')[1])]
        # s = [2017, 1]
        # e = [2020, 8]
        clean_flag = self.cleaner.last_clean_flag()
        # 放在clean_flag3里
        uuids = []
        c = []
        for each_smonth, each_emonth in self.each_month(s, e):
            cc = 0
            ret, sales_by_uuid = self.cleaner.process_top(each_smonth, each_emonth, rate=0.8, where=where)
            for uuid2, source, tb_item_id, p1, in ret:
                # if tb_item_id in ['552982987824','312606','1068549',552982987824,312606,1068549]:
                #     print('yeeeeeeeeeees', tb_item_id)
                #     print(self.skip_brush(source, tb_item_id, p1, remove=True))
                if self.skip_brush(source, tb_item_id, p1, remove=True):
                    continue
                # if tb_item_id in ['552982987824', '312606', '1068549', 552982987824, 312606, 1068549]:
                #     print('yeeeeeeeeeees', tb_item_id)
                uuids.append(uuid2)
                cc = cc + 1
            print('add new brush {}'.format(len(uuids)))
            c.append([each_smonth, each_emonth, cc])
        self.cleaner.add_brush(uuids, clean_flag, 1)
        for i in c:
            print(i)
        return True

    def brush_bak(self, smonth, emonth):
        uuids = []
        where = 'uuid2 in (select uuid2 from {} where sp1=\'漱口水/牙膏/牙粉\') and source != \'tb\''.format(self.get_clean_tbl())
        ret, sales_by_uuid = self.cleaner.process_top(smonth, emonth, rate=0.8, limit=99999999,where=where)
        for uuid2, source, tb_item_id, p1, alias_all_bid in ret:

            uuids.append(uuid2)
        # clean_flag = self.cleaner.last_clean_flag() + 1
        # self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid)
        print('121 add new brush {}'.format(len(set(uuids))))
        return True

    def brush(self, smonth, emonth, logId=-1):
        # sql = 'delete FROM {} where flag = 0'.format(self.cleaner.get_tbl())
        # self.cleaner.db26.execute(sql)
        # self.cleaner.db26.commit(sql)
        uuids = []
        sales_by_uuid = {}
        aname, atbl = self.get_a_tbl()
        cname, ctbl = self.get_c_tbl()
        db = self.cleaner.get_db(aname)
        cc = {}
        for each_source in ['source = 1 and (shop_type > 20 or shop_type < 10 )', 'source = 1 and (shop_type < 20 and shop_type > 10 )', 'source = 2', 'source = 3', 'source = 5', 'source =6']:
            for smonth, emonth in [['2019-01-01','2021-01-01'],['2021-01-01','2021-05-01']]:
                top_brand_sql = '''select alias_all_bid,sum(sales*sign) as ss from {atbl} where uuid2 in (select uuid2 from {ctbl} where sp1='漱口水/牙膏/牙粉'
                and sp5 = '牙膏' and pkey >= '{smonth}' and pkey < '{emonth}') and {each_source}
                group by alias_all_bid order by ss desc limit 20'''.format(atbl=atbl, ctbl=ctbl, each_source=each_source, smonth=smonth, emonth=emonth)
                bids = [v[0] for v in db.query_all(top_brand_sql)]
                for each_bid in bids:
                    c= 0
                    where = 'uuid2 in (select uuid2 from {} where sp1=\'漱口水/牙膏/牙粉\' and sp5 = \'牙膏\') and {} and alias_all_bid = {}'.format(ctbl,each_source, each_bid)
                    ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, rate=0.8,where=where)
                    sales_by_uuid.update(sales_by_uuid1)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if self.skip_brush(source, tb_item_id, p1):
                            continue
                        uuids.append(uuid2)
                        c = c + 1
                    cc['source = {} and bid = {} and smonth = {}'.format(each_source, each_bid, smonth)] = c
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid)
        print('121 add new brush {}'.format(len(set(uuids))))

        # for i in cc:
        #     print(i, cc[i])
        return True

    # # 预处理
    # def pre_brush_modify(self, v, products, prefix):
    #     v['spid17'] = ''

    #     for pid in products:
    #         products[pid]['spid17'] = ''
    #     import pdb;pdb.set_trace()

    # def pre_brush_modify(self, v, products, prefix):
    #     if v['flag'] == 2 and v['split'] > 0:
    #         tbl = self.cleaner.get_tbl()
    #         db26 = self.cleaner.get_db(self.cleaner.db26name)

    #         sql = 'SELECT pid, number FROM {}_split WHERE entity_id = {}'.format(tbl, v['id'])
    #         vvv = db26.query_all(sql)

    #         spid35 = {}
    #         for pid, number, in vvv:
    #             product = products[pid]
    #             k = product['spid24'].replace('g', '')
    #             if k == '':
    #                 continue
    #             if k not in spid35:
    #                 spid35[k] = 0
    #             spid35[k] += number

    #         tmp = []
    #         for k in spid35:
    #             tmp.append('{}*{}'.format(k, spid35[k]))
    #         v['spid35'] = '+'.join(tmp)
    #         v['split'] = 0
    #     return v


    def calc_splitrate(self, item, data, custom_price={}):
        if len(data) <= 1:
            return data

        source = str(0 if item['snum']==1 and item['pkey'].day<20 else item['snum'])

        if 'sku_price' not in self.cleaner.cache:
            dba, atbl = self.get_all_tbl()
            _, stbl = self.get_sku_price_tbl()
            dba = self.cleaner.get_db(dba)
            db26 = self.cleaner.get_db('26_apollo')
            tbl = self.cleaner.get_product_tbl()

            sql = 'SELECT pid FROM {} WHERE name LIKE \'\\\\_\\\\_\\\\_%\' OR alias_all_bid = 0'.format(tbl)
            ret = db26.query_all(sql)
            ret = list(ret) + [[0]]

            pids = {}

            sql = '''
                WITH IF(`source`=1 AND shop_type>10 AND shop_type<20,0,`source`) AS s
                SELECT toString(b_pid) k, median(price/`b_number`) FROM {}
                WHERE b_pid NOT IN ({})
                GROUP BY k
            '''.format(stbl, ','.join([str(v) for v, in ret]))
            rrr = dba.execute(sql)
            pids.update({v[0]: v[1] for v in rrr})

            # 防止历史数据缺失先不限时间
            sql = '''
                WITH IF(`source`=1 AND shop_type>10 AND shop_type<20,0,`source`) AS s
                SELECT concat(toString(b_pid),'_',toString(s)) k, median(price/`b_number`) FROM {}
                WHERE b_pid NOT IN ({})
                GROUP BY k
            '''.format(stbl, ','.join([str(v) for v, in ret]), source)
            rrr = dba.execute(sql)
            pids.update({v[0]: v[1] for v in rrr})

            sql = '''
                WITH IF(`source`=1 AND shop_type>10 AND shop_type<20,0,`source`) AS s
                SELECT concat(toString(b_pid),'_',toString(s)) k, median(price/`b_number`) FROM {}
                WHERE b_similarity = 0 AND b_pid NOT IN ({})
                AND date >= toStartOfMonth(date_sub(MONTH, 6, toDate(NOW())))
                GROUP BY k
            '''.format(stbl, ','.join([str(v) for v, in ret]), source)
            rrr = dba.execute(sql)
            pids.update({v[0]: v[1] for v in rrr})

            self.cleaner.cache['sku_price'] = pids

        pids = {v['pid']: self.cleaner.cache['sku_price'][str(v['pid'])+'_'+str(source)] if str(v['pid'])+'_'+str(source) in self.cleaner.cache['sku_price'] else self.cleaner.cache['sku_price'][str(v['pid'])] for v in data if str(v['pid']) in self.cleaner.cache['sku_price']}
        pids.update({v['pid']: custom_price[v['pid']] for v in data if v['pid'] in custom_price})

        item['price'] = item['sales'] / max(item['num'],1)
        item['price'] = max(item['price'],1)

        if len(pids.keys()) == len(data):
            # 按价值比例分
            total = sum([pids[v['pid']]*v['number'] for v in data])
            rate, vv = 1, None
            for v in data:
                r = pids[v['pid']]*v['number']/total
                r = round(r,3)
                rate -= r
                v['split_rate'] = r
                v['split_flag'] = 0
                if vv is None or v['split_rate'] > vv['split_rate']:
                    vv = v
            if rate != 0:
                vv['split_rate']+= round(rate, 3)
        else:
            # 按件数分
            total = sum([v['number'] for v in data])
            rate, vv = 1, None
            for v in data:
                r = v['number']/total
                r = round(r,3)
                rate -= r
                v['split_rate'] = r
                v['split_flag'] = 1
                if vv is None or v['split_rate'] > vv['split_rate']:
                    vv = v
            if rate != 0:
                vv['split_rate']+= round(rate, 3)

        return data


    def brush_modify(self, v, bru_items):
        sp16, sp14 = 0, 0
        for vv in v['split_pids']:
            if vv['pid'] > 0:
                vv['sp26'] = vv['sku_name'].split('[')[0]

            if len(v['split_pids']) == 1:
                sp16 = vv['sp28']
            else:
                sp16+= vv['number']

            if vv['sp24'] == '' and vv['sp35'] == '':
                continue

            if vv['sp35'] == '':
                vv['sp35'] = '{}*{}'.format(vv['sp24'].replace('g', ''), vv['number'])
            vv['sp35'] = str(round(eval(vv['sp35']), 2))

            if len(v['split_pids']) == 1:
                sp14 = vv['sp35']
            else:
                sp14+= vv['number']*float(vv['sp24'] or 0)

            if vv['sp17'] == '':
                continue

            vv['sp7'] = '是' if vv['sp17'].find('清火/防上火') != -1 else '否'
            vv['sp8'] = '是' if vv['sp17'].find('清新口气') != -1 else '否'
            vv['sp9'] = '是' if vv['sp17'].find('防止蛀牙') != -1 else '否'
            vv['sp10'] = '是' if vv['sp17'].find('保护牙龈') != -1 else '否'
            vv['sp11'] = '是' if vv['sp17'].find('美白牙齿') != -1 else '否'
            vv['sp13'] = '是' if vv['sp17'].find('加固牙齿') != -1 else '否'
            vv['sp15'] = '是' if vv['sp17'].find('抗敏感') != -1 else '否'
            vv['sp41'] = '是' if vv['sp17'].find('中草药') != -1 else '否'
            vv['sp42'] = '是' if vv['sp17'].find('多功能') != -1 else '否'

        for vv in v['split_pids']:
            vv['sp16'] = str(sp16) or ''
            vv['sp14'] = '{:g}'.format(round(float(sp14),1)) if sp14 else ''