import sys
import time
import json
import datetime
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')
import models.plugins.batch as Batch
import application as app


class main(Batch.main):

    def brush_0117(self, smonth, emonth, logId=-1):
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()

        sales_by_uuid = {}
        cc = {}

        for each_sid in [170685533, 187028404]:
        # for each_sid in [64772083, 68677432, 66410465, 168161135]:

            uuids = []
            where = '''
            uuid2 in (select uuid2 from {ctbl} where c_sp1='润滑剂') and sid = {each_sid}
            '''.format(ctbl=ctbl, each_sid=each_sid)
            ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, rate=0.8)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    continue
                else:
                    uuids.append(uuid2)
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
            cc['{}+{}+{}'.format(smonth, emonth, each_sid)] = len(uuids)
            self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=1, sales_by_uuid=sales_by_uuid)
        for i in cc:
            print (i, cc[i])

        return True


    def brush_0207(self, smonth, emonth, logId=-1):
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()

        sales_by_uuid = {}
        cc = {}

        lists = [['吮吸玩具', 200], ['震动环', 200], ['无吮吸跳蛋', 200]]
        for ssmonth, eemonth in [['2020-01-01', '2022-01-01']]:
            for each_sp13, each_limit in lists:
                uuids = []
                where = '''
                uuid2 in (select uuid2 from {ctbl} where c_sp1='成人玩具' and c_sp13 = '{each_sp13}')
                '''.format(ctbl=ctbl, each_sp13=each_sp13)
                ret, sales_by_uuid1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=each_limit)
                sales_by_uuid.update(sales_by_uuid1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        continue
                    else:
                        uuids.append(uuid2)
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                cc['{}+{}+{}+{}'.format(ssmonth, eemonth, each_sp13,each_limit)] = len(uuids)
                self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=2, sales_by_uuid=sales_by_uuid)
        for i in cc:
            print (i, cc[i])

        return True

    def brush_0208(self, smonth, emonth, logId=-1):
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()

        sales_by_uuid = {}
        cc = {}


        for ssmonth, eemonth in [['2020-01-01', '2022-01-01']]:

            uuids = []
            where = '''
            uuid2 in (select uuid2 from {ctbl} where c_sp1='成人玩具' and c_sp13 = '阴蒂刺激')
            '''.format(ctbl=ctbl)
            ret, sales_by_uuid1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=400)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    continue
                else:
                    uuids.append(uuid2)
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
            # cc['{}+{}+{}+{}'.format(ssmonth, eemonth, each_sp13,each_limit)] = len(uuids)
            self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=2, sales_by_uuid=sales_by_uuid)
        for i in cc:
            print (i, cc[i])

        return True

    def brush_0209(self, smonth, emonth, logId=-1):
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()

        sales_by_uuid = {}
        cc = {}




        uuid_ttl = ['a7178b91-31b9-4ef3-89f2-7792d8638416',
                    'f52b0a02-d453-4d50-9223-cca0bbe5bc67',
                    ]
        for each_uuid in uuid_ttl:
            uuids = []
            where = '''
            uuid2 = '{each_uuid}'
            '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_uuid=each_uuid)
            ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=10)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                uuids.append(uuid2)
            # cc['{}+{}+{}'.format(smonth, emonth, each_sp1)] = len(uuids)
            self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=5, sales_by_uuid=sales_by_uuid)



        return True

    def brush(self, smonth, emonth, logId=-1):
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {} where visible>0'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()

        sales_by_uuid = {}
        cc = {}




        # uuids = []
        # where = '''
        # uuid2 in (select uuid2 from {ctbl} where c_sp1='安全套' )
        # '''.format(ctbl=ctbl)
        # ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, rate=0.5)
        # sales_by_uuid.update(sales_by_uuid1)
        # for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
        #     if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
        #         continue
        #     else:
        #         uuids.append(uuid2)
        #         mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
        # self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=104, sales_by_uuid=sales_by_uuid)

        uuids = []
        where = '''
                uuid2 in (select uuid2 from {ctbl} where c_sp1='润滑剂' )
                '''.format(ctbl=ctbl)
        ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, rate=0.62)
        sales_by_uuid.update(sales_by_uuid1)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            else:
                uuids.append(uuid2)
                mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
        self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=5, sales_by_uuid=sales_by_uuid)
        print(len(uuids))


        return True


    def calc_splitrate(self, item, data, custom_price={}):
        if len(data) <= 1:
            return data

        source = str(0 if item['snum']==1 and item['pkey'].day<20 else item['snum'])

        if 'sku_price'+source not in self.cache:
            dba, atbl = self.get_all_tbl()
            dba = self.cleaner.get_db(dba)
            db26 = self.cleaner.get_db('26_apollo')
            tbl = self.cleaner.get_product_tbl()

            sql = 'SELECT pid FROM {} WHERE name LIKE \'\\\\_\\\\_\\\\_%\' OR alias_all_bid = 0'.format(tbl)
            ret = db26.query_all(sql)
            ret = list(ret) + [[0]]

            pids = {}

            sql = '''
                SELECT pid, AVG(sales/num/ifNull(toUInt32OrNull(condom_box),1))
                FROM sop_c.entity_prod_{eid}_C_brush WHERE pid > 0 AND num > 0 GROUP BY pid
            '''.format(eid=self.cleaner.eid)
            rrr = dba.query_all(sql)
            pids.update({v[0]: v[1] for v in rrr})

            sql = '''
                WITH IF(`source`=1 AND toDayOfMonth(pkey)<20,0,`source`) AS s
                SELECT b_pid, median(sales/num/`b_number`) FROM {}
                WHERE b_split_rate = 1 AND b_id > 0 AND b_pid NOT IN ({})
                GROUP BY b_pid
            '''.format(atbl, ','.join([str(v) for v, in ret]))
            rrr = dba.execute(sql)
            pids.update({v[0]: v[1] for v in rrr})

            # 防止历史数据缺失先不限时间
            sql = '''
                WITH IF(`source`=1 AND toDayOfMonth(pkey)<20,0,`source`) AS s
                SELECT b_pid, median(sales/num/`b_number`) FROM {}
                WHERE b_split_rate = 1 AND b_id > 0 AND b_pid NOT IN ({}) AND s = {}
                GROUP BY b_pid
            '''.format(atbl, ','.join([str(v) for v, in ret]), source)
            rrr = dba.execute(sql)
            pids.update({v[0]: v[1] for v in rrr})

            sql = '''
                WITH IF(`source`=1 AND toDayOfMonth(pkey)<20,0,`source`) AS s
                SELECT b_pid, median(sales/num/`b_number`) FROM {}
                WHERE b_split_rate = 1 AND b_similarity = 0 AND b_id > 0 AND b_pid NOT IN ({})
                AND date >= toStartOfMonth(date_sub(MONTH, 6, toDate(NOW()))) AND s = {}
                GROUP BY b_pid
            '''.format(atbl, ','.join([str(v) for v, in ret]), source)
            rrr = dba.execute(sql)
            pids.update({v[0]: v[1] for v in rrr})

            self.cache['sku_price'+source] = pids

        pids = {v['pid']: self.cache['sku_price'+source][v['pid']] for v in data if v['pid'] in self.cache['sku_price'+source]}
        pids.update({v['pid']: custom_price[v['pid']] for v in data if v['pid'] in custom_price})

        item['price'] = item['sales'] / max(item['num'],1)
        item['price'] = max(item['price'],1)

        total = sum([pids[v['pid']]*v['number'] for v in data if v['pid'] in pids])
        if total < item['price']*0.8 and len(pids.keys()) == len(data) and len(data) > 0:
            d = {k:'' for k in data[0].keys()}
            d.update({'pid': 7301, 'all_bid': item['alias_all_bid'], 'alias_all_bid': item['alias_all_bid'], 'sku_name': '出数专用-价格溢出分配', 'number': 1, 'split': 0, 'split_rate': 1, 'split_flag': 0, 'sp1': '', 'sp2': '', 'sp3': '', 'sp4': '', 'sp5': '出数专用-价格溢出分配', 'sp6': '', 'sp7': '', 'sp8': '', 'sp9': '1', 'sp10': '', 'sp11': '', 'sp12': '', 'sp13': '', 'sp14': '', 'sp15': '', 'sp16': '', 'sp17': '', 'sp18': '', 'sp19': '', 'sp20': '', 'sp21': '', 'sp22': '', 'sp23': '', 'sp24': ''})
            data.append(d)

        if total >= item['price']:
            rate, vv = 1, None
            for v in data:
                if v['pid'] in pids:
                    r = pids[v['pid']]*v['number']/total
                    r = round(r,3)
                    rate -= r
                    v['split_rate'] = r
                    vv = v
                else:
                    v['split_rate'] = 0
                v['split_flag'] = 2
            if rate != 0:
                vv['split_rate']+= round(rate, 3)
        elif len(pids.keys()) == len(data):
            # 总价值小于商品价值 肯定有问题
            rate, vv = 1, None
            for v in data:
                r = pids[v['pid']]*v['number']/total
                r = round(r,3)
                rate -= r
                v['split_rate'] = r
                vv = v
                v['split_flag'] = 0
            if rate != 0:
                vv['split_rate']+= round(rate, 3)
        else:
            rate, less_num, vv = 1, 0, None
            for v in data:
                if v['pid'] in pids:
                    r = pids[v['pid']]*v['number']/item['price']
                else:
                    less_num += v['number']
                    continue
                r = round(r,3)
                rate -= r
                v['split_rate'] = r
                v['split_flag'] = 1
                vv = v

            less_rate = rate
            for v in data:
                if v['pid'] in pids:
                    continue
                r = v['number']/less_num * rate
                r = round(r,3)
                less_rate -= r
                v['split_rate'] = r
                v['split_flag'] = 1
                vv = v

            if less_rate != 0:
                vv['split_rate']+= round(less_rate, 3)

        return data