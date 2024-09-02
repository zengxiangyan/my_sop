import sys
import time
import datetime
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')
import models.plugins.batch as Batch
import application as app


class main(Batch.main):

    def brush_0121A(self, smonth, emonth, logId=-1):
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()

        sales_by_uuid = {}
        cc = {}

        # sp1s = ['Handbag', 'Other Accessories', 'Shoes', 'SLG', 'RTW', 'Watch& Jewelry', 'Eyewear', '其它']
        sp1s = ['Cross-category gift set']
        for ssmonth, eemonth in [['2019-01-01', '2020-01-01'], ['2020-01-01', '2021-01-01'], ['2021-01-01', '2022-01-01']]:
            for each_sp1 in sp1s:
                uuids = []
                where = '''
                uuid2 in (select uuid2 from {ctbl} where pkey>='{ssmonth}' and pkey<'{eemonth}' and c_sp1='{each_sp1}')
                '''.format(ctbl=ctbl, ssmonth=ssmonth, eemonth=eemonth, each_sp1=each_sp1)
                ret, sales_by_uuid1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=500000)
                sales_by_uuid.update(sales_by_uuid1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        continue
                    else:
                        uuids.append(uuid2)
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                cc['{}+{}+{}'.format(ssmonth, eemonth, each_sp1)] = len(uuids)
                self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=1, sales_by_uuid=sales_by_uuid)

        for i in cc:
            print (i, cc[i])

        return True


    def brush_0120B(self, smonth, emonth, logId=-1):
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()

        sales_by_uuid = {}
        cc = {}

        sp1s = ['其它']
        for ssmonth, eemonth in [['2019-01-01', '2020-01-01'], ['2020-01-01', '2021-01-01'], ['2021-01-01', '2022-01-01']]:
            for each_sp1 in sp1s:
                uuids = []
                where = '''
                uuid2 in (select uuid2 from {ctbl} where pkey>='{ssmonth}' and pkey<'{eemonth}' and c_sp1='{each_sp1}') and (name like '%肩带%' or name like '%背包带%')
                '''.format(ctbl=ctbl, ssmonth=ssmonth, eemonth=eemonth, each_sp1=each_sp1)
                ret, sales_by_uuid1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=50000)
                sales_by_uuid.update(sales_by_uuid1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        continue
                    else:
                        uuids.append(uuid2)
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                cc['{}+{}+{}'.format(ssmonth, eemonth, each_sp1)] = len(uuids)
                self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=1, sales_by_uuid=sales_by_uuid)

        for i in cc:
            print (i, cc[i])

        return True

    def brush_0121B(self, smonth, emonth, logId=-1):
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()

        sales_by_uuid = {}
        cc = {}


        uuids = []
        where = '''
        item_id in('607441765603',
                    '623222615443',
                    '614431349314',
                    '610096568314',
                    '631927852192',
                    '620979030899',
                    '614109232215',
                    '631927664756',
                    '624165120130',
                    '651681999394',
                    '586623696476',
                    '585956436025',
                    '100005023363',
                    '598230307957')
        '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth)
        ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=10000)
        sales_by_uuid.update(sales_by_uuid1)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            else:
                uuids.append(uuid2)
                mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
        # cc['{}+{}+{}'.format(smonth, emonth, each_sp1)] = len(uuids)
        self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=1, sales_by_uuid=sales_by_uuid)

        for i in cc:
            print (i, cc[i])

        return True


    def brush(self, smonth, emonth, logId=-1):
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()

        sales_by_uuid = {}
        cc = {}




        uuid_ttl = ['8b9e5d98-f6df-4fb5-9300-719283d7c568',
                    'd3674c79-73bd-4291-80d7-0b43ef2e03df',
                    '4bb3a192-e5c0-43ca-a756-d7d38cb291e3',
                    '6dcbf6a4-60a0-449c-b7aa-3aa950348b4c',
                    '2c5e94c3-bcd6-41f2-a1e7-ae8925d132e0',
                    '499c2a80-b576-446a-a548-3104425917f9',
                    'abaf7871-423a-4198-ac09-ee1d13e1a09e']
        for each_uuid in uuid_ttl:
            uuids = []
            where = '''
            uuid2 = '{each_uuid}'
            '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_uuid=each_uuid)
            ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=10000)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                uuids.append(uuid2)
            # cc['{}+{}+{}'.format(smonth, emonth, each_sp1)] = len(uuids)
            self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=1, sales_by_uuid=sales_by_uuid)



        return True