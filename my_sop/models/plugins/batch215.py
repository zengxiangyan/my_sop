import sys
import time
import datetime
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')
import models.plugins.batch as Batch
import application as app


class main(Batch.main):
    def brush_0119(self, smonth, emonth, logId=-1):
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4], v[5]] for v in ret}

        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()

        sales_by_uuid = {}
        cc = {}

        sources = ['source = 1 and shop_type> 20', 'source = 2']

        ## V1
        sp1s = ['保健品', 'OTC药品', '食品']
        sources = ['source = 1 and shop_type> 20', 'source = 2']
        uuids1 = []
        for each_sp1 in sp1s:
            for each_source in sources:
                uuid_t = []
                where = '''
                uuid2 in (select uuid2 from {ctbl} where pkey>='{smonth}' and pkey<'{emonth}' and c_sp1='{each_sp1}') and {each_source}
                '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_sp1=each_sp1, each_source=each_source)
                ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=50)
                sales_by_uuid.update(sales_by_uuid1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        continue
                    else:
                        uuids1.append(uuid2)
                        uuid_t.append(uuid2)
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                cc['{}+{}+{}+{}+{}'.format(each_source, smonth, emonth, each_sp1, 'V1')] = [len(uuid_t)]
        self.cleaner.add_brush(uuids1, clean_flag, visible_check=1, visible=1, sales_by_uuid=sales_by_uuid)

        ## V2.1
        sources = ['source = 1 and shop_type> 20', 'source = 2']
        uuid21 = []
        alias_bids21 = [48219,387270,5471756,130547,5434241,4664995,475116,6719622,472799,326026,3107672]
        for each_source in sources:
            for each_bid in alias_bids21:
                uuid_t = []
                where = '''
                uuid2 in (select uuid2 from {ctbl} where pkey>='{smonth}' and pkey<'{emonth}' and c_sp1='保健品') and alias_all_bid = {each_bid} and {each_source}
                '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_bid=each_bid, each_source=each_source)
                ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=12)
                sales_by_uuid.update(sales_by_uuid1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        continue
                    else:
                        uuid21.append(uuid2)
                        uuid_t.append(uuid2)
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                cc['{}+{}+{}+{}+{}'.format(each_source, smonth, emonth, each_bid, 'V2_保健品')] = [len(uuid_t)]
        self.cleaner.add_brush(uuid21, clean_flag, visible_check=1, visible=2, sales_by_uuid=sales_by_uuid)

        ## V2.2
        sources = ['source = 1 and shop_type> 20', 'source = 2']
        uuid22 = []
        alias_bids22 = [89901,713972,3486334,6334966,140341,48219,327979,20439,472570,190439,3159305]
        for each_source in sources:
            for each_bid in alias_bids22:
                uuid_t = []
                where = '''
                uuid2 in (select uuid2 from {ctbl} where pkey>='{smonth}' and pkey<'{emonth}' and c_sp1='OTC药品') and alias_all_bid = {each_bid} and {each_source}
                '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_bid=each_bid, each_source=each_source)
                ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=12)
                sales_by_uuid.update(sales_by_uuid1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        continue
                    else:
                        uuid22.append(uuid2)
                        uuid_t.append(uuid2)
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                cc['{}+{}+{}+{}+{}'.format(each_source, smonth, emonth, each_bid, 'V2_OTC药品')] = [len(uuid_t)]
        self.cleaner.add_brush(uuid22, clean_flag, visible_check=1, visible=2, sales_by_uuid=sales_by_uuid)

        ## V2.3
        sources = ['source = 1 and shop_type> 20', 'source = 2']
        uuid23 = []
        alias_bids23 = [48200,48219,5780803,5973877,5176918,6635988,199996,48257,6021628,5717343,5967043,0]
        for each_source in sources:
            for each_bid in alias_bids23:
                uuid_t = []
                where = '''
                uuid2 in (select uuid2 from {ctbl} where pkey>='{smonth}' and pkey<'{emonth}' and c_sp1='食品') and alias_all_bid = {each_bid} and {each_source}
                '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_bid=each_bid, each_source=each_source)
                ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=12)
                sales_by_uuid.update(sales_by_uuid1)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                        continue
                    else:
                        uuid23.append(uuid2)
                        uuid_t.append(uuid2)
                        mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
                cc['{}+{}+{}+{}+{}'.format(each_source, smonth, emonth, each_bid, 'V2_食品')] = [len(uuid_t)]
        self.cleaner.add_brush(uuid23, clean_flag, visible_check=1, visible=2, sales_by_uuid=sales_by_uuid)

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

        sources = ['source = 1 and shop_type> 20', 'source = 2']

        for each_source in sources:

            uuids = []

            where = '''
            uuid2 in (select uuid2 from {ctbl} where c_sp9='蛋白粉' and c_sp15='其它') and {each_source}
            '''.format(ctbl=ctbl, each_source=each_source)
            ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=100)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    continue
                else:
                    uuids.append(uuid2)
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
            self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=3, sales_by_uuid=sales_by_uuid)

        return True




