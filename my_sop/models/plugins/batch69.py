import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import random

class main(Batch.main):

    def brush_bak(self, smonth, emonth):

        # clean_flag = self.cleaner.last_clean_flag() + 1
        clean_flag = self.cleaner.last_clean_flag()
        ret1, sales_by_uuid = self.cleaner.process_top(smonth, emonth, limit=5000)
        ret2 = self.cleaner.process_rand(smonth, emonth, limit=1000)

        ret1_uuid = [v[0] for v in ret1]
        ret2_uuid = [v[0] for v in ret2]

        # sql = 'select uuid2 from {}_parts where uuid2 in ({}) and ' \
        #       'sp1 = \'其他\' '.format(self.cleaner.get_plugin().get_entity_tbl(), ','.join(['\''+str(t)+'\'' for t in ret1_uuid]))

        sql = 'select distinct(b.uuid2) from entity_{}_clean a ' \
              'join entity_{}_origin_parts b on b.uniq_k = a.uniq_k ' \
              'where b.uuid2 in ({}) and a.sp1 = \'其它\''.format(self.cleaner.eid, self.cleaner.eid, ','.join(['\''+str(t)+'\'' for t in ret1_uuid]))

        uuids_others = [v[0] for v in self.cleaner.dbch.query_all(sql)]
        uuids_others = list(set(uuids_others))

        try:
            uuids_others_200 = random.sample(uuids_others, 200)
        except:
            uuids_others_200 = uuids_others
        uuids_1800 = random.sample(ret1_uuid, 2000-len(uuids_others_200))

        self.cleaner.add_brush(ret2_uuid, clean_flag, 2)
        self.cleaner.add_brush(uuids_1800 + uuids_others_200, clean_flag, 1, sales_by_uuid=sales_by_uuid)

    def brush_new(self, smonth, emonth):

        # clean_flag = self.cleaner.last_clean_flag() + 1
        clean_flag = self.cleaner.last_clean_flag()
        ret1, sales_by_uuid = self.cleaner.process_top(smonth, emonth, limit=5000)
        ret2 = self.cleaner.process_rand(smonth, emonth, limit=1000)

        ret1_uuid = [v[0] for v in ret1]
        ret2_uuid = [v[0] for v in ret2]

        sql = '''
        select uuid2 from {} where sp1 = '其它' and uuid2 in ({})
        '''.format(self.get_clean_tbl(), ','.join(['\'' + str(t) + '\'' for t in ret1_uuid]))
        uuids_others = [v[0] for v in self.cleaner.dbch.query_all(sql)]
        uuids_others = list(set(uuids_others))
        try:
            uuids_others_200 = random.sample(uuids_others, 200)
        except:
            uuids_others_200 = uuids_others
        uuids_1800 = random.sample(ret1_uuid, 2000 - len(uuids_others_200))

        self.cleaner.add_brush(ret2_uuid, clean_flag, 2)
        self.cleaner.add_brush(uuids_1800 + uuids_others_200, clean_flag, 1, sales_by_uuid=sales_by_uuid)

    def brush_0524(self, smonth, emonth):
        uuids = []
        ret, sales_by_uuid = self.cleaner.process_top_anew(smonth, emonth, limit=100)
        for uuid2, source, tb_item_id, p1, cid in ret:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids.append(uuid2)
        uuids2 = []
        ret2,sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, orderBy='rand()', limit=100)
        sales_by_uuid.update(sales_by_uuid1)
        for uuid2, source, tb_item_id, p1, cid in ret2:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids2.append(uuid2)
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        self.cleaner.add_brush(uuids2, clean_flag, 2)
        return True


    def brush_1011(self, smonth, emonth, logId=-1):
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()

        sales_by_uuid = {}
        cc = {}

        uuids = []

        where = '''
                        uuid2 in ('f60914d2-06e3-4012-a666-db964bc775b6',
'7c57813d-2cad-4901-81c7-63e1605bb60e',
'a3b10dcb-11ab-4927-90c6-6b714b35bb4d',
'536d6c9f-6296-4413-8450-6ab23017d4cb',
'de63ec59-c898-4f17-b5c1-d048a3e4946f',
'7b1ca8dc-0dcb-419d-8aec-81538297f8a5',
'a814be36-3185-491e-a1b5-82312a131287'
                                )
                        '''
        ret, sales_by_uuid1 = self.cleaner.process_top_anew_byuuid(smonth=smonth, emonth=emonth, where=where,
                                                                   limit=100000)
        sales_by_uuid.update(sales_by_uuid1)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            uuids.append(uuid2)
        self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=1, sales_by_uuid=sales_by_uuid)
        return True


    def brush_0620(self, smonth, emonth, logId=-1):
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()

        sales_by_uuid = {}
        cc = {}

        uuids_ttl = []

        sp14s = ['N3Pro',	'N6 II',	'N8',	'M11',	'M11Pro',	'M15',	'M3',	'M6',	'M7',	'M9',	'M17',	'X5',	'X5III',	'X7',	'X7 II',	'HM650',	'HM802S',	'HM901U',	'HM1000',	'AK70',	'AK70 64G',	'AK70 M2',	'KANN',	'SE100',	'SE200',	'SP1000',	'SP1000m',	'SP2000',	'SR15',	'SR25',	'SP2000T',	'Kann Alpha',	'Kann Cube',	'SE180',	'M2S',	'M2X',	'M3S',	'M5S',	'M6',	'M6 Pro',	'M8',	'M9',	'DMP-Z1',	'NW-A100TPS',	'NW-A105',	'NW-A105HN',	'NW-A106HN',	'NW-A35',	'NW-A35HN',	'NW-A36HN',	'NW-A37HN',	'NW-A45',	'NW-A45HN',	'NW-A46HN',	'NW-A55',	'NW-A55HN',	'NW-A56HN',	'NW-WM1A',	'NW-WM1Z',	'NW-WS623',	'NW-WS625',	'NWZ-A25',	'NWZ-A25HN',	'NWZ-A27',	'NWZ-B183',	'NWZ-WS413',	'NWZ-WS414',	'NWZ-WS615',	'NW-ZX100',	'NW-ZX2',	'NW-ZX300',	'NW-ZX300A',	'NW-ZX505',	'NW-ZX507',	'NWZ-ZX1',	'RS6',	'R8',	'R6 New',	'DX240',	'DX300',	'PAW 6000',	'PAW GOLD TOUCH',	'PAW-GOLD']

        for each_sp14 in sp14s:
            uuids = []
            where = '''
            uuid2 in (select uuid2 from {ctbl} where pkey>='{smonth}' and pkey<'{emonth}' and sp14 = '{each_sp14}' and sp1='mp3/mp4')
            '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_sp14=each_sp14)
            ret, sales_by_uuid1 = self.cleaner.process_top_anew_byuuid(smonth=smonth, emonth=emonth, where=where, limit=50)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    continue
                else:
                    uuids.append(uuid2)
                    uuids_ttl.append(uuid2)
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
            cc['{}~{}@{}'.format(smonth, emonth, each_sp14)] = [len(uuids)]

        self.cleaner.add_brush(uuids_ttl, clean_flag, visible_check=1, visible=1, sales_by_uuid=sales_by_uuid)
        # for i in cc:
        #     print(i, cc[i])

        return True

    def brush(self, smonth, emonth, logId=-1):
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()

        sales_by_uuid = {}
        cc = {}

        uuids_ttl = []

        sp14s = ['N3Pro', 'N6 II', 'N8', 'M11', 'M11Pro', 'M15', 'M3', 'M6', 'M7', 'M9', 'M17', 'X5', 'X5III', 'X7',
                 'X7 II', 'HM650', 'HM802S', 'HM901U', 'HM1000', 'AK70', 'AK70 64G', 'AK70 M2', 'KANN', 'SE100',
                 'SE200', 'SP1000', 'SP1000m', 'SP2000', 'SR15', 'SR25', 'SP2000T', 'Kann Alpha', 'Kann Cube', 'SE180',
                 'M2S', 'M2X', 'M3S', 'M5S', 'M6', 'M6 Pro', 'M8', 'M9', 'DMP-Z1', 'NW-A100TPS', 'NW-A105', 'NW-A105HN',
                 'NW-A106HN', 'NW-A35', 'NW-A35HN', 'NW-A36HN', 'NW-A37HN', 'NW-A45', 'NW-A45HN', 'NW-A46HN', 'NW-A55',
                 'NW-A55HN', 'NW-A56HN', 'NW-WM1A', 'NW-WM1Z', 'NW-WS623', 'NW-WS625', 'NWZ-A25', 'NWZ-A25HN',
                 'NWZ-A27', 'NWZ-B183', 'NWZ-WS413', 'NWZ-WS414', 'NWZ-WS615', 'NW-ZX100', 'NW-ZX2', 'NW-ZX300',
                 'NW-ZX300A', 'NW-ZX505', 'NW-ZX507', 'NWZ-ZX1', 'RS6', 'R8', 'R6 New', 'DX240', 'DX300', 'PAW 6000',
                 'PAW GOLD TOUCH', 'PAW-GOLD']

        for each_sp14 in sp14s:
            uuids = []
            where = '''
            uuid2 in (select uuid2 from {ctbl} where pkey>='{smonth}' and pkey<'{emonth}' and sp14 = '{each_sp14}' and sp1='mp3/mp4')
            '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, each_sp14=each_sp14)
            ret, sales_by_uuid1 = self.cleaner.process_top_anew_byuuid(smonth=smonth, emonth=emonth, where=where,
                                                                       limit=50)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    continue
                else:
                    uuids.append(uuid2)
                    uuids_ttl.append(uuid2)
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
            cc['{}~{}@{}'.format(smonth, emonth, each_sp14)] = [len(uuids)]

        self.cleaner.add_brush(uuids_ttl, clean_flag, visible_check=1, visible=1, sales_by_uuid=sales_by_uuid)
        # for i in cc:
        #     print(i, cc[i])

        return True