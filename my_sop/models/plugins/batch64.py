import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import random

class main(Batch.main):

    def brush_3(self, smonth, emonth):
        sql = 'SELECT source, tb_item_id, real_p1 FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): True for v in ret}

        sales_by_uuid = {}
        item_id = [['tmall', 559914254509],
                   ['tmall', 559765648234]]

        # item_id = [['tmall', 615599996411],
        #            ['tb', 596688730904],
        #            ['jd', 41769718580],
        #            ['jd', 48423717745]]
        uuids2 = []
        for source, tb_item_id in item_id:
            ret, sales_by_uuid2 = self.cleaner.process_top(smonth, emonth, where='source = \'{}\' and tb_item_id = \'{}\''.format(source, tb_item_id))
            sales_by_uuid.update(sales_by_uuid2)
            for uuid2, source, tb_item_id, p1, in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    continue
                uuids2.append(uuid2)

        clean_flag = self.cleaner.last_clean_flag() + 1
        # self.cleaner.add_brush(uuids2, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        print('add brush item_id: {}'.format(len(set(uuids2))))
        return True

    def brush_2(self, smonth, emonth):
        sql = 'SELECT source, tb_item_id, real_p1 FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): True for v in ret}
        uuids1 = []
        ret, sales_by_uuid1 = self.cleaner.process_top(smonth, emonth, limit=800)
        for uuid2, source, tb_item_id, p1, in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            uuids1.append(uuid2)

        sales_by_uuid = {}
        sales_by_uuid.update(sales_by_uuid1)

        item_id = [['tmall', 572946812738],
                   ['tmall', 612637063487],
                   ['tmall', 559913830900],
                   ['tmall', 559765648234],
                   ['jd', 35683897222],
                   ['jd', 10222852902],
                   ['jd', 35898116020]]
        uuids2 = []
        for source, tb_item_id in item_id:
            ret, sales_by_uuid2 = self.cleaner.process_top(smonth, emonth, where='source = \'{}\' and tb_item_id = \'{}\''.format(source, tb_item_id))
            sales_by_uuid.update(sales_by_uuid2)
            for uuid2, source, tb_item_id, p1, in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    continue
                uuids2.append(uuid2)

        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids1 + uuids2, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        print('add brush top80: {}'.format(len(set(uuids1))))
        print('add brush item_id: {}'.format(len(set(uuids2))))
        return True

    def brush(self, smonth, emonth):
        sql = 'SELECT source, tb_item_id, real_p1 FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): True for v in ret}

        sales_by_uuid = {}
        # item_id = [['tmall', 559914254509],
                   # ['tmall', 559765648234]]

        # item_id = [['tmall', 615599996411],
        #            ['tb', 596688730904],
        #            ['jd', 41769718580],
        #            ['jd', 48423717745]]

        item_id = self.get_item_id()

        uuids2 = []
        for tb_item_id, source in item_id:
            ret, sales_by_uuid2 = self.cleaner.process_top(smonth, emonth, where='source = \'{}\' and tb_item_id = \'{}\''.format(source, tb_item_id))
            sales_by_uuid.update(sales_by_uuid2)
            for uuid2, source, tb_item_id, p1, in ret:
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    continue
                uuids2.append(uuid2)

        clean_flag = self.cleaner.last_clean_flag() + 1
        # self.cleaner.add_brush(uuids2, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        print('add brush item_id: {}'.format(len(set(uuids2))))
        return True

    def get_item_id(self):
        a = [
            ['29659190035','jd'],
            ['63033287429','jd'],
            ['63032186797','jd'],
            ['63033918633','jd'],
            ['63627090158','jd'],
            ['63627090159','jd'],
            ['57413896166','jd'],
            ['63557091832','jd'],
            ['63557091831','jd'],
            ['100005238093','jd'],
            ['100009280518','jd'],
            ['100006450565','jd'],
            ['100011698912','jd'],
            ['100011820314','jd'],
            ['6133637','kaola'],
            ['5756787','kaola'],
            ['5636790','kaola'],
            ['5669717','kaola'],
            ['6232478','kaola'],
            ['6025891','kaola'],
            ['5611302','kaola'],
            ['5744895','kaola'],
            ['5612341','kaola'],
            ['6034587','kaola'],
            ['6086756','kaola'],
            ['596451574189','tb'],
            ['618221463882','tb'],
            ['557815260054','tb'],
            ['569174044214','tb'],
            ['570708092303','tb'],
            ['570936522929','tb'],
            ['557816408974','tb'],
            ['586965986797','tb'],
            ['576643657048','tb'],
            ['581908691107','tb'],
            ['584005098547','tb'],
            ['591312542639','tb'],
            ['585044679275','tb'],
            ['597660226916','tb'],
            ['599183895250','tb'],
            ['602915797231','tb'],
            ['606732895403','tb'],
            ['607188604710','tb'],
            ['607492129567','tb'],
            ['608853009091','tb'],
            ['610470195782','tb'],
            ['610597393024','tb'],
            ['612765122867','tb'],
            ['613115568601','tb'],
            ['614385577015','tb'],
            ['562828700630','tb'],
            ['606851031953','tb'],
            ['595559235209','tb'],
            ['602865345395','tb'],
            ['607686883884','tb'],
            ['595209292700','tb'],
            ['586741083925','tb'],
            ['597353864911','tb'],
            ['619663046675','tb'],
            ['617856495835','tb'],
            ['599611097728','tb'],
            ['606814543681','tb'],
            ['595367717136','tb'],
            ['604713723432','tb'],
            ['601076534869','tb'],
            ['601988098108','tb'],
            ['610353290801','tb'],
            ['600979736805','tb'],
            ['590289587756','tb'],
            ['581431951534','tb'],
            ['618458382144','tb'],
            ['596090580596','tb'],
            ['614004971223','tb'],
            ['578495141969','tb'],
            ['583668447734','tb'],
            ['599054911853','tb'],
            ['614336837773','tb'],
            ['598001535987','tb'],
            ['606966444730','tb'],
            ['618514148870','tb'],
            ['618207129503','tb'],
            ['612495452135','tb'],
            ['594706199672','tb'],
            ['608942984274','tb'],
            ['597109003018','tb'],
            ['595437665089','tb'],
            ['616512449661','tmall'],
            ['602583050492','tmall'],
            ['602661970100','tmall'],
            ['610835359714','tmall'],
            ['610835639868','tmall'],
            ['611728295335','tmall'],
            ['610981056370','tmall'],
            ['584138000552','tmall'],
            ['619612222830','tmall'],
            ['619739897363','tmall']]
        return a