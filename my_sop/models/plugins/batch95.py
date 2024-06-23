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
        #m默認規則
        sql = 'SELECT source, tb_item_id, real_p1 FROM {} WHERE flag != 0'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): True for v in ret}

        sql = 'SELECT uniq_k FROM artificial.entity_{}_clean WHERE month >= \'{}\' and month < \'{}\' and sp1 = \'饼干\' and source = \'jd\''.format(self.cleaner.eid, smonth, emonth)
        uniq_k = [str(v[0]) for v in self.cleaner.dbch.query_all(sql)]
        uniq_k = ','.join(uniq_k)

        uuids = []
        ret1, sales_by_uuid = self.cleaner.process_top(smonth, emonth, limit=300, where='uniq_k in ({})'.format(uniq_k))
        for uuid, source, tb_item_id, p1, in ret1:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True
            uuids.append(uuid)

        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 2, sales_by_uuid=sales_by_uuid)
        print('add brush {}'.format(len(set(uuids))))
        return True

    def brush_2(self, smonth, emonth):

        clean_flag = self.cleaner.last_clean_flag() + 1
        # clean_flag = self.cleaner.last_clean_flag()
        ret1, sales_by_uuid = self.cleaner.process_top(smonth, emonth, limit=2000)
        ret2 = self.cleaner.process_rand(smonth, emonth, limit=1000)

        ret1_uuid = [v[0] for v in ret1]
        ret2_uuid = [v[0] for v in ret2]

        # sql = 'select uuid from {}_parts where uuid in ({}) and ' \
        #       'sp1 = \'其他\' '.format(self.cleaner.get_plugin().get_entity_tbl(), ','.join(['\''+str(t)+'\'' for t in ret1_uuid]))

        sql = 'select distinct(b.uuid) from entity_{}_clean a ' \
              'join entity_{}_origin_parts b on b.uniq_k = a.uniq_k ' \
              'where b.uuid in ({}) and a.sp1 = \'其它\''.format(self.cleaner.eid, self.cleaner.eid, ','.join(['\''+str(t)+'\'' for t in ret1_uuid]))

        uuids_top_others = [v[0] for v in self.cleaner.dbch.query_all(sql)]
        uuids_top_others = list(set(uuids_top_others))

        try:
            uuids_top_others_200 = random.sample(uuids_top_others, 200)
        except:
            uuids_top_others_200 = uuids_top_others
        # uuids_top_1800 = random.sample(list(set(ret1_uuid)-set(uuids_top_others)), 2000 - len(uuids_top_others_200))
        uuids_top_1800 = list(set(ret1_uuid)-set(uuids_top_others))

        sql = 'select distinct(b.uuid) from entity_{}_clean a ' \
              'join entity_{}_origin_parts b on b.uniq_k = a.uniq_k ' \
              'where b.uuid in ({}) and a.sp1 = \'其它\''.format(self.cleaner.eid, self.cleaner.eid, ','.join(['\'' + str(t) + '\'' for t in ret2_uuid]))

        uuids_rand_others = [v[0] for v in self.cleaner.dbch.query_all(sql)]
        uuids_rand_others = list(set(uuids_rand_others))

        try:
            uuids_rand_others_200 = random.sample(uuids_rand_others, 200)
        except:
            uuids_rand_others_200 = uuids_rand_others
        uuids_rand_800 = list(set(ret2_uuid)-set(uuids_rand_others))

        # self.cleaner.add_brush(ret2_uuid, clean_flag, 2)
        # self.cleaner.add_brush(uuids_rand_800 + uuids_rand_others_200, clean_flag, 2)
        # self.cleaner.add_brush(uuids_top_1800 + uuids_top_others_200, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        print('')
        return True

    def brush_bak0119(self, smonth, emonth):
        sql = 'SELECT source, tb_item_id, real_p1 FROM {} WHERE flag != 0'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): True for v in ret}

        uuids = []
        ret1, sales_by_uuid = self.cleaner.process_top(smonth, emonth, where='tb_item_id = \'71706598400\'')
        for uuid, source, tb_item_id, p1, in ret1:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = True
            uuids.append(uuid)

        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, sales_by_uuid=sales_by_uuid)
        print('add brush {}'.format(len(set(uuids))))
        return True

    def brush(self, smonth, emonth):
        uuids = []
        ret, sales_by_uuid = self.cleaner.process_top_anew(smonth, emonth, limit=100)
        for uuid, source, tb_item_id, p1, cid, alias in ret:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids.append(uuid)
        uuids2 = []
        ret2 = self.cleaner.process_rand_anew(smonth, emonth, orderBy='rand()', limit=100)
        for uuid, source, tb_item_id, p1, cid, alias in ret2:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids2.append(uuid)
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        self.cleaner.add_brush(uuids2, clean_flag, 2)
        return True

    def finish(self, tbl, dba, prefix):
        # 删除602418051419宝贝
        sql = '''
            ALTER TABLE {} UPDATE `c_props.value` = arrayMap((k, v) -> IF(k='是否无效链接', '无效链接', v), c_props.name, c_props.value)
            WHERE item_id = '602418051419'
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)
