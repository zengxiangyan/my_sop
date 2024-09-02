import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import random

class main(Batch.main):

    def brush_2(self, smonth, emonth):

        clean_flag = self.cleaner.last_clean_flag() + 1
        # clean_flag = self.cleaner.last_clean_flag()
        ret1, sales_by_uuid = self.cleaner.process_top(smonth, emonth, limit=2000)
        ret2 = self.cleaner.process_rand(smonth, emonth, limit=1000)

        ret1_uuid = [v[0] for v in ret1]
        ret2_uuid = [v[0] for v in ret2]

        # sql = 'select uuid2 from {}_parts where uuid2 in ({}) and ' \
        #       'sp1 = \'其他\' '.format(self.cleaner.get_plugin().get_entity_tbl(), ','.join(['\''+str(t)+'\'' for t in ret1_uuid]))

        sql = 'select distinct(b.uuid2) from entity_{}_clean a ' \
              'join entity_{}_origin_parts b on b.uniq_k = a.uniq_k ' \
              'where b.uuid2 in ({}) and a.sp1 = \'其它\''.format(self.cleaner.eid, self.cleaner.eid, ','.join(['\''+str(t)+'\'' for t in ret1_uuid]))

        uuids_top_others = [v[0] for v in self.cleaner.dbch.query_all(sql)]
        uuids_top_others = list(set(uuids_top_others))

        try:
            uuids_top_others_200 = random.sample(uuids_top_others, 200)
        except:
            uuids_top_others_200 = uuids_top_others
        # uuids_top_1800 = random.sample(list(set(ret1_uuid)-set(uuids_top_others)), 2000 - len(uuids_top_others_200))
        uuids_top_1800 = list(set(ret1_uuid)-set(uuids_top_others))

        sql = 'select distinct(b.uuid2) from entity_{}_clean a ' \
              'join entity_{}_origin_parts b on b.uniq_k = a.uniq_k ' \
              'where b.uuid2 in ({}) and a.sp1 = \'其它\''.format(self.cleaner.eid, self.cleaner.eid, ','.join(['\'' + str(t) + '\'' for t in ret2_uuid]))

        uuids_rand_others = [v[0] for v in self.cleaner.dbch.query_all(sql)]
        uuids_rand_others = list(set(uuids_rand_others))

        try:
            uuids_rand_others_200 = random.sample(uuids_rand_others, 200)
        except:
            uuids_rand_others_200 = uuids_rand_others
        uuids_rand_800 = list(set(ret2_uuid)-set(uuids_rand_others))

        self.cleaner.add_brush(uuids_rand_800 + uuids_rand_others_200, clean_flag, 2)
        self.cleaner.add_brush(uuids_top_1800 + uuids_top_others_200, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        print('')
        return True


    def brush(self, smonth, emonth):
        sql = 'select uuid2 from product_lib.entity_90924_item where flag > 0 and visible_check = 1'
        ret = self.cleaner.dbch.query_all(sql)
        uuids = [str(v[0]) for v in ret]
        uuid_list = ','.join(['\''+t+'\'' for t in uuids])
        ret1, sales_by_uuid = self.cleaner.process_top(smonth, emonth, where='uuid2 in ({})'.format(uuid_list))
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        print('')
        return True