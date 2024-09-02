import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import random

class main(Batch.main):
    def brush_default(self, smonth, emonth):

        sql = 'SELECT distinct(source) FROM {}_parts '.format(self.get_entity_tbl())
        source = [v[0] for v in self.cleaner.dbch.query_all(sql)]
        uuids1 = []
        for each_source in source:
            ret, sales_by_uuid = self.cleaner.process_top(smonth, emonth, limit=150, where='source = \'{}\''.format(each_source))
            uuids1 = uuids1 + [v[0] for v in ret]

        uuids2 = []
        sql = 'SELECT source, cid FROM {}_parts GROUP BY source, cid'.format(self.get_entity_tbl())
        cids = self.cleaner.dbch.query_all(sql)
        for source, cid, in cids:
            where = 'source=\'{}\' and cid={} and uniq_k in (SELECT uniq_k FROM artificial.entity_{}_clean WHERE month >= \'{}\' and month < \'{}\' and sp6 = \'\')'.format(source, cid, self.cleaner.eid,smonth, emonth)
            ret, sales_by_uuid = self.cleaner.process_top(smonth, emonth, rate=0.8, where=where)
            for v in ret:
                if v[0] not in uuids1:
                    uuids2.append(v[0])


        clean_flag = self.cleaner.last_clean_flag() + 1
        print('add brush 450: {}'.format(len(set(uuids1))))
        print('add brush 80% by cid,source: {}'.format(len(set(uuids2))))
        self.cleaner.add_brush(uuids1, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        self.cleaner.add_brush(uuids2, clean_flag, 2, sales_by_uuid=sales_by_uuid)
        print('')
        return True

    def brush(self, smonth, emonth):

        # where1 = 'uuid2 in (select uuid2 from {} where sp1 in (\'纸尿裤\',\'尿片\'))'.format(self.get_clean_tbl())
        # where2 = 'uuid2 in (select uuid2 from {} where sp1 in (\'纸尿裤\',\'尿片\')) and source=\'tb\''.format(self.get_clean_tbl())
        # where3 = 'uuid2 in (select uuid2 from {} where sp1 in (\'纸尿裤\',\'尿片\')) and source=\'tmall\''.format(self.get_clean_tbl())
        # where4 = 'uuid2 in (select uuid2 from {} where sp1 in (\'纸尿裤\',\'尿片\')) and source=\'jd\''.format(self.get_clean_tbl())
        where1 = 'uuid2 in (select uuid2 from {} where sp1 in (\'护理垫\',\'尿片\'))'.format(self.get_clean_tbl())
        where2 = 'uuid2 in (select uuid2 from {} where sp1 in (\'纸尿裤\',\'尿片\')) and source=\'tb\''.format(self.get_clean_tbl())
        where3 = 'uuid2 in (select uuid2 from {} where sp1 in (\'纸尿裤\',\'尿片\')) and source=\'tmall\''.format(self.get_clean_tbl())
        where4 = 'uuid2 in (select uuid2 from {} where sp1 in (\'纸尿裤\',\'尿片\')) and source=\'jd\''.format(self.get_clean_tbl())
        where = [where1,where2,where3,where4]
        print(where)
        # exit()
        uuids = []
        for each_where in where:
            each_uuids = []
            ret, sales_by_uuid = self.cleaner.process_top(smonth, emonth, rate=0.8, limit=99999999,where=each_where)
            for uuid2, source, tb_item_id, p1, alias_all_bid in ret:
                each_uuids.append(uuid2)
            uuids.append(len(each_uuids))
        print('top80', uuids)
        return True

    def brush_2(self, smonth, emonth):

        clean_flag = self.cleaner.last_clean_flag() + 1
        # clean_flag = self.cleaner.last_clean_flag()
        ret1,sales_by_uuid = self.cleaner.process_top(smonth, emonth, limit=2000)
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

        # self.cleaner.add_brush(ret2_uuid, clean_flag, 2)
        self.cleaner.add_brush(uuids_rand_800 + uuids_rand_others_200, clean_flag, 2)
        self.cleaner.add_brush(uuids_top_1800 + uuids_top_others_200, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        print('')
        return True


    def finish(self, tbl, dba, prefix):
        sql = '''
            ALTER TABLE {} UPDATE `c_props.value` = arrayMap((k, v) ->
                IF( k='总片数' OR k='出数用-总片数',
                    IF(v='','0',REPLACE(v,'片','')),
                    v),
                c_props.name, c_props.value)
            WHERE 1
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)


    def brush_modify(self, v, bru_items):
        for vv in v['split_pids']:
            vv['sp9'] = vv['sp8']
