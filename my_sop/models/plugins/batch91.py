import sys
import time
import datetime
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import random

class main(Batch.main):
    def brush_default(self,smonth,emonth):
        # 默认规则
        sql = 'SELECT source, tb_item_id, real_p1 FROM {} where flag > 0'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): True for v in ret}
        # mpp = {}
        uuids = []
        ret, sales_by_uuid = self.cleaner.process_top(smonth, emonth, where='(source=\'jd\' AND tb_item_id =\'100007125623\')')
        for uuid, source, tb_item_id, p1, in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            uuids.append(uuid)
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        print('add brush item_id: {}'.format(len(set(uuids))))

    def brush(self, smonth, emonth, logId=-1):
        cname,ctbl = self.get_c_tbl()
        db = self.cleaner.get_db(cname)
        sql = 'select distinct(sp2) from {}'.format(ctbl)
        sp2s = [v[0] for v in db.query_all(sql)]
        sales_by_uuid = {}
        uuids = []
        for sp2 in sp2s:
            where = 'uuid in (select uuid from {} where sp2 = \'{}\')'.format(ctbl, sp2)
            ret, sales_by_uuid1 = self.cleaner.process_top_anew(smonth, emonth, where=where, limit=100)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1):
                    continue
                uuids.append(uuid)
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, visible=2, sales_by_uuid=sales_by_uuid)
        return True

    def brush_3(self, smonth, emonth, logId=-1):
        tb_item_ids = [100004265218, 100002726335]
        tb_item_ids = ['\'{}\''.format(t) for t in tb_item_ids]
        tb_item_ids = ','.join(tb_item_ids)
        where = 'source = \'jd\' and tb_item_id in ({})'.format(tb_item_ids)
        uuids = []
        ret, sales_by_uuid = self.cleaner.process_top(smonth, emonth, where=where)
        for uuid, source, tb_item_id, p1, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids.append(uuid)
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        print('add brush item_id: {}'.format(len(set(uuids))))

    def brush_2(self, smonth, emonth):

        # if self.cleaner.check(smonth, emonth) == False:
        #     return True
        # jd 2678  这个cid不出题

        clean_flag = self.cleaner.last_clean_flag() + 1

        ret1,sales_by_uuid = self.cleaner.process_top(smonth, emonth, limit=5000, where='(source=\'jd\' AND cid=15054)')
        ret2 = self.cleaner.process_rand(smonth, emonth, limit=1000, where='(source=\'jd\' AND cid=15054)')

        ret1_uuid = [v[0] for v in ret1]
        ret2_uuid = [v[0] for v in ret2]

        # sql = 'select uuid from {}_parts where uuid in ({}) and ' \
        #       'sp1 = \'其他\' '.format(self.cleaner.get_plugin().get_entity_tbl(), ','.join(['\''+str(t)+'\'' for t in ret1_uuid]))

        sql = 'select distinct(b.uuid) from entity_90775_clean a ' \
              'join entity_90775_origin_parts b on b.uniq_k = a.uniq_k ' \
              'where b.uuid in ({}) and a.sp1 = \'其它\''.format(','.join(['\''+str(t)+'\'' for t in ret1_uuid]))

        uuids_others = [v[0] for v in self.cleaner.dbch.query_all(sql)]
        uuids_others = list(set(uuids_others))

        try:
            uuids_others_200 = random.sample(uuids_others, 200)
        except:
            uuids_others_200 = uuids_others
        uuids_800 = random.sample(ret1_uuid, 1000-len(uuids_others_200))

        self.cleaner.add_brush(ret2_uuid, clean_flag, 2)
        self.cleaner.add_brush(uuids_800 + uuids_others_200, clean_flag, 1, sales_by_uuid=sales_by_uuid)

        # tb_item_ids = [
        #     2374274,3965352,100000086871,2374274,100010087272,7649025,5056147,3965352
        # ]
        # tb_item_ids = ['\'{}\''.format(v) for v in tb_item_ids]
        #
        # sql = 'DELETE FROM {} WHERE tb_item_id IN ({})'.format(self.cleaner.get_tbl(), ','.join(tb_item_ids))
        # self.cleaner.db26.execute(sql)
        #
        # sql = 'SELECT argMax(uuid, month) FROM {}_parts WHERE tb_item_id IN ({}) AND sign > 0 GROUP BY source, tb_item_id, p1'.format(
        #     self.get_entity_tbl(), ','.join(tb_item_ids)
        # )
        # uuids = self.cleaner.dbch.query_all(sql)
        # uuids = [v[0] for v in uuids]
        #
        # clean_flag = self.cleaner.last_clean_flag() + 1
        # self.cleaner.add_brush(uuids, clean_flag, 1)
        #
        # print('add new brush {}'.format(len(uuids)))
        # return True
        #
        # if self.cleaner.check(smonth, emonth) == False:
        #     return True
        # # jd 2678  这个cid不出题
        #
        # clean_flag = self.cleaner.last_clean_flag() + 1
        #
        # ret1 = self.cleaner.process_top(smonth, emonth, limit=5000, where='not (source=\'jd\' AND cid=2678)')
        # ret2 = self.cleaner.process_rand(smonth, emonth, limit=1000, where='not (source=\'jd\' AND cid=2678)')
        # c = 0
        # total = 0
        # uuids1 = []
        # uuids2 = []
        # for uuid, source, tb_item_id, p1, in ret1:
        #     if total < 2000:
        #         sql = 'SELECT clean_type FROM artificial.entity_{}_item WHERE ' \
        #               'source=\'{}\' and tb_item_id={} and p1={} '.format(self.cleaner.eid, source, tb_item_id, p1)
        #         rrr = self.cleaner.db26.query_all(sql)
        #         clean_type = rrr[0][0] if len(rrr) > 0 and rrr[0][0] is not None else -1
        #         if clean_type == -1:
        #             c += 1
        #         if c < 200 and clean_type == -1:
        #             uuids1.append(uuid)
        #             total += 1
        #         if clean_type != -1 and total < 2000:
        #             uuids2.append(uuid)
        #             total += 1
        #
        # self.cleaner.add_brush([v[0] for v in ret2], clean_flag, 2)
        # self.cleaner.add_brush(uuids1 + uuids2, clean_flag, 1)


        #
        # ret1_uuid = [str(i[0]) for i in ret1]
        #
        # # sql = 'select uuid from {}_parts where uuid in ({}) and ' \
        # #       'sp1 = \'其他\' '.format(self.cleaner.get_plugin().get_entity_tbl(), ','.join(ret1_uuid))
        #
        # # sql = 'select uuid from {}_parts where uuid in ({}) and ' \
        # #       'sp1 = \'其他\' '.format(self.cleaner.get_plugin().get_entity_tbl(), ','.join(ret1_uuid))
        #
        # sql = 'select distinct(b.uuid) from entity_90775_clean a ' \
        #       'join entity_90775_origin_parts b on b.uniq_k = a.uniq_k ' \
        #       'where b.uuid in ({}) and a.sp1 = \'其它\''.format(','.join(ret1_uuid))
        #
        # ret1_uuid_others = [list(row)[0] for row in self.cleaner.dbch.query_all(sql)]
        # try:
        #     ret1_uuid_others_200 = random.sample(ret1_uuid_others, 200)
        # except:
        #     ret1_uuid_others_200 = ret1_uuid_others
        #
        # ret1_uuid_not_others_1800 = random.sample(list(set(ret1_uuid) - set(ret1_uuid_others)), 2000-len(ret1_uuid_others_200))
        #
        # ret_uuid_sample = ret1_uuid_others_200 + ret1_uuid_not_others_1800
        #
        # ret1a = []
        # for row in ret1:
        #     if row[0] in ret_uuid_sample:
        #         ret1a.append(row)
        #
        # self.cleaner.add_brush([v[0] for v in ret1a], clean_flag, 1)
        # self.cleaner.add_brush([v[0] for v in ret2], clean_flag, 2)

        print('add new brush top:{} rand:{}'.format(len(ret1), len(ret2)))

        return True


    def hotfix_new(self, tbl, dba, prefix):
        sql = '''
            ALTER TABLE {} UPDATE shop_type = 11, pkey = toStartOfMonth(date) + 10
            WHERE `source`=2 AND shop_type=12 AND sid IN (
                11539845,11453222,11490313,11442110,10113287,10052806,10075676,10026347,925809,10184509,10297312,10140761,995976,11315620,10115629,10148860,10450972,10100015,10088644,10300928,10479689,10136833,10152176,10132892,10226072,10040501,11516896,11483186,10297451,946575,10179506,10299108,10033692,10155167
            )
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)


    # # 拆套包 默认按照number均分 返回 [[rate, id], ...]
    def calc_splitrate_new(self, item, data):
        total = sum([v['number'] for v in data])
        less  = 1
        for v in data:
            split_rate = v['number'] / total
            less -= split_rate
            v['split_rate'] = split_rate
        data[-1]['split_rate'] += less
        return data


# SELECT uuid,sign,ver,pkey,date,cid,real_cid,item_id,'sku_id',name,sid,shop_type,brand,'rbid','all_bid','alias_all_bid','sub_brand','region','region_str',price,org_price,
# promo_price,'trade',num,sales,img,trade_props.name,trade_props.value,trade_props_full.name,trade_props_full.value,props.name,props.value,'tip','source',now()
# FROM jd4_master.trade_81_215 WHERE uuid NOT IN (SELECT uuid FROM jd4_master.trade_81_215 WHERE sign = -1) LIMIT 10;

# SELECT bid, all_bid, alias_bid FROM jd.brand_has_alias_bid_month WHERE bid IN (SELECT brand FROM jd4_master.trade_81_215) LIMIT 10;