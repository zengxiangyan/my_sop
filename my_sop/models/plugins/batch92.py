import sys
import datetime
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app

class main(Batch.main):

    def brush(self, smonth, emonth, logId=-1):
        aname, atbl = self.get_a_tbl()
        cname, ctbl = self.get_c_tbl()
        where = "uuid2 in (SELECT uuid2 FROM {} WHERE date >= '{}' and date < '{}' and (sp7 = '' or sp8 = '假货'))".format(ctbl, smonth, emonth)
        ret,sbs = self.cleaner.process_top_anew(smonth, emonth, where=where)
        uuids = []
        for uuid2, source, tb_item_id, p1, cid, bid in ret:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids.append(uuid2)
        print('uuid2',len(uuids))
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1)
        sql = 'update {} set visible = 0 where clean_flag = {}'.format(self.cleaner.get_tbl(), clean_flag)
        self.cleaner.db26.execute(sql)
        self.cleaner.db26.commit()
        sql = 'select max(id) from {} where clean_flag = {} group by tb_item_id, sales/num'.format(self.cleaner.get_tbl(), clean_flag)
        max_id = ','.join([str(v[0]) for v in self.cleaner.db26.query_all(sql)])
        sql = 'update {} set visible = 1 where id in ({})'.format(self.cleaner.get_tbl(), max_id)
        self.cleaner.db26.execute(sql)
        self.cleaner.db26.commit()
        self.set_default_val()
        return True

    def set_default_val(self):
        clean_flag = self.cleaner.last_clean_flag()
        db26 = self.cleaner.get_db(self.cleaner.db26name)
        dbch = self.cleaner.get_db('chsop')
        # sku答题，只刷pid
        # 92的配置是sku答题，实际要刷sp
        # 属性答题，alias_all_bid_sp刷答题表的all_bid,alias_all_bid
        # 刷机洗值
        sql = 'SELECT spid from `dataway`.`clean_props` where eid = {}'.format(self.cleaner.eid)
        ret = db26.query_all(sql)
        cols = ['sp1'] + ['sp{}'.format(v[0]) for v in ret] + ['alias_all_bid_sp']

        cname, ctbl = self.get_c_tbl()
        cdba = self.cleaner.get_db(cname)
        db26 = self.cleaner.get_db('26_apollo')

        sql = 'SELECT id, uuid2, snum, pkey FROM {} where flag = 0 AND uuid2 != 0 and clean_flag = {}'.format(self.cleaner.get_tbl(), clean_flag)
        ret = db26.query_all(sql)

        for id, uuid2, snum, pkey, in ret:
            sql = 'SELECT {} FROM {} WHERE `source`={} AND pkey=\'{}\' AND uuid2={}'.format(
                ','.join(cols), ctbl, snum, pkey, uuid2
            )
            ret1 = cdba.query_all(sql)
            if len(ret1) == 0:
                continue
            sql = '''
                       UPDATE {} SET {}, alias_all_bid=IF({bid}=0,alias_all_bid,{bid}),all_bid=IF({bid}=0,all_bid,{bid}) WHERE id = {}
                   '''.format(self.cleaner.get_tbl(), ','.join(['{}=%s'.format(v.replace('sp', 'spid')) for v in cols[:-1]]), id, bid=(ret1[0][-1] or 0))
            print(sql)
            print(tuple(ret1[0][:-1]))
            db26.execute(sql, tuple(ret1[0][:-1]))
        # 以上刷sp

        sp1_list = [['服装', 1],
                    ['大皮具', 2],
                    ['小皮具', 3],
                    ['鞋类', 4],
                    ['腰带', 5],
                    ['围巾', 6]]
        for sp1, visible in sp1_list:
            sql = 'select id from {} where spid1 = \'{}\' and clean_flag = {} and visible = 1'.format(self.cleaner.get_tbl(), sp1, clean_flag)
            ids = ','.join([str(v[0]) for v in self.cleaner.db26.query_all(sql)])
            if ids != '':
                sql = 'update {} set visible = {} where id in ({})'.format(self.cleaner.get_tbl(), visible, ids)
                self.cleaner.db26.execute(sql)
                self.cleaner.db26.commit()


    def brush_0318(self, smonth, emonth, logId=-1):
        #补充出题
        # sql = '''
        #     SELECT source, tb_item_id, p1, argMax(uuid2, month)
        #     FROM {}_parts
        #     WHERE month >= '{smonth}' AND month < '{emonth}' and uuid2 in
        #     (SELECT uuid2 FROM {} WHERE date >= '{smonth}' and date < '{emonth}' and (sp12 = '包含改价'))
        #     GROUP BY source, tb_item_id, p1
        # '''.format(self.get_entity_tbl(), self.get_clean_tbl(), smonth=smonth, emonth=emonth)
        # ret = self.cleaner.dbch.query_all(sql)
        cname, ctbl = self.get_c_tbl()
        where = 'uuid2 in (select uuid2 from {} where sp12 = \'包含改价\')'.format(ctbl)
        uuids = []
        ret,sales_by_uuid = self.cleaner.process_top_anew(smonth,emonth, where=where)
        for uuid2, source, item_id, p1, cid, alias_all_bid in ret:
        # for source, tb_item_id, p1, uuid2, in ret:
            if self.skip_brush(source, item_id, p1):
                continue
            uuids.append(uuid2)

        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1)
        # self.set_default_val()
        # update visible = 0 where clean_flag = x
        # sql = 'update {} set visible = 0 where clean_flag = {}'.format(self.cleaner.get_tbl(), clean_flag)
        # self.cleaner.db26.execute(sql)
        # self.cleaner.db26.commit()
        # sql = 'select max(id) from {} where clean_flag = {} group by tb_item_id, sales/num'.format(self.cleaner.get_tbl(), clean_flag)
        # max_id = ','.join([str(v[0]) for v in self.cleaner.db26.query_all(sql)])
        # sql = 'update {} set visible = 1 where id in ({})'.format(self.cleaner.get_tbl(), max_id)
        # self.cleaner.db26.execute(sql)
        # self.cleaner.db26.commit()
        self.set_default_val()
        return True

    def brush_2(self, smonth, emonth):
        sql = 'SELECT source, tb_item_id, real_p1 FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0],v[1],v[2]):True for v in ret}

        sql = '''
            SELECT source, tb_item_id, p1, argMax(uuid2, month)
            FROM {}_parts
            WHERE month >= '{}' AND month < '{}'
            GROUP BY source, tb_item_id, p1
        '''.format(self.get_entity_tbl(), smonth, emonth)
        ret = self.cleaner.dbch.query_all(sql)

        uuids = []
        for source, tb_item_id, p1, uuid2, in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            uuids.append(uuid2)

        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag)

        print('add new brush {}'.format(len(uuids)))
        return True

    def brush_modify(self, v, bru_items):
        pass
        # if v['visible'] > 0:
        #     return
        # if len(bru_items) > 0:
        #     for vv in bru_items:
        #         if vv['itemid'] != v['itemid']:
        #             continue
        #         if vv['sales']/vv['num'] != v['sales']/v['num']:
        #             continue
        #         v['split_pids'] = vv['split_pids']
        #         v['pid'] = vv['pid']
        #         v['uid'] = vv['uid']
        #         v['number'] = vv['number']

#     def brush_modify(self, v, bru_items):
#         pass
#         # d = (datetime.date.today() - datetime.timedelta(days=10)).strftime("%Y-%m-%d")
#
#         # sql = 'SELECT count(*) FROM {} WHERE date>\'{}\' AND source={} AND item_id=\'{}\''.format(self.get_entity_tbl(), d, v['source'], v['tb_item_id'])
#         # ret = self.cleaner.dbch.query_all(sql)
#
#         # if ret[0][0] == 0:
#         #     sp5 = '失效'
#         # else:
#         #     sp5 = '未失效'
#
#         # if v['flag'] == 2:
#         #     for vv in v['split_pids']:
#         #         vv['sp5'] = sp5
#         # else:
#         #     v['sp5'] = sp5
