import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import random

class main(Batch.main):
    def brush_0705(self, smonth, emonth):

        clean_flag = self.cleaner.last_clean_flag() + 1

        ret1, sales_by_uuid = self.cleaner.process_top(smonth, emonth, limit=5000)
        ret2 = self.cleaner.process_rand(smonth, emonth, limit=1000)

        ret1_uuid = [v[0] for v in ret1]
        ret2_uuid = [v[0] for v in ret2]

        # sql = 'select uuid2 from {}_parts where uuid2 in ({}) and ' \
        #       'sp1 = \'其他\' '.format(self.cleaner.get_plugin().get_entity_tbl(), ','.join(['\''+str(t)+'\'' for t in ret1_uuid]))

        sql = 'select distinct(b.uuid) from entity_{}_clean a ' \
              'join entity_{}_origin_parts b on b.uniq_k = a.uniq_k ' \
              'where b.uuid2 in ({}) and a.sp3 = \'其它\''.format(self.cleaner.eid, self.cleaner.eid, ','.join(['\''+str(t)+'\'' for t in ret1_uuid]))

        uuids_others = [v[0] for v in self.cleaner.dbch.query_all(sql)]
        uuids_others = list(set(uuids_others))

        try:
            uuids_others_200 = random.sample(uuids_others, 200)
        except:
            uuids_others_200 = uuids_others
        uuids_800 = random.sample(ret1_uuid, 1000-len(uuids_others_200))

        self.cleaner.add_brush(ret2_uuid, clean_flag, 2)
        self.cleaner.add_brush(uuids_800 + uuids_others_200, clean_flag, 1, sales_by_uuid=sales_by_uuid)

    def brush_brush(self, smonth, emonth):

        clean_flag = self.cleaner.last_clean_flag() + 1

        ret1, sales_by_uuid = self.cleaner.process_top(smonth, emonth, limit=5000)
        ret2 = self.cleaner.process_rand(smonth, emonth, limit=1000)

        ret1_uuid = [v[0] for v in ret1]
        ret2_uuid = [v[0] for v in ret2]

        # sql = 'select uuid2 from {}_parts where uuid2 in ({}) and ' \
        #       'sp1 = \'其他\' '.format(self.cleaner.get_plugin().get_entity_tbl(), ','.join(['\''+str(t)+'\'' for t in ret1_uuid]))

        # sql = 'select distinct(b.uuid) from  a ' \
        #       'join entity_{}_origin_parts b on b.uniq_k = a.uniq_k ' \
        #       'where b.uuid2 in ({}) and a.sp3 = \'其它\''.format(self.cleaner.eid, self.cleaner.eid, ','.join(['\''+str(t)+'\'' for t in ret1_uuid]))

        sql = '''
        select uuid2 from {} where sp3 = '其它' and uuid2 in ({})
        '''.format(self.get_clean_tbl(), ','.join(['\''+str(t)+'\'' for t in ret1_uuid]))
        uuids_others = [v[0] for v in self.cleaner.dbch.query_all(sql)]
        uuids_others = list(set(uuids_others))
        try:
            uuids_others_200 = random.sample(uuids_others, 200)
        except:
            uuids_others_200 = uuids_others
        uuids_800 = random.sample(ret1_uuid, 1000-len(uuids_others_200))

        self.cleaner.add_brush(ret2_uuid, clean_flag, 2)
        self.cleaner.add_brush(uuids_800 + uuids_others_200, clean_flag, 1, sales_by_uuid=sales_by_uuid)




    def brush(self, smonth, emonth,logId=-1):

        sales_by_uuid = {}

        aname, atbl = self.get_a_tbl()
        cname, ctbl = self.get_all_tbl()
        db = self.cleaner.get_db(aname)

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}
        rets = []

        uuids = []

        sp3_list = ['男士沐浴露', '沐浴露']
        sources = ['source=11', 'source = 1 and (shop_type > 20 or shop_type < 10 )']
        for each_sp3 in sp3_list:
            for each_source in sources:
                top_brand_sql = '''select alias_all_bid,sum(sales*sign) as ss from {atbl} where uuid2 in (select uuid2 from {ctbl} where c_sp3='{each_sp3}') and {each_source} and pkey>=\'{smonth}\' and pkey<\'{emonth}\' group by alias_all_bid order by ss desc limit 15
                '''.format(atbl=atbl, ctbl=ctbl,each_sp3=each_sp3, each_source=each_source, smonth=smonth, emonth=emonth)
                bids = [v[0] for v in db.query_all(top_brand_sql)]
                for each_bid in bids:
                    where = '''
                    uuid2 in (select uuid2 from {ctbl} where alias_all_bid = {each_bid} and c_sp3='{each_sp3}') and {each_source}
                    '''.format(ctbl=ctbl, each_bid=each_bid, each_sp3=each_sp3, each_source=each_source)
                    ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=15, where=where)
                    sales_by_uuid.update(sbs)
                    rets.append(ret)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            continue
                        else:
                            uuids.append(uuid2)
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]
        print('--------------------------',len(uuids))

        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)

        return True



    # 2021.11.05 刷新品牌从出数步骤改到清洗步骤
    # def new_replace_info(self, item):
    #     if item['snum'] != 11:
    #         return

    #     k = 'replace {} {}'.format(item['snum'], item['pkey'])
    #     if k not in self.cache:
    #         dba = self.cleaner.get_db('chsop')

    #         sql = '''
    #             SELECT item_id,date,ner_bid,ner_brand,ner_cid,ner_category
    #             FROM sop_c.entity_prod_90694_C_tiktok WHERE pkey = '{}'
    #         '''.format(item['pkey'])
    #         ret = dba.query_all(sql)
    #         self.cache[k] = {'{}-{}'.format(v[0],v[1]):v for v in ret}

    #     kk = '{}-{}'.format(item['item_id'],item['date'])
    #     if kk in self.cache[k]:
    #         item['ner_bid'] = self.cache[k][kk][2]
    #         item['ner_brand'] = self.cache[k][kk][3]
    #         item['ner_cid'] = self.cache[k][kk][4]
    #         item['ner_category'] = self.cache[k][kk][5]


    def hotfix_new(self, tbl, dba, prefix):
        sql = '''
            SELECT DISTINCT sid FROM {} WHERE clean_alias_all_bid IN (
                SELECT IF(alias_bid=0,bid,alias_bid) FROM artificial.all_brand WHERE bid IN (11220)
            )
        '''.format(tbl)
        ret = dba.query_all(sql)
        ret = [v[0] for v in ret]

        if len(ret) > 0:
            sql = '''
                ALTER TABLE {} UPDATE `sp店铺分类` = 'EDT' WHERE sid IN {} SETTINGS mutations_sync = 1
            '''.format(tbl, ret)
            dba.execute(sql)


    def process_exx(self, tbl, prefix, logId=0):
        self.update_e_alias_bid(tbl)