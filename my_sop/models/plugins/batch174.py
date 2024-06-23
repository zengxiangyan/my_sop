import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app

class main(Batch.main):

    def brush_0716(self, smonth, emonth,logId=-1):
        uuids = []
        sales_by_uuid = {}

        aname, atbl = self.get_a_tbl()
        cname, ctbl = self.get_c_tbl()
        db = self.cleaner.get_db(aname)
        rets = []

        sp1_list = ['BB霜','安瓶','彩妆其它','彩妆套装','唇彩','唇膏','防晒','粉饼','粉底液','高光修容','隔离霜/妆前','护肤品其它','护肤套装','化妆水','睫毛膏','精华液','精油','眉笔眉粉','美甲','美容仪','美妆工具','面部磨砂','面膜','面霜','男士保湿霜','男士护肤套装','男士化妆水','男士洁面','男士精华','男士其它护肤','气垫','乳液','腮红','散粉','洗面奶','香水','卸妆','眼唇护理','眼线','眼影','遮瑕']
        for each_sp1 in sp1_list:
            where = '''uuid2 in (select uuid2 from {ctbl} where sp1 = \'{each_sp1}\')
            '''.format(ctbl=ctbl, each_sp1=each_sp1)
            ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=10,where=where)
            sales_by_uuid.update(sbs)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1):
                    continue
                uuids.append(uuid2)

        print('--------------------------',len(uuids))

        for each_sp1 in sp1_list:
            top_brand_sql = '''select alias_all_bid,sum(sales*sign) as ss from {atbl} where uuid2 in (select uuid2 from {ctbl} where sp1='{each_sp1}') and pkey>=\'{smonth}\' and pkey<\'{emonth}\' group by alias_all_bid order by ss desc limit 5
            '''.format(atbl=atbl, ctbl=ctbl,each_sp1=each_sp1, smonth=smonth, emonth=emonth)
            bids = [v[0] for v in db.query_all(top_brand_sql)]
            for each_bid in bids:
                where = '''
                uuid2 in (select uuid2 from {ctbl} where alias_all_bid = {each_bid})
                '''.format(ctbl=ctbl, each_bid=each_bid)
                ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=5, where=where)
                sales_by_uuid.update(sbs)
                rets.append(ret)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if self.skip_brush(source, tb_item_id, p1):
                        continue
                    uuids.append(uuid2)
        print('--------------------------',len(uuids))

        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)

        return True

    def brush_0720(self, smonth, emonth,logId=-1):
        uuids = []
        sales_by_uuid = {}

        aname, atbl = self.get_a_tbl()
        cname, ctbl = self.get_c_tbl()
        db = self.cleaner.get_db(aname)
        rets = []
        itemid = [3447806958172828510,3467310677956384562,3446875757564012136,3455498413934780059,3444075722271017006,3472760733564023680,3472760516802422357,611334105856,583936130279,575164751061,598628732928,574948341557,575061422878,593879363123,593319228204,593894843094,601302276452,593317140283,614126850846,607162791429,608578995669,607860680343,608581475225,593894111272,608358550101,624821494024,619022383974,617363635667,628198034056,627571040744,627574580617,628197858239,614201751502,629726468233,627902521962,628197426985,626410418157,630633593085,628186945166,628257469190,625657863939,627571476052,628201130909,629530147295,628849951145,627571272677,628492671531,627899085465,628492227921,629232930188,630368194975,628849659599,629232458366,627899125722,3466155198483986966,3447099379238998012,3454527463571500774,3462106257593377322]
        where = '''
        item_id in ({itemid})
        '''.format(itemid=','.join(["'"+str(x)+"'" for x in itemid]))
        ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=10000, where=where)
        sales_by_uuid.update(sbs)
        rets.append(ret)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids.append(uuid2)

        print('--------------------------', len(uuids))
        # uuid2 in (select uuid2 from {ctbl} where pkey >= \'{smonth}\' and pkey<\'{emonth}\') and item_id in ({itemid})
        # '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, itemid=','.join(["'"+str(x)+"'" for x in itemid]))
        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)

        return True

    def brush_0723(self, smonth, emonth,logId=-1):

        sales_by_uuid = {}

        aname, atbl = self.get_a_tbl()
        cname, ctbl = self.get_c_tbl()
        db = self.cleaner.get_db(aname)
        rets = []
        clean_flag = self.cleaner.last_clean_flag() + 1
        # ##dy
        # sp1_list = ['BB霜','安瓶','彩妆其它','彩妆套装','唇彩','唇膏','防晒','粉饼','粉底液','高光修容','隔离霜/妆前','护肤品其它','护肤套装','化妆水','睫毛膏','精华液','精油','眉笔眉粉','美甲','美容仪','美妆工具','面部磨砂','面膜','面霜','男士保湿霜','男士护肤套装','男士化妆水','男士洁面','男士精华','男士其它护肤','气垫','乳液','腮红','散粉','洗面奶','香水','卸妆','眼唇护理','眼线','眼影','遮瑕']
        # uuids = []
        # for each_sp1 in sp1_list:
        #     where = '''uuid2 in (select uuid2 from {ctbl} where sp1 = \'{each_sp1}\') and source = 11
        #     '''.format(ctbl=ctbl, each_sp1=each_sp1)
        #     ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=20, where=where)
        #     sales_by_uuid.update(sbs)
        #     for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
        #         if self.skip_brush(source, tb_item_id, p1):
        #             continue
        #         uuids.append(uuid2)
        # self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        #
        # sp1_list = ['BB霜', '安瓶', '彩妆其它', '彩妆套装', '唇彩', '唇膏', '防晒', '粉饼', '粉底液', '高光修容', '隔离霜/妆前', '护肤品其它', '护肤套装',
        #             '化妆水', '睫毛膏', '精华液', '精油', '眉笔眉粉', '美甲', '美容仪', '美妆工具', '面部磨砂', '面膜', '面霜', '男士保湿霜', '男士护肤套装',
        #             '男士化妆水', '男士洁面', '男士精华', '男士其它护肤', '气垫', '乳液', '腮红', '散粉', '洗面奶', '香水', '卸妆', '眼唇护理', '眼线', '眼影',
        #             '遮瑕']
        # uuids = []
        # for each_sp1 in sp1_list:
        #     top_brand_sql = '''select alias_all_bid,sum(sales*sign) as ss from {atbl} where uuid2 in (select uuid2 from {ctbl} where sp1='{each_sp1}') and source = 11 and pkey>=\'{smonth}\' and pkey<\'{emonth}\' group by alias_all_bid order by ss desc limit 5
        #     '''.format(atbl=atbl, ctbl=ctbl, each_sp1=each_sp1, smonth=smonth, emonth=emonth)
        #     bids = [v[0] for v in db.query_all(top_brand_sql)]
        #     for each_bid in bids:
        #         where = '''
        #         uuid2 in (select uuid2 from {ctbl} where alias_all_bid = {each_bid} and sp1 = \'{each_sp1}\') and source = 11
        #         '''.format(ctbl=ctbl, each_bid=each_bid, each_sp1=each_sp1)
        #         ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=5, where=where)
        #         sales_by_uuid.update(sbs)
        #         rets.append(ret)
        #         for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
        #             if self.skip_brush(source, tb_item_id, p1):
        #                 continue
        #             uuids.append(uuid2)
        # self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        ##cdf
        sp1_list = ['BB霜', '安瓶', '彩妆其它', '彩妆套装', '唇彩', '唇膏', '防晒', '粉饼', '粉底液', '高光修容', '隔离霜/妆前', '护肤品其它', '护肤套装',
                    '化妆水', '睫毛膏', '精华液', '精油', '眉笔眉粉', '美甲', '美容仪', '美妆工具', '面部磨砂', '面膜', '面霜', '男士保湿霜', '男士护肤套装',
                    '男士化妆水', '男士洁面', '男士精华', '男士其它护肤', '气垫', '乳液', '腮红', '散粉', '洗面奶', '香水', '卸妆', '眼唇护理', '眼线', '眼影',
                    '遮瑕', '男士面膜']
        uuids = []
        for each_sp1 in sp1_list:
            where = '''uuid2 in (select uuid2 from {ctbl} where sp1 = \'{each_sp1}\') and source = 12
                    '''.format(ctbl=ctbl, each_sp1=each_sp1)
            ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=20, where=where)
            sales_by_uuid.update(sbs)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                if self.skip_brush(source, tb_item_id, p1):
                    continue
                uuids.append(uuid2)
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)

        sp1_list = ['BB霜', '安瓶', '彩妆其它', '彩妆套装', '唇彩', '唇膏', '防晒', '粉饼', '粉底液', '高光修容', '隔离霜/妆前', '护肤品其它', '护肤套装',
                    '化妆水', '睫毛膏', '精华液', '精油', '眉笔眉粉', '美甲', '美容仪', '美妆工具', '面部磨砂', '面膜', '面霜', '男士保湿霜', '男士护肤套装',
                    '男士化妆水', '男士洁面', '男士精华', '男士其它护肤', '气垫', '乳液', '腮红', '散粉', '洗面奶', '香水', '卸妆', '眼唇护理', '眼线', '眼影',
                    '遮瑕', '男士面膜']
        uuids = []
        for each_sp1 in sp1_list:
            top_brand_sql = '''select alias_all_bid,sum(sales*sign) as ss from {atbl} where uuid2 in (select uuid2 from {ctbl} where sp1='{each_sp1}') and source = 12 and pkey>=\'{smonth}\' and pkey<\'{emonth}\' group by alias_all_bid order by ss desc limit 5
                    '''.format(atbl=atbl, ctbl=ctbl, each_sp1=each_sp1, smonth=smonth, emonth=emonth)
            bids = [v[0] for v in db.query_all(top_brand_sql)]
            for each_bid in bids:
                where = '''
                        uuid2 in (select uuid2 from {ctbl} where alias_all_bid = {each_bid} and sp1 = \'{each_sp1}\') and source = 12
                        '''.format(ctbl=ctbl, each_bid=each_bid, each_sp1=each_sp1)
                ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=5, where=where)
                sales_by_uuid.update(sbs)
                rets.append(ret)
                for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                    if self.skip_brush(source, tb_item_id, p1):
                        continue
                    uuids.append(uuid2)
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        # ##lvgou
        # sp1_list = ['BB霜', '安瓶', '彩妆其它', '彩妆套装', '唇彩', '唇膏', '防晒', '粉饼', '粉底液', '高光修容', '隔离霜/妆前', '护肤品其它', '护肤套装',
        #             '化妆水', '睫毛膏', '精华液', '精油', '眉笔眉粉', '美甲', '美容仪', '美妆工具', '面部磨砂', '面膜', '面霜', '男士保湿霜', '男士护肤套装',
        #             '男士化妆水', '男士洁面', '男士精华', '男士其它护肤', '气垫', '乳液', '腮红', '散粉', '洗面奶', '香水', '卸妆', '眼唇护理', '眼线', '眼影',
        #             '遮瑕']
        # uuids = []
        # for each_sp1 in sp1_list:
        #     where = '''uuid2 in (select uuid2 from {ctbl} where sp1 = \'{each_sp1}\') and source = 13
        #             '''.format(ctbl=ctbl, each_sp1=each_sp1)
        #     ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=20, where=where)
        #     sales_by_uuid.update(sbs)
        #     for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
        #         if self.skip_brush(source, tb_item_id, p1):
        #             continue
        #         uuids.append(uuid2)
        # self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        #
        # sp1_list = ['BB霜', '安瓶', '彩妆其它', '彩妆套装', '唇彩', '唇膏', '防晒', '粉饼', '粉底液', '高光修容', '隔离霜/妆前', '护肤品其它', '护肤套装',
        #             '化妆水', '睫毛膏', '精华液', '精油', '眉笔眉粉', '美甲', '美容仪', '美妆工具', '面部磨砂', '面膜', '面霜', '男士保湿霜', '男士护肤套装',
        #             '男士化妆水', '男士洁面', '男士精华', '男士其它护肤', '气垫', '乳液', '腮红', '散粉', '洗面奶', '香水', '卸妆', '眼唇护理', '眼线', '眼影',
        #             '遮瑕']
        # uuids = []
        # for each_sp1 in sp1_list:
        #     top_brand_sql = '''select alias_all_bid,sum(sales*sign) as ss from {atbl} where uuid2 in (select uuid2 from {ctbl} where sp1='{each_sp1}') and source = 13 and pkey>=\'{smonth}\' and pkey<\'{emonth}\' group by alias_all_bid order by ss desc limit 5
        #             '''.format(atbl=atbl, ctbl=ctbl, each_sp1=each_sp1, smonth=smonth, emonth=emonth)
        #     bids = [v[0] for v in db.query_all(top_brand_sql)]
        #     for each_bid in bids:
        #         where = '''
        #                 uuid2 in (select uuid2 from {ctbl} where alias_all_bid = {each_bid} and sp1 = \'{each_sp1}\') and source = 13
        #                 '''.format(ctbl=ctbl, each_bid=each_bid, each_sp1=each_sp1)
        #         ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=5, where=where)
        #         sales_by_uuid.update(sbs)
        #         rets.append(ret)
        #         for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
        #             if self.skip_brush(source, tb_item_id, p1):
        #                 continue
        #             uuids.append(uuid2)
        # self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)

        return True

    def brush_0729(self, smonth, emonth,logId=-1):

        sales_by_uuid = {}

        aname, atbl = self.get_a_tbl()
        cname, ctbl = self.get_c_tbl()
        db = self.cleaner.get_db(aname)
        rets = []
        clean_flag = self.cleaner.last_clean_flag() + 1
        uuids = []

        itemid_list = ['413770/413773','443575/432298','443575/432298','443575/432298','448023/432103','422170/418137','413770/413773','415256/415262','403987/403986','404058/404057','413611/413614','413611/413614','415377/415381',3485820247959851066,3460993908509663465,3471169991628463863,3471170010955830693,3459440019406745143,3474147950253457560,3462977771100325036,3460423543152606799,3483253951204294748,3459440019406745143,3468663792320249968,3467128369513119166,3471170002231642854,3446148816984743639,3475035077996576923,3467128369513119166,3477350578676482595,3474183471612743601,3474147950253457560,3486291554065366098,3485835076317641789,3470037997070766541,3483288587926149152,3474147950253457560,3486767298986010365,3459438402284467029,3459438402284467029,3477350234928091710,3468648356207850189,3459127755881462913,3459440019406745143,3460423543152606799,420240,394693,401086,607909,408476,678436,399585,408155,648069,548304,548303,648141939714,589986963617,643798380267,581333547897,638711611100,565178626329,610562140185,643039261577]


        where ='''
        uuid2 in (select uuid2 from {ctbl} where item_id in ({itemid}))
        '''.format(ctbl=ctbl, itemid=','.join(["'"+str(v)+"'" for v in itemid_list]))

        ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=10000, where=where)
        sales_by_uuid.update(sbs)
        rets.append(ret)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids.append(uuid2)

        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)

        return True

    def brush_0813(self, smonth, emonth,logId=-1):

        sales_by_uuid = {}

        aname, atbl = self.get_a_tbl()
        cname, ctbl = self.get_c_tbl()
        db = self.cleaner.get_db(aname)
        rets = []
        clean_flag = self.cleaner.last_clean_flag() + 1
        uuids = []
        sp1_list = ['护肤品其它']
        sources = ['source = 11', 'source = 12', 'source = 13']
        for ssmonth, eemonth in [['2019-01-01', '2021-04-01'], ['2021-04-01', '2021-08-01']]:
            for each_sp1 in sp1_list:
                for each_source in sources:
                    where = '''uuid2 in (select uuid2 from {ctbl} where sp1 = \'{each_sp1}\' and sp9 = 'Prestige') and {each_source} and pkey>=\'{ssmonth}\' and pkey<\'{eemonth}\'
                                        '''.format(ctbl=ctbl, each_sp1=each_sp1, each_source=each_source, ssmonth=ssmonth, eemonth=eemonth)
                    ret, sbs = self.cleaner.process_top_anew(ssmonth, eemonth, limit=50, where=where)
                    sales_by_uuid.update(sbs)
                    rets.append(ret)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if self.skip_brush(source, tb_item_id, p1):
                            continue
                        uuids.append(uuid2)
        print ('-----------------', len(uuids))
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)

        return True

    def brush_0816(self, smonth, emonth,logId=-1):

        sales_by_uuid = {}

        aname, atbl = self.get_a_tbl()
        cname, ctbl = self.get_c_tbl()
        db = self.cleaner.get_db(aname)
        rets = []
        clean_flag = self.cleaner.last_clean_flag() + 1

        sources = ['source = 11', 'source = 12', 'source = 13', 'source = 1 and shop_type>20']
        sp1_list = ['BB霜','安瓶','彩妆其它','彩妆套装','唇彩','唇膏','防晒','粉饼','粉底液','高光修容','隔离霜/妆前','护肤品其它','护肤套装','化妆水','睫毛膏','精华液','精油','眉笔眉粉','美甲','美容仪','美妆工具','面部磨砂','面膜','面霜','男士保湿霜','男士护肤套装','男士化妆水','男士洁面','男士精华','男士其它护肤','气垫','乳液','腮红','散粉','洗面奶','香水','卸妆','眼唇护理','眼线','眼影','遮瑕']
        sp9s = ['Mass', 'Prestige']
        uuids = []
        for each_sp1 in sp1_list:
            for each_source in sources:
                for each_sp9 in sp9s:
                    where = '''uuid2 in (select uuid2 from {ctbl} where sp1 = \'{each_sp1}\' and sp9=\'{each_sp9}\') and {each_source}
                    '''.format(ctbl=ctbl, each_sp1=each_sp1, each_sp9=each_sp9, each_source=each_source)
                    ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=20, where=where)
                    sales_by_uuid.update(sbs)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if self.skip_brush(source, tb_item_id, p1):
                            continue
                        uuids.append(uuid2)

        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)

        uuids = []
        for each_sp1 in sp1_list:
            for each_source in sources:
                for each_sp9 in sp9s:
                    top_brand_sql = '''select alias_all_bid,sum(sales*sign) as ss from {atbl} where uuid2 in (select uuid2 from {ctbl} where sp1='{each_sp1}' and sp9=\'{each_sp9}\') and {each_source} and pkey>=\'{smonth}\' and pkey<\'{emonth}\' group by alias_all_bid order by ss desc limit 5
                            '''.format(atbl=atbl, ctbl=ctbl, each_sp1=each_sp1, each_sp9=each_sp9, smonth=smonth, emonth=emonth, each_source=each_source)
                    bids = [v[0] for v in db.query_all(top_brand_sql)]
                    for each_bid in bids:
                        where = '''
                                uuid2 in (select uuid2 from {ctbl} where alias_all_bid = {each_bid} and sp1 = \'{each_sp1}\' and sp9=\'{each_sp9}\') and {each_source}
                                '''.format(ctbl=ctbl, each_bid=each_bid, each_sp1=each_sp1, each_sp9=each_sp9, each_source=each_source)
                        ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=5, where=where)
                        sales_by_uuid.update(sbs)
                        rets.append(ret)
                        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                            if self.skip_brush(source, tb_item_id, p1):
                                continue
                            uuids.append(uuid2)
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)



        return True


    def brush_0831(self, smonth, emonth,logId=-1):

        sales_by_uuid = {}

        aname, atbl = self.get_a_tbl()
        cname, ctbl = self.get_c_tbl()
        db = self.cleaner.get_db(aname)
        rets = []
        clean_flag = self.cleaner.last_clean_flag() + 1
        uuids = []

        itemid_list = ['3468030909419270793',
'3490915253829634882',
'3453430369834122644',
'3486412755878152460',
'3483620509558689022',
'3489939742303369701',
'3493447847934876228',
'3490851417797856685',
'3491270527098721340',
'3491597393454046243',
'3434246839837281570',
'3492180237494650628',
'3491021953593130613',
'3484715633734715939',
'3491121079383098038',
'412477/412480',
'283570/283570',
'400178/400177',
'302841/302841',
'403409/403408',
'444239/434371',
'412785/412788',
'449079/439067',
'288902/288902',
'426175/420855',
'415765/415768',
'405241/405240',
'287069/287069',
'300683/300683',
'414833/414839',
'402686/402685',
'434677/425858',
'426188/420868',
'413546/413549',
'442281/432367',
'408001/408004',
'407995/407998',
'407998/408001',
'441848/432202',
'418650/416134',
'422308/418255',
'400381',
'405880',
'417422',
'617267',
'419981',
'406694',
'722925',
'406696',
'400427',
'391862',
'426476',
'417666',
'405321',
'585144',
'403606',
'617266',
'665681',
'406695',
'420272',
'393770',
'420273',
'665655',
'409007',
'713209',
'422339',
'422061',
'396863',
'420236',
'391937',
'620377',
'407392',
'681462',
'419702',
'614700',
'426543',
'603306',
'545122',
'408894',
'390215',
'576733',
'391436',
'417429',
'417969',
'422144',
'624673019748',
'647148307163',
'23054112965']


        where ='''
        uuid2 in (select uuid2 from {ctbl} where item_id in ({itemid}))
        '''.format(ctbl=ctbl, itemid=','.join(["'"+str(v)+"'" for v in itemid_list]))

        ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=10000, where=where)
        sales_by_uuid.update(sbs)
        rets.append(ret)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids.append(uuid2)

        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)

#         sources = ['source = 11']
#         sp1_list = ['BB霜',
# '安瓶',
# '彩妆其它',
# '彩妆套装',
# '唇彩',
# '唇膏',
# '防晒',
# '粉饼',
# '粉底液',
# '高光修容',
# '隔离霜/妆前',
# '护肤品其它',
# '护肤套装',
# '化妆水',
# '睫毛膏',
# '精华液',
# '精油',
# '眉笔眉粉',
# '美甲',
# '美容仪',
# '美妆工具',
# '面部磨砂',
# '面膜',
# '面霜',
# '男士乳液面霜',
# '男士护肤套装',
# '男士化妆水',
# '男士洁面',
# '男士精华',
# '男士其它护肤',
# '气垫',
# '乳液',
# '腮红',
# '散粉',
# '洗面奶',
# '香水',
# '卸妆',
# '眼唇护理',
# '眼线',
# '眼影',
# '遮瑕',
# '男士面膜']
#         sp9s = ['Mass', 'Prestige']
#         uuids = []
#         for each_sp1 in sp1_list:
#             for each_source in sources:
#                 for each_sp9 in sp9s:
#                     where = '''uuid2 in (select uuid2 from {ctbl} where sp1 = \'{each_sp1}\' and sp9=\'{each_sp9}\') and {each_source}
#                             '''.format(ctbl=ctbl, each_sp1=each_sp1, each_sp9=each_sp9, each_source=each_source)
#                     ret, sbs = self.cleaner.process_top_anew('2021-07-01', '2021-08-01', limit=100, where=where)
#                     sales_by_uuid.update(sbs)
#                     for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
#                         if self.skip_brush(source, tb_item_id, p1):
#                             continue
#                         uuids.append(uuid2)
#
#         self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)



        return True

    def brush_0516(self, smonth, emonth,logId=-1):

        sales_by_uuid = {}

        aname, atbl = self.get_a_tbl()
        cname, ctbl = self.get_c_tbl()
        db = self.cleaner.get_db(aname)
        rets = []
        clean_flag = self.cleaner.last_clean_flag() + 1

        sources1 = ['source = 12', 'source = 13', 'source = 1 and shop_type>20']
        sp1_list = ['BB霜',	'安瓶',	'彩妆其它',	'彩妆套装',	'唇彩',	'唇膏',	'防晒',	'粉饼',	'粉底液',	'高光修容',	'隔离霜/妆前',	'护肤品其它',	'护肤套装',	'化妆水',	'睫毛膏',	'精华液',	'精油',	'眉笔眉粉',	'美甲',	'美容仪',	'美妆工具',	'面部磨砂',	'面膜',	'面霜',	'男士面膜',	'男士保湿霜',	'男士护肤套装',	'男士化妆水',	'男士洁面',	'男士精华',	'男士其它护肤',	'气垫',	'乳液',	'腮红',	'散粉',	'洗面奶',	'香水',	'卸妆',	'眼唇护理',	'眼线',	'眼影',	'遮瑕']
        sp9s = ['Mass', 'Prestige']
        uuids = []
        for each_sp1 in sp1_list:
            for each_source in sources1:
                for each_sp9 in sp9s:
                    where = '''uuid2 in (select uuid2 from {ctbl} where sp1 = \'{each_sp1}\' and sp9=\'{each_sp9}\') and {each_source}
                    '''.format(ctbl=ctbl, each_sp1=each_sp1, each_sp9=each_sp9, each_source=each_source)
                    ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=20, where=where)
                    sales_by_uuid.update(sbs)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if self.skip_brush(source, tb_item_id, p1):
                            continue
                        uuids.append(uuid2)

        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)

        uuids = []
        for each_sp1 in sp1_list:
            for each_source in sources1:
                for each_sp9 in sp9s:
                    top_brand_sql = '''select alias_all_bid,sum(sales*sign) as ss from {atbl} where uuid2 in (select uuid2 from {ctbl} where sp1='{each_sp1}' and sp9=\'{each_sp9}\') and {each_source} and pkey>=\'{smonth}\' and pkey<\'{emonth}\' group by alias_all_bid order by ss desc limit 5
                            '''.format(atbl=atbl, ctbl=ctbl, each_sp1=each_sp1, each_sp9=each_sp9, smonth=smonth, emonth=emonth, each_source=each_source)
                    bids = [v[0] for v in db.query_all(top_brand_sql)]
                    for each_bid in bids:
                        where = '''
                                uuid2 in (select uuid2 from {ctbl} where alias_all_bid = {each_bid} and sp1 = \'{each_sp1}\' and sp9=\'{each_sp9}\') and {each_source}
                                '''.format(ctbl=ctbl, each_bid=each_bid, each_sp1=each_sp1, each_sp9=each_sp9, each_source=each_source)
                        ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=5, where=where)
                        sales_by_uuid.update(sbs)
                        rets.append(ret)
                        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                            if self.skip_brush(source, tb_item_id, p1):
                                continue
                            uuids.append(uuid2)
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)

        sources2 = ['source = 11']

        uuids = []
        for each_sp1 in sp1_list:
            for each_source in sources2:
                for each_sp9 in sp9s:
                    where = '''uuid2 in (select uuid2 from {ctbl} where sp1 = \'{each_sp1}\' and sp9=\'{each_sp9}\') and {each_source}
                            '''.format(ctbl=ctbl, each_sp1=each_sp1, each_sp9=each_sp9, each_source=each_source)
                    ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=50, where=where)
                    sales_by_uuid.update(sbs)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if self.skip_brush(source, tb_item_id, p1):
                            continue
                        uuids.append(uuid2)

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
    #             FROM sop_c.entity_prod_91363_C_tiktok WHERE pkey = '{}'
    #         '''.format(item['pkey'])
    #         ret = dba.query_all(sql)
    #         self.cache[k] = {'{}-{}'.format(v[0],v[1]):v for v in ret}

    #     kk = '{}-{}'.format(item['item_id'],item['date'])
    #     if kk in self.cache[k]:
    #         item['ner_bid'] = self.cache[k][kk][2]
    #         item['ner_brand'] = self.cache[k][kk][3]
    #         item['ner_cid'] = self.cache[k][kk][4]
    #         item['ner_category'] = self.cache[k][kk][5]

    def brush_monthly_old(self, smonth, emonth,logId=-1):
    ###月度出题规则

        sales_by_uuid = {}

        aname, atbl = self.get_a_tbl()
        cname, ctbl = self.get_all_tbl()
        db = self.cleaner.get_db(aname)
        rets = []
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}



        # Part1

        sp1_list_part1 = ['眉笔眉粉',	'眼线',	'眼影',	'睫毛膏',	'BB霜',	'腮红',	'遮瑕',	'气垫',	'粉底液',	'高光修容',	'散粉',	'隔离霜/妆前',	'粉饼',	'唇彩',	'唇膏',	'彩妆套装']
        sp9_list_part1 = ['Mass', 'Prestige']
        uuids_part1 = []

        sources_tmall = ['source = 1 and (shop_type > 20 or shop_type < 10 )']

        for each_sp1 in sp1_list_part1:
            for each_source in sources_tmall:
                for each_sp9 in sp9_list_part1:
                    where = '''uuid2 in (select uuid2 from {ctbl} where c_sp1 = \'{each_sp1}\' and c_sp9=\'{each_sp9}\' and pkey>'{smonth}' and pkey <'{emonth}') and {each_source}
                    '''.format(ctbl=ctbl, each_sp1=each_sp1, each_sp9=each_sp9, each_source=each_source, smonth=smonth, emonth=emonth)
                    ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=20, where=where)
                    sales_by_uuid.update(sbs)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            continue
                        else:
                            uuids_part1.append(uuid2)
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

        for each_sp1 in sp1_list_part1:
            for each_source in sources_tmall:
                for each_sp9 in sp9_list_part1:
                    top_brand_sql = '''select alias_all_bid,sum(sales*sign) as ss from {atbl} where uuid2 in (select uuid2 from {ctbl} where c_sp1='{each_sp1}' and c_sp9=\'{each_sp9}\' and pkey>'{smonth}' and pkey <'{emonth}') and {each_source} and pkey>=\'{smonth}\' and pkey<\'{emonth}\' group by alias_all_bid order by ss desc limit 5
                            '''.format(atbl=atbl, ctbl=ctbl, each_sp1=each_sp1, each_sp9=each_sp9, smonth=smonth, emonth=emonth, each_source=each_source)
                    bids = [v[0] for v in db.query_all(top_brand_sql)]
                    for each_bid in bids:
                        where = '''
                                uuid2 in (select uuid2 from {ctbl} where alias_all_bid = {each_bid} and c_sp1 = \'{each_sp1}\' and c_sp9=\'{each_sp9}\' and pkey>'{smonth}' and pkey <'{emonth}') and {each_source}
                                '''.format(ctbl=ctbl, each_bid=each_bid, each_sp1=each_sp1, each_sp9=each_sp9, each_source=each_source, smonth=smonth, emonth=emonth)
                        ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=5, where=where)
                        sales_by_uuid.update(sbs)
                        rets.append(ret)
                        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                                continue
                            else:
                                uuids_part1.append(uuid2)
                                mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

        sources_dy = ['source = 11']

        for each_sp1 in sp1_list_part1:
            for each_source in sources_dy:
                for each_sp9 in sp9_list_part1:
                    where = '''uuid2 in (select uuid2 from {ctbl} where c_sp1 = \'{each_sp1}\' and c_sp9=\'{each_sp9}\' and pkey>'{smonth}' and pkey <'{emonth}') and {each_source}
                                '''.format(ctbl=ctbl, each_sp1=each_sp1, each_sp9=each_sp9, each_source=each_source,
                                           smonth=smonth, emonth=emonth)
                    ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=30, where=where)
                    sales_by_uuid.update(sbs)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            continue
                        else:
                            uuids_part1.append(uuid2)
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

        self.cleaner.add_brush(uuids_part1, clean_flag, 1, sales_by_uuid=sales_by_uuid)

        ## Part2

        sp1_list_part2 = ['面部磨砂',	'洗面奶',	'卸妆',	'面霜',	'美容仪',	'乳液',	'安瓶',	'精油',	'精华液',	'眼唇护理',	'化妆水',	'面膜',	'男士洁面',	'男士精华',	'男士面膜',	'男士乳液面霜',	'男士其它护肤',	'男士护肤套装',	'男士化妆水',	'护肤套装',	'防晒',	'香水']
        sp9_list_part2 = ['Prestige']
        uuids_part2 = []

        sources_tmall = ['source = 1 and (shop_type > 20 or shop_type < 10 )']

        for each_sp1 in sp1_list_part2:
            for each_source in sources_tmall:
                for each_sp9 in sp9_list_part2:
                    where = '''uuid2 in (select uuid2 from {ctbl} where c_sp1 = \'{each_sp1}\' and c_sp9=\'{each_sp9}\' and pkey>'{smonth}' and pkey <'{emonth}') and {each_source}
                            '''.format(ctbl=ctbl, each_sp1=each_sp1, each_sp9=each_sp9, each_source=each_source,
                                       smonth=smonth, emonth=emonth)
                    ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=20, where=where)
                    sales_by_uuid.update(sbs)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            continue
                        else:
                            uuids_part2.append(uuid2)
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

        for each_sp1 in sp1_list_part2:
            for each_source in sources_tmall:
                for each_sp9 in sp9_list_part2:
                    top_brand_sql = '''select alias_all_bid,sum(sales*sign) as ss from {atbl} where uuid2 in (select uuid2 from {ctbl} where c_sp1='{each_sp1}' and c_sp9=\'{each_sp9}\' and pkey>'{smonth}' and pkey <'{emonth}') and {each_source} and pkey>=\'{smonth}\' and pkey<\'{emonth}\' group by alias_all_bid order by ss desc limit 5
                                    '''.format(atbl=atbl, ctbl=ctbl, each_sp1=each_sp1, each_sp9=each_sp9, smonth=smonth,
                                               emonth=emonth, each_source=each_source)
                    bids = [v[0] for v in db.query_all(top_brand_sql)]
                    for each_bid in bids:
                        where = '''
                                        uuid2 in (select uuid2 from {ctbl} where alias_all_bid = {each_bid} and c_sp1 = \'{each_sp1}\' and c_sp9=\'{each_sp9}\' and pkey>'{smonth}' and pkey <'{emonth}') and {each_source}
                                        '''.format(ctbl=ctbl, each_bid=each_bid, each_sp1=each_sp1, each_sp9=each_sp9,
                                                   each_source=each_source, smonth=smonth, emonth=emonth)
                        ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=5, where=where)
                        sales_by_uuid.update(sbs)
                        rets.append(ret)
                        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                                continue
                            else:
                                uuids_part2.append(uuid2)
                                mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

        sources_dy = ['source = 11']

        for each_sp1 in sp1_list_part2:
            for each_source in sources_dy:
                for each_sp9 in sp9_list_part2:
                    where = '''uuid2 in (select uuid2 from {ctbl} where c_sp1 = \'{each_sp1}\' and c_sp9=\'{each_sp9}\' and pkey>'{smonth}' and pkey <'{emonth}') and {each_source}
                                        '''.format(ctbl=ctbl, each_sp1=each_sp1, each_sp9=each_sp9, each_source=each_source,
                                                   smonth=smonth, emonth=emonth)
                    ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=30, where=where)
                    sales_by_uuid.update(sbs)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            continue
                        else:
                            uuids_part2.append(uuid2)
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

        self.cleaner.add_brush(uuids_part2, clean_flag, 1, sales_by_uuid=sales_by_uuid)

        # Part3

        sp1_list_part3 = ['BB霜',	'安瓶',	'彩妆其它',	'彩妆套装',	'唇彩',	'唇膏',	'防晒',	'粉饼',	'粉底液',	'高光修容',	'隔离霜/妆前',	'护肤品其它',	'护肤套装',	'化妆水',	'睫毛膏',	'精华液',	'精油',	'眉笔眉粉',	'美甲',	'美容仪',	'美妆工具',	'面部磨砂',	'面膜',	'面霜',	'男士乳液面霜',	'男士护肤套装',	'男士化妆水',	'男士洁面',	'男士精华',	'男士其它护肤',	'气垫',	'乳液',	'腮红',	'散粉',	'洗面奶',	'香水',	'卸妆',	'眼唇护理',	'眼线',	'眼影',	'遮瑕',	'男士面膜']
        sp9_list_part3 = ['Prestige']
        uuids_part3 = []

        sources_cdf_lvgou = ['source = 12', 'source in (13,16)']

        for each_sp1 in sp1_list_part3:
            for each_source in sources_cdf_lvgou:
                for each_sp9 in sp9_list_part3:
                    where = '''uuid2 in (select uuid2 from {ctbl} where c_sp1 = \'{each_sp1}\' and c_sp9=\'{each_sp9}\' and pkey>'{smonth}' and pkey <'{emonth}') and {each_source}
                                    '''.format(ctbl=ctbl, each_sp1=each_sp1, each_sp9=each_sp9, each_source=each_source,
                                               smonth=smonth, emonth=emonth)
                    ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=20, where=where)
                    sales_by_uuid.update(sbs)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            continue
                        else:
                            uuids_part3.append(uuid2)
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

        for each_sp1 in sp1_list_part3:
            for each_source in sources_cdf_lvgou:
                for each_sp9 in sp9_list_part3:
                    top_brand_sql = '''select alias_all_bid,sum(sales*sign) as ss from {atbl} where uuid2 in (select uuid2 from {ctbl} where c_sp1='{each_sp1}' and c_sp9=\'{each_sp9}\' and pkey>'{smonth}' and pkey <'{emonth}') and {each_source} and pkey>=\'{smonth}\' and pkey<\'{emonth}\' group by alias_all_bid order by ss desc limit 5
                                    '''.format(atbl=atbl, ctbl=ctbl, each_sp1=each_sp1, each_sp9=each_sp9, smonth=smonth,
                                               emonth=emonth, each_source=each_source)
                    bids = [v[0] for v in db.query_all(top_brand_sql)]
                    for each_bid in bids:
                        where = '''
                                        uuid2 in (select uuid2 from {ctbl} where alias_all_bid = {each_bid} and c_sp1 = \'{each_sp1}\' and c_sp9=\'{each_sp9}\' and pkey>'{smonth}' and pkey <'{emonth}') and {each_source}
                                        '''.format(ctbl=ctbl, each_bid=each_bid, each_sp1=each_sp1, each_sp9=each_sp9,
                                                   each_source=each_source, smonth=smonth, emonth=emonth)
                        ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=5, where=where)
                        sales_by_uuid.update(sbs)
                        rets.append(ret)
                        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                                continue
                            else:
                                uuids_part3.append(uuid2)
                                mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

        self.cleaner.add_brush(uuids_part3, clean_flag, 1, sales_by_uuid=sales_by_uuid)



        return True

    def brush_jidu_old(self, smonth, emonth,logId=-1):
    ###季度出题规则220808已废弃

        sales_by_uuid = {}

        aname, atbl = self.get_a_tbl()
        cname, ctbl = self.get_all_tbl()
        db = self.cleaner.get_db(aname)
        rets = []
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        sp1_lists = ['安瓶',	'防晒',	'护肤品其它',	'护肤套装',	'化妆水',	'精华液',	'精油',	'美容仪',	'面部磨砂',	'面膜',	'面霜',	'男士面膜',	'男士乳液面霜',	'男士护肤套装',	'男士化妆水',	'男士洁面',	'男士精华',	'男士其它护肤',	'乳液',	'洗面奶',	'香水',	'卸妆',	'眼唇护理']
        sp9_lists = ['Mass', 'Prestige']
        # sources = ['source = 1 and (shop_type > 20 or shop_type < 10 )', 'source = 12', 'source in (13,16)']

        sources = ['source = 1 and (shop_type > 20 or shop_type < 10 )']

        uuidsA = []

        for each_sp1 in sp1_lists:
            for each_source in sources:
                for each_sp9 in sp9_lists:
                    where = '''uuid2 in (select uuid2 from {ctbl} where c_sp1 = \'{each_sp1}\' and c_sp9=\'{each_sp9}\' and pkey>'{smonth}' and pkey <'{emonth}') and {each_source}
                                    '''.format(ctbl=ctbl, each_sp1=each_sp1, each_sp9=each_sp9, each_source=each_source,
                                               smonth=smonth, emonth=emonth)
                    ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=20, where=where)
                    sales_by_uuid.update(sbs)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            continue
                        else:
                            uuidsA.append(uuid2)
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

        for each_sp1 in sp1_lists:
            for each_source in sources:
                for each_sp9 in sp9_lists:
                    top_brand_sql = '''select alias_all_bid,sum(sales*sign) as ss from {atbl} where uuid2 in (select uuid2 from {ctbl} where c_sp1='{each_sp1}' and c_sp9=\'{each_sp9}\' and pkey>'{smonth}' and pkey <'{emonth}') and {each_source} and pkey>=\'{smonth}\' and pkey<\'{emonth}\' group by alias_all_bid order by ss desc limit 5
                                    '''.format(atbl=atbl, ctbl=ctbl, each_sp1=each_sp1, each_sp9=each_sp9, smonth=smonth,
                                               emonth=emonth, each_source=each_source)
                    bids = [v[0] for v in db.query_all(top_brand_sql)]
                    for each_bid in bids:
                        where = '''
                                        uuid2 in (select uuid2 from {ctbl} where alias_all_bid = {each_bid} and c_sp1 = \'{each_sp1}\' and c_sp9=\'{each_sp9}\' and pkey>'{smonth}' and pkey <'{emonth}') and {each_source}
                                        '''.format(ctbl=ctbl, each_bid=each_bid, each_sp1=each_sp1, each_sp9=each_sp9,
                                                   each_source=each_source, smonth=smonth, emonth=emonth)
                        ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=5, where=where)
                        sales_by_uuid.update(sbs)
                        rets.append(ret)
                        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                                continue
                            else:
                                uuidsA.append(uuid2)
                                mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

        self.cleaner.add_brush(uuidsA, clean_flag, 1, sales_by_uuid=sales_by_uuid)

        uuidsB = []

        sources_dy = ['source = 11']

        for each_sp1 in sp1_lists:
            for each_source in sources_dy:
                for each_sp9 in sp9_lists:
                    where = '''uuid2 in (select uuid2 from {ctbl} where c_sp1 = \'{each_sp1}\' and c_sp9=\'{each_sp9}\' and pkey>'{smonth}' and pkey <'{emonth}') and {each_source}
                                                '''.format(ctbl=ctbl, each_sp1=each_sp1, each_sp9=each_sp9,
                                                           each_source=each_source,
                                                           smonth=smonth, emonth=emonth)
                    ret, sbs = self.cleaner.process_top_anew(smonth, emonth, limit=50, where=where)
                    sales_by_uuid.update(sbs)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            continue
                        else:
                            uuidsB.append(uuid2)
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

        self.cleaner.add_brush(uuidsB, clean_flag, 1, sales_by_uuid=sales_by_uuid)

        return True


    def brush(self, smonth, emonth,logId=-1):
    ###月度新出题规则

        sales_by_uuid = {}

        aname, atbl = self.get_a_tbl()
        cname, ctbl = self.get_all_tbl()
        db = self.cleaner.get_db(aname)
        rets = []
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT snum, tb_item_id, p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        brush_p1, filter_p1 = self.cleaner.get_plugin().filter_brush_props()
        data = []
        for snum, tb_item_id, p1, id, visible in ret:
            p1 = brush_p1(snum, p1)
            data.append([snum, tb_item_id, p1, id, visible])

        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in data}

        # cname, ctbl = self.get_c_tbl()
        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()



        # Part1

        sp1_list_part1 = ['眉笔眉粉',	'眼线',	'眼影',	'睫毛膏',	'BB霜',	'腮红',	'遮瑕',	'气垫',	'粉底液',	'高光修容',	'散粉',	'隔离霜/妆前',	'粉饼',	'唇彩',	'唇膏',	'彩妆套装']
        sp9_list_part1 = ['Mass', 'Prestige']
        uuids_part1 = []

        sources_tmall = ['source = 1 and (shop_type > 20 or shop_type < 10 )']

        for each_sp1 in sp1_list_part1:
            for each_source in sources_tmall:
                for each_sp9 in sp9_list_part1:
                    where = '''c_sp1 = \'{each_sp1}\' and c_sp9=\'{each_sp9}\' and {each_source}
                    '''.format(ctbl=ctbl, each_sp1=each_sp1, each_sp9=each_sp9, each_source=each_source, smonth=smonth, emonth=emonth)
                    ret, sbs = self.cleaner.process_top_aft200(smonth, emonth, limit=20, where=where)
                    sales_by_uuid.update(sbs)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            continue
                        else:
                            uuids_part1.append(uuid2)
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

        for each_sp1 in sp1_list_part1:
            for each_source in sources_tmall:
                for each_sp9 in sp9_list_part1:
                    top_brand_sql = '''select alias_all_bid,sum(sales*sign) as ss from {ctbl}  where c_sp1='{each_sp1}' and c_sp9=\'{each_sp9}\' and {each_source} and toYYYYMM(date) >= toYYYYMM(toDate(\'{smonth}\')) AND toYYYYMM(date) < toYYYYMM(toDate(\'{emonth}\')) group by alias_all_bid order by ss desc limit 5
                            '''.format(atbl=atbl, ctbl=ctbl, each_sp1=each_sp1, each_sp9=each_sp9, smonth=smonth, emonth=emonth, each_source=each_source)
                    bids = [v[0] for v in db.query_all(top_brand_sql)]
                    for each_bid in bids:
                        where = '''
                                alias_all_bid = {each_bid} and c_sp1 = \'{each_sp1}\' and c_sp9=\'{each_sp9}\' and {each_source}
                                '''.format(ctbl=ctbl, each_bid=each_bid, each_sp1=each_sp1, each_sp9=each_sp9, each_source=each_source, smonth=smonth, emonth=emonth)
                        ret, sbs = self.cleaner.process_top_aft200(smonth, emonth, limit=5, where=where)
                        sales_by_uuid.update(sbs)
                        rets.append(ret)
                        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                                continue
                            else:
                                uuids_part1.append(uuid2)
                                mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

        sources_dy = ['source = 11']

        for each_sp1 in sp1_list_part1:
            for each_source in sources_dy:
                for each_sp9 in sp9_list_part1:
                    where = '''c_sp1 = \'{each_sp1}\' and c_sp9=\'{each_sp9}\' and {each_source}
                                '''.format(ctbl=ctbl, each_sp1=each_sp1, each_sp9=each_sp9, each_source=each_source,
                                           smonth=smonth, emonth=emonth)
                    ret, sbs = self.cleaner.process_top_aft200(smonth, emonth, limit=30, where=where)
                    sales_by_uuid.update(sbs)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            continue
                        else:
                            uuids_part1.append(uuid2)
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

        self.cleaner.add_brush(uuids_part1, clean_flag, 1, sales_by_uuid=sales_by_uuid)

        ## Part2

        sp1_list_part2 = ['面部磨砂',	'洗面奶',	'卸妆',	'面霜',	'美容仪',	'乳液',	'安瓶',	'精油',	'精华液',	'眼唇护理',	'化妆水',	'面膜',	'男士洁面',	'男士精华',	'男士面膜',	'男士乳液面霜',	'男士其它护肤',	'男士护肤套装',	'男士化妆水',	'护肤套装',	'防晒',	'香水']
        sp9_list_part2 = ['Prestige']
        uuids_part2 = []

        sources_tmall = ['source = 1 and (shop_type > 20 or shop_type < 10 )']

        for each_sp1 in sp1_list_part2:
            for each_source in sources_tmall:
                for each_sp9 in sp9_list_part2:
                    where = '''c_sp1 = \'{each_sp1}\' and c_sp9=\'{each_sp9}\' and {each_source}
                            '''.format(ctbl=ctbl, each_sp1=each_sp1, each_sp9=each_sp9, each_source=each_source,
                                       smonth=smonth, emonth=emonth)
                    ret, sbs = self.cleaner.process_top_aft200(smonth, emonth, limit=20, where=where)
                    sales_by_uuid.update(sbs)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            continue
                        else:
                            uuids_part2.append(uuid2)
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

        for each_sp1 in sp1_list_part2:
            for each_source in sources_tmall:
                for each_sp9 in sp9_list_part2:
                    top_brand_sql = '''select alias_all_bid,sum(sales*sign) as ss from {ctbl} where c_sp1='{each_sp1}' and c_sp9=\'{each_sp9}\' and {each_source} and toYYYYMM(date) >= toYYYYMM(toDate(\'{smonth}\')) AND toYYYYMM(date) < toYYYYMM(toDate(\'{emonth}\')) group by alias_all_bid order by ss desc limit 5
                                    '''.format(atbl=atbl, ctbl=ctbl, each_sp1=each_sp1, each_sp9=each_sp9, smonth=smonth,
                                               emonth=emonth, each_source=each_source)
                    bids = [v[0] for v in db.query_all(top_brand_sql)]
                    for each_bid in bids:
                        where = '''
                                alias_all_bid = {each_bid} and c_sp1 = \'{each_sp1}\' and c_sp9=\'{each_sp9}\' and {each_source}
                                '''.format(ctbl=ctbl, each_bid=each_bid, each_sp1=each_sp1, each_sp9=each_sp9,
                                                   each_source=each_source, smonth=smonth, emonth=emonth)
                        ret, sbs = self.cleaner.process_top_aft200(smonth, emonth, limit=5, where=where)
                        sales_by_uuid.update(sbs)
                        rets.append(ret)
                        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                                continue
                            else:
                                uuids_part2.append(uuid2)
                                mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

        sources_dy = ['source = 11']

        for each_sp1 in sp1_list_part2:
            for each_source in sources_dy:
                for each_sp9 in sp9_list_part2:
                    where = '''c_sp1 = \'{each_sp1}\' and c_sp9=\'{each_sp9}\' and {each_source}
                                        '''.format(ctbl=ctbl, each_sp1=each_sp1, each_sp9=each_sp9, each_source=each_source,
                                                   smonth=smonth, emonth=emonth)
                    ret, sbs = self.cleaner.process_top_aft200(smonth, emonth, limit=30, where=where)
                    sales_by_uuid.update(sbs)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            continue
                        else:
                            uuids_part2.append(uuid2)
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

        self.cleaner.add_brush(uuids_part2, clean_flag, 1, sales_by_uuid=sales_by_uuid)

        # Part3

        sp1_list_part3 = ['BB霜',	'安瓶',	'彩妆其它',	'彩妆套装',	'唇彩',	'唇膏',	'防晒',	'粉饼',	'粉底液',	'高光修容',	'隔离霜/妆前',	'护肤品其它',	'护肤套装',	'化妆水',	'睫毛膏',	'精华液',	'精油',	'眉笔眉粉',	'美甲',	'美容仪',	'美妆工具',	'面部磨砂',	'面膜',	'面霜',	'男士乳液面霜',	'男士护肤套装',	'男士化妆水',	'男士洁面',	'男士精华',	'男士其它护肤',	'气垫',	'乳液',	'腮红',	'散粉',	'洗面奶',	'香水',	'卸妆',	'眼唇护理',	'眼线',	'眼影',	'遮瑕',	'男士面膜']
        sp9_list_part3 = ['Prestige']
        uuids_part3 = []

        sources_cdf_lvgou = ['source = 12', 'source in (13,16)']

        for each_sp1 in sp1_list_part3:
            for each_source in sources_cdf_lvgou:
                for each_sp9 in sp9_list_part3:
                    where = '''c_sp1 = \'{each_sp1}\' and c_sp9=\'{each_sp9}\' and {each_source}
                                    '''.format(ctbl=ctbl, each_sp1=each_sp1, each_sp9=each_sp9, each_source=each_source,
                                               smonth=smonth, emonth=emonth)
                    ret, sbs = self.cleaner.process_top_aft200(smonth, emonth, limit=20, where=where)
                    sales_by_uuid.update(sbs)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            continue
                        else:
                            uuids_part3.append(uuid2)
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

        for each_sp1 in sp1_list_part3:
            for each_source in sources_cdf_lvgou:
                for each_sp9 in sp9_list_part3:
                    top_brand_sql = '''select alias_all_bid,sum(sales*sign) as ss from {ctbl} where c_sp1='{each_sp1}' and c_sp9=\'{each_sp9}\' and {each_source} and toYYYYMM(date) >= toYYYYMM(toDate(\'{smonth}\')) AND toYYYYMM(date) < toYYYYMM(toDate(\'{emonth}\')) group by alias_all_bid order by ss desc limit 5
                                    '''.format(atbl=atbl, ctbl=ctbl, each_sp1=each_sp1, each_sp9=each_sp9, smonth=smonth,
                                               emonth=emonth, each_source=each_source)
                    bids = [v[0] for v in db.query_all(top_brand_sql)]
                    for each_bid in bids:
                        where = '''
                                alias_all_bid = {each_bid} and c_sp1 = \'{each_sp1}\' and c_sp9=\'{each_sp9}\' and {each_source}
                                '''.format(ctbl=ctbl, each_bid=each_bid, each_sp1=each_sp1, each_sp9=each_sp9,
                                           each_source=each_source, smonth=smonth, emonth=emonth)
                        ret, sbs = self.cleaner.process_top_aft200(smonth, emonth, limit=5, where=where)
                        sales_by_uuid.update(sbs)
                        rets.append(ret)
                        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                                continue
                            else:
                                uuids_part3.append(uuid2)
                                mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

        self.cleaner.add_brush(uuids_part3, clean_flag, 1, sales_by_uuid=sales_by_uuid)



        return True

    def brush_jidu_new(self, smonth, emonth,logId=-1):
    ###季度出题规则

        sales_by_uuid = {}

        aname, atbl = self.get_a_tbl()
        cname, ctbl = self.get_all_tbl()
        db = self.cleaner.get_db(aname)
        rets = []
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT snum, tb_item_id, p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        brush_p1, filter_p1 = self.cleaner.get_plugin().filter_brush_props()
        data = []
        for snum, tb_item_id, p1, id, visible in ret:
            p1 = brush_p1(snum, p1)
            data.append([snum, tb_item_id, p1, id, visible])

        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in data}

        sp1_lists = ['安瓶',	'防晒',	'护肤品其它',	'护肤套装',	'化妆水',	'精华液',	'精油',	'美容仪',	'面部磨砂',	'面膜',	'面霜',	'男士面膜',	'男士乳液面霜',	'男士护肤套装',	'男士化妆水',	'男士洁面',	'男士精华',	'男士其它护肤',	'乳液',	'洗面奶',	'香水',	'卸妆',	'眼唇护理']
        sp9_lists = ['Mass', 'Prestige']
        # sources = ['source = 1 and (shop_type > 20 or shop_type < 10 )', 'source = 12', 'source in (13,16)']

        sources = ['source = 1 and (shop_type > 20 or shop_type < 10 )']

        uuidsA = []

        for each_sp1 in sp1_lists:
            for each_source in sources:
                for each_sp9 in sp9_lists:
                    where = '''c_sp1 = \'{each_sp1}\' and c_sp9=\'{each_sp9}\' and {each_source}
                                    '''.format(ctbl=ctbl, each_sp1=each_sp1, each_sp9=each_sp9, each_source=each_source,
                                               smonth=smonth, emonth=emonth)
                    ret, sbs = self.cleaner.process_top_aft200(smonth, emonth, limit=20, where=where)
                    sales_by_uuid.update(sbs)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            continue
                        else:
                            uuidsA.append(uuid2)
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

        for each_sp1 in sp1_lists:
            for each_source in sources:
                for each_sp9 in sp9_lists:
                    top_brand_sql = '''select alias_all_bid,sum(sales*sign) as ss from {ctbl} where c_sp1='{each_sp1}' and c_sp9=\'{each_sp9}\' and {each_source} and toYYYYMM(date) >= toYYYYMM(toDate(\'{smonth}\')) AND toYYYYMM(date) < toYYYYMM(toDate(\'{emonth}\')) group by alias_all_bid order by ss desc limit 5
                                    '''.format(atbl=atbl, ctbl=ctbl, each_sp1=each_sp1, each_sp9=each_sp9, smonth=smonth,
                                               emonth=emonth, each_source=each_source)
                    bids = [v[0] for v in db.query_all(top_brand_sql)]
                    for each_bid in bids:
                        where = '''
                                        alias_all_bid = {each_bid} and c_sp1 = \'{each_sp1}\' and c_sp9=\'{each_sp9}\' and {each_source}
                                        '''.format(ctbl=ctbl, each_bid=each_bid, each_sp1=each_sp1, each_sp9=each_sp9,
                                                   each_source=each_source, smonth=smonth, emonth=emonth)
                        ret, sbs = self.cleaner.process_top_aft200(smonth, emonth, limit=5, where=where)
                        sales_by_uuid.update(sbs)
                        rets.append(ret)
                        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                                continue
                            else:
                                uuidsA.append(uuid2)
                                mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

        self.cleaner.add_brush(uuidsA, clean_flag, 1, sales_by_uuid=sales_by_uuid)

        uuidsB = []

        sources_dy = ['source = 11']

        for each_sp1 in sp1_lists:
            for each_source in sources_dy:
                for each_sp9 in sp9_lists:
                    where = '''c_sp1 = \'{each_sp1}\' and c_sp9=\'{each_sp9}\' and {each_source}
                                                '''.format(ctbl=ctbl, each_sp1=each_sp1, each_sp9=each_sp9,
                                                           each_source=each_source,
                                                           smonth=smonth, emonth=emonth)
                    ret, sbs = self.cleaner.process_top_aft200(smonth, emonth, limit=50, where=where)
                    sales_by_uuid.update(sbs)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            continue
                        else:
                            uuidsB.append(uuid2)
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0]

        self.cleaner.add_brush(uuidsB, clean_flag, 1, sales_by_uuid=sales_by_uuid)

        return True


    def hotfix_new(self, tbl, dba, prefix):
        sql = '''
            SELECT DISTINCT sid FROM {} WHERE clean_alias_all_bid IN (
                SELECT IF(alias_bid=0,bid,alias_bid) FROM artificial.all_brand WHERE bid IN (13914,19690,6758,6404,31466,5324,11367,1042268,5316,5586,53191,113650,218449,218619,218498,218573)
            )
        '''.format(tbl)
        ret = dba.query_all(sql)
        ret = [v[0] for v in ret]

        if len(ret) > 0:
            sql = '''
                ALTER TABLE {} UPDATE `sp店铺分类` = 'EDT' WHERE sid IN {}
                SETTINGS mutations_sync = 1
            '''.format(tbl, ret)
            dba.execute(sql)


        sku_table_name = 'sop_c.clean_174_sku_data_0814'

        dba.execute('DROP TABLE IF EXISTS {}_join'.format(sku_table_name))


        sql = '''
            CREATE TABLE {sku_table_name}_join ENGINE = Join(ANY, LEFT, source, item_id, name, `trade_props.name`, `trade_props.value`, `sp三级品类`) AS
            SELECT source, item_id, name, `trade_props.name`, `trade_props.value`, `sp三级品类`, dl_pid, item_info, model_result, top_pid, top_sku, top_similarity, top_confidence, after_brand_link_pid, specification, m_wash, m_sex from {sku_table_name}
        '''.format(sku_table_name=sku_table_name)
        dba.execute(sql)

        sql = '''
            ALTER TABLE {t} UPDATE
            `sp人工机洗SKU` = ifNull(joinGet('{sku_table_name}_join', 'after_brand_link_pid', toString(source), item_id, name, toString(`trade_props.name`), toString(`trade_props.value`), `sp三级品类`), ''),
            `sp出数专用SKU` = if(clean_pid != 0, toString(clean_pid), ifNull(joinGet('{sku_table_name}_join', 'after_brand_link_pid', toString(source), item_id, name, toString(`trade_props.name`), toString(`trade_props.value`), `sp三级品类`), '')),
            `sp人工机洗SKU规格` = ifNull(joinGet('{sku_table_name}_join', 'specification', toString(source), item_id, name, toString(`trade_props.name`), toString(`trade_props.value`), `sp三级品类`), ''),
            `sp出数用SKU规格` = multiIf(`sp辅助_标准库产品规格` != '', `sp辅助_标准库产品规格`, ifNull(joinGet('{sku_table_name}_join', 'after_brand_link_pid', toString(source), item_id, name, toString(`trade_props.name`), toString(`trade_props.value`), `sp三级品类`), '') != '', joinGet('{sku_table_name}_join', 'after_brand_link_pid', toString(source), item_id, name, toString(`trade_props.name`), toString(`trade_props.value`), `sp三级品类`), `sp净含量`),
            `sp人工机洗SKU子品类` = ifNull(joinGet('{sku_table_name}_join', 'm_wash', toString(source), item_id, name, toString(`trade_props.name`), toString(`trade_props.value`), `sp三级品类`), ''),
            `sp人工机洗SKU适用性别` = ifNull(joinGet('{sku_table_name}_join', 'm_sex', toString(source), item_id, name, toString(`trade_props.name`), toString(`trade_props.value`), `sp三级品类`), ''),
            `spSKU子品类` = if(`sp辅助_标准库子品类` != '', `sp辅助_标准库子品类`, ifNull(joinGet('{sku_table_name}_join', 'm_wash', toString(source), item_id, name, toString(`trade_props.name`), toString(`trade_props.value`), `sp三级品类`), '')),
            `spSKU适用性别` = if(`sp辅助_标准库适用性别` != '', `sp辅助_标准库适用性别`, ifNull(joinGet('{sku_table_name}_join', 'm_sex', toString(source), item_id, name, toString(`trade_props.name`), toString(`trade_props.value`), `sp三级品类`), ''))
            WHERE uuid2 != 0
        '''.format(t=tbl, sku_table_name=sku_table_name)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        dba.execute('DROP TABLE IF EXISTS {}_join'.format(sku_table_name))

        sku_name = 'artificial.sku_name_91363'
        dba.execute('DROP TABLE IF EXISTS {}'.format(sku_name))
        sql = '''
            CREATE TABLE {sku_name} ENGINE = Join(ANY, LEFT, pid_s) AS
            SELECT toString(pid) pid_s, name from artificial.product_91363
        '''.format(sku_name=sku_name)
        dba.execute(sql)
        sql = '''
            ALTER TABLE {t} UPDATE
            `sp出数专用SKU名` = ifNull(joinGet('{sku_name}', 'name', `sp出数专用SKU`), '')
            where uuid2 != 0
        '''.format(t=tbl, sku_name=sku_name)
        dba.execute(sql)
        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        dba.execute('DROP TABLE IF EXISTS {}'.format(sku_name))


    def process_exx(self, tbl, prefix, logId=0):
        self.cleaner.update_aliasbid(tbl, self.cleaner.get_db('chsop'))