import sys
import time
import datetime
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')
import models.plugins.batch as Batch
import application as app


class main(Batch.main):

    def brush_modify(self, v, bru_items):
        mp = {
            '肤色':'sp21', '灰':'sp22', '黑':'sp23', '白':'sp24', '绿':'sp25', '黄':'sp26', '蓝':'sp27',
            '紫':'sp28', '红':'sp29', '棕':'sp30', '粉':'sp31', '青':'sp32', '其它':'sp33'
        }
        for vv in v['split_pids']:
            sp18 = vv['sp18'].split('Ծ‸ Ծ')
            sp18 = [vvv.strip() for vvv in sp18 if vvv.strip() in mp]
            sp18.sort()
            sp9  = int(vv['sp9'] or 1)
            for c in mp:
                vv[mp[c]] = '{}/{}'.format(sp9, len(sp18)) if c in sp18 else '空'
            vv['sp34'] = str(len(sp18))
            vv['sp18'] = ' & '.join(sp18)


    def brush_old_quartly(self, smonth, emonth, logId=-1):
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible, pid FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4], v[5]] for v in ret}

        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()
        db = self.cleaner.get_db(aname)

        sales_by_uuid = {}
        cc = {}

        sp5s = ['普通哺乳文胸', '免手扶式哺乳文胸']

        for ssmonth, eemonth in [['2022-01-01', '2022-04-01'], ['2022-04-01', '2022-07-01']]: ##, ['2020-07-01', '2020-10-01'], ['2020-10-01', '2021-01-01'], ['2021-01-01', '2021-04-01'], ['2021-04-01', '2021-07-01'], ['2021-07-01', '2021-10-01'], ['2021-10-01', '2022-01-01']]:
        # for ssmonth, eemonth in [['2021-10-01', '2022-01-01']]:
            for each_sp5 in sp5s:
                uuids = []
                top_brand_sql = '''
                select alias_all_bid,sum(sales*sign) as ss from {atbl} where uuid2 in (select uuid2 from {ctbl} where c_sp5='{each_sp5}' and c_sp1 = '哺乳文胸' and pkey>=\'{ssmonth}\' and pkey<\'{eemonth}\') and pkey>=\'{ssmonth}\' and pkey<\'{eemonth}\' group by alias_all_bid order by ss desc limit 11
                '''.format(atbl=atbl, ctbl=ctbl, each_sp5=each_sp5, ssmonth=ssmonth, eemonth=eemonth)
                bids = [v[0] for v in db.query_all(top_brand_sql)]
                for each_bid in bids:
                    where = '''
                    uuid2 in (select uuid2 from {ctbl} where alias_all_bid = {each_bid} and c_sp5='{each_sp5}' and c_sp1 = '哺乳文胸' and pkey>=\'{ssmonth}\' and pkey<\'{eemonth}\')
                    '''.format(ctbl=ctbl, each_bid=each_bid, each_sp5=each_sp5, ssmonth=ssmonth, eemonth=eemonth)
                    ret, sales_by_uuid1 = self.cleaner.process_top_anew(ssmonth, eemonth, where=where, limit=12)
                    sales_by_uuid.update(sales_by_uuid1)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            continue
                        else:
                            uuids.append(uuid2)
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0, 0]
                cc['{}+{}+{}'.format(ssmonth, eemonth, each_sp5)] = [len(uuids)]
                self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=1, sales_by_uuid=sales_by_uuid)

        for i in cc:
            print(i, cc[i])

        return True

    ### 200 以后新接口默认出题
    def brush(self, smonth, emonth, logId=-1):
        clean_flag = self.cleaner.last_clean_flag() + 1

        sql = 'SELECT snum, tb_item_id, p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        brush_p1, filter_p1 = self.cleaner.get_plugin().filter_brush_props()
        data = []
        for snum, tb_item_id, p1, id, visible in ret:
            p1 = brush_p1(snum, p1)
            data.append([snum, tb_item_id, p1, id, visible])

        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in data}

        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()
        db = self.cleaner.get_db(aname)

        sales_by_uuid = {}
        cc = {}

        sp5s = ['普通哺乳文胸', '免手扶式哺乳文胸']

        for ssmonth, eemonth in [['2022-01-01', '2022-04-01'], ['2022-04-01', '2022-07-01']]: ##, ['2020-07-01', '2020-10-01'], ['2020-10-01', '2021-01-01'], ['2021-01-01', '2021-04-01'], ['2021-04-01', '2021-07-01'], ['2021-07-01', '2021-10-01'], ['2021-10-01', '2022-01-01']]:
        # for ssmonth, eemonth in [['2021-10-01', '2022-01-01']]:
            for each_sp5 in sp5s:
                uuids = []
                top_brand_sql = '''
                select alias_all_bid,sum(sales*sign) as ss from {ctbl}  where c_sp5='{each_sp5}' and c_sp1 = '哺乳文胸' and toYYYYMM(date) >= toYYYYMM(toDate('{ssmonth}')) AND toYYYYMM(date) < toYYYYMM(toDate('{eemonth}')) group by alias_all_bid order by ss desc limit 11
                '''.format(ctbl=ctbl, each_sp5=each_sp5, ssmonth=ssmonth, eemonth=eemonth)
                bids = [v[0] for v in db.query_all(top_brand_sql)]
                for each_bid in bids:
                    where = '''
                    alias_all_bid = {each_bid} and c_sp5='{each_sp5}' and c_sp1 = '哺乳文胸'
                    '''.format(each_bid=each_bid, each_sp5=each_sp5, ssmonth=ssmonth, eemonth=eemonth)
                    ret, sales_by_uuid1 = self.cleaner.process_top_aft200(ssmonth, eemonth, where=where, limit=12)
                    sales_by_uuid.update(sales_by_uuid1)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            continue
                        else:
                            uuids.append(uuid2)
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 0, 0]
                cc['{}+{}+{}'.format(ssmonth, eemonth, each_sp5)] = [len(uuids)]
                self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=1, sales_by_uuid=sales_by_uuid)

        for i in cc:
            print(i, cc[i])

        return True