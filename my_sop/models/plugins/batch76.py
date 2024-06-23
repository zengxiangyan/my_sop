import sys
import time
from pypinyin import lazy_pinyin
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app
import re

class main(Batch.main):

    def calc_sp(self, v, is_brush=False):
        sp3a, sp3b = [], []
        sp4, sp5, sp18 = 0, 0, []
        if v['sp6'] == '删除':
            return
        for vv in v['sp31'].split('+'):
            vvv = vv.split('*')
            c = 1
            index = 1
            while len(vvv) > index:
                c = c*int(vvv[index])
                index += 1
            ml = vvv[0].replace('ml','')
            if (ml.isdigit() and (True if isinstance(c,int) else c.isdigit())):
                ml, c = float(ml), int(c)
                sp4 += c
                sp5 += ml * c
                if c == 1:
                    sp18.append(ml)
                sp3a.append(ml)
                sp3b.append(c)
        sp18 = 1 if len(list(set(sp18))) != len(sp18) else 0

        sp7 = ''
        if v['sp6'] == 'Soft MPS':
            if len(list(set(sp3a))) == 1:
                sp7 = '单瓶装' if sp4 == 1 else '促销装（单瓶多瓶）'
            else:
                sp7 = '促销装（大+小）'

        sp15 = '其它'
        if v['sp6'] == 'Soft MPS' and v['sp16'] != '空' and v['sp16'] != '':
            h1, h2 = False, False
            for ii, ml in enumerate(sp3a):
                if ml > 30:
                    h1 = True
                if ml <=30:
                    if v['alias_all_bid'] in (1074902,5696731,6207191,19039) and sp3b[ii] >= 5:
                        h2 = False
                    else:
                        h2 = True
            if h1 and h2:
                sp15 = '是'

        sp17 = ''
        if sp5 >= 900:
            sp17 = '900ml+'
        elif sp5 >= 700:
            sp17 = '700ml-899ml'
        elif sp5 >= 500:
            sp17 = '500ml-699ml'
        elif sp5 >= 300:
            sp17 = '300ml-499ml'
        elif sp5 > 100:
            sp17 = '101ml-299ml'
        else:
            sp17 = '≤100ml'

        v['sp16'] = v['sp16'] or '空'

        sp19 = ''
        if v['sp6'] == 'Soft MPS' and v['sp16'] != '空':
            sp16a, sp16b = [], 0
            for vv in v['sp16'].split('+'):
                vvv = vv.split('*')
                ml, c = vvv[0].replace('ml',''), 1 if len(vvv)==1 else vvv[1]
                ml, c = float(ml), int(c)
                sp16a.append(ml)
                sp16b += c

            if len(list(set(sp16a))) == 1:
                sp19 = '单瓶装' if sp16b == 1 else '促销装（单瓶多瓶）'
            else:
                sp19 = '促销装（大+小）'

        v['sp4'] = '空' if is_brush and sp4 == '' else str(sp4)
        v['sp5'] = '空' if is_brush and sp5 == '' else str(sp5)
        v['sp7'] = '空' if is_brush and sp7 == '' else sp7
        v['sp15'] = '空' if is_brush and sp15 == '' else sp15
        v['sp17'] = '空' if is_brush and sp17 == '' else sp17
        v['sp18'] = '空' if is_brush and sp18 == '' else str(sp18)
        v['sp19'] = '空' if is_brush and sp19 == '' else sp19

        bids = [
            [48200,52390],[469419,52143],[380580,201237],[380584,3911904],[3312877,431917],[3456984,431917],[4606866,52390],[17696,52390],[3616797,311028],[629691,311028],[5905750,311028],[3158551,52390],[380535,52143],[19039,1074902],[5696731,1074902],[6207191,1074902],[289060,52390],[2281067,431917],[431984,243393],[6385378,3312878]
        ]
        for obid, nbid, in bids:
            if str(obid) == str(v['alias_all_bid']):
                v['alias_all_bid'] = v['all_bid'] = nbid

        if str(v['alias_all_bid']) == '1820677':
            v['sp20'], v['sp21'] = '欧朗睛', '欧朗睛'
        elif str(v['alias_all_bid']) == '311028':
            v['sp20'], v['sp21'] = '库博', 'COOPER'
        else:
            info = self.get_allbrand_info(v['alias_all_bid'])
            for k in ['name_cn_front', 'name_en_front', 'name']:
                if info[k] != '':
                    v['sp20'] = info[k]
                    break
            if info['name_en_front'] != '':
                v['sp21'] = info['name_en_front']
            elif str(v['alias_all_bid']) == '243393':
                v['sp21'] = 'B+L'
            elif str(v['alias_all_bid']) == '130598':
                v['sp21'] = 'Hydron'
            elif str(v['alias_all_bid']) == '52390':
                v['sp21'] = 'J&J'
            elif str(v['alias_all_bid']) == '201237':
                v['sp21'] = 'Alcon'
            elif str(v['alias_all_bid']) == '5696731':
                v['sp21'] = 'Penta Plex'
            else:
                py = lazy_pinyin(v['sp20'])
                v['sp21'] = ' '.join([p.capitalize() for p in py])

        if v['alias_all_bid'] in ['243393','52143','130598','176723','109463','52390','1074902','201237','243071','677737']:
            v['sp22'] = '1'
        else:
            v['sp22'] = '0'


    def brush_modify(self, v, bru_items):
        p = re.compile('([0-9]+)ml')
        spxx = []
        for vv in v['split_pids']:
            sp3 = vv['sp3']
            volumes = re.findall(p, sp3)
            if len(volumes) > 0:
                volumes = [int(i) for i in volumes]
                max_v = max(volumes)
                vv['sp14'] = str(max_v)
            if vv['sp3'] != '':
                vv['sp33'] = '{}*{}'.format(vv['sp3'].replace('ml',''), vv['number'])
                vv['sp33'] = '{:g}'.format(round(eval(vv['sp33']),1))
                if vv['number'] > 1:
                    vv['sp3'] = '{}*{}'.format(vv['sp3'], vv['number'])
                spxx.append([vv['sp8'], vv['sp3'], vv['sp29']])

        pack_name = []
        for i, vv in enumerate(v['split_pids']):
            vv['sp30'] = '+'.join([s[0] for s in spxx if s!=''])
            vv['sp31'] = '+'.join([s[1] for s in spxx if s!=''])
            vv['sp32'] = '+'.join([s[2] for s in spxx if s!=''])
            if vv['sp33'] == '':
                vv['sp34'] = ''
            elif float(vv['sp33']) >= 900:
                vv['sp34'] = '900ml+'
            elif float(vv['sp33']) >= 700:
                vv['sp34'] = '700ml-899ml'
            elif float(vv['sp33']) >= 500:
                vv['sp34'] = '500ml-699ml'
            elif float(vv['sp33']) >= 300:
                vv['sp34'] = '300ml-499ml'
            elif float(vv['sp33']) >= 100:
                vv['sp34'] = '101ml-299ml'
            else:
                vv['sp34'] = '≤100ml'
            self.calc_sp(vv, is_brush=True)
            if vv['sp3'] != '':
                spxx[i].append(vv['sp6'])

        spxx.sort(key=lambda x: eval(x[1].split('*')[0].replace('ml','')), reverse=True)

        for vv in v['split_pids']:
            vv['sp30'] = '+'.join([s[0] for s in spxx if s!=''])
            vv['sp31'] = '+'.join([s[1] for s in spxx if s!=''])
            vv['sp32'] = '+'.join([s[2] for s in spxx if s!=''])
            vv['sp35'] = '+'.join([s[3] for s in spxx if s!=''])

    def skip_helper_76(self, smonth, emonth, uuids):

        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()
        db = self.cleaner.get_db(aname)

        # 找出当期之前所有出过的题
        # sql = '''select uuid2 from {tbl}  where (snum, tb_item_id, real_p1,month)
        # in (select snum, tb_item_id, real_p1, max(month) max_ from {tbl}  where pkey<'{smonth}'  group by snum, tb_item_id, real_p1)  and length(uuid2)>1
        # '''.format(tbl= self.cleaner.get_tbl(), smonth=smonth)
        sql = '''select concat(snum, tb_item_id, real_p1) from {tbl}  where (snum, tb_item_id, real_p1,month)
                in (select snum, tb_item_id, real_p1, max(month) max_ from {tbl}  where pkey<'{smonth}'  group by snum, tb_item_id, real_p1)
                '''.format(tbl=self.cleaner.get_tbl(), smonth=smonth)
        ret = self.cleaner.db26.query_all(sql)
        # map_0 = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}
        map = [v[0] for v in ret]

        # 找出当期前出题的最近一条机洗结果
        # sql_result_old_uuid = '''
        # select a.*,b.sp3,b.sp8 from
        # (WITH arrayReduce('BIT_XOR',arrayMap(x->crc32(x),arrayFilter(y->trim(y)<>'',arrayReduce('groupUniqArray',trade_props.value)))) AS p1
        # SELECT source, item_id, p1,uuid FROM {atbl}
        # WHERE pkey  < '{smonth}'
        # GROUP BY source, item_id, p1,uuid) a
        # JOIN
        # (select * from {ctbl}) b
        # on a.uuid2=b.uuid2
        # where a.uuid2 in({uuids})
        # '''.format(atbl=atbl, ctbl=ctbl, smonth=smonth, uuids=','.join(["'"+str(v)+"'" for v in map]))

        sql_result_old_uuid = '''
        select a.*,b.sp3,b.sp8 from
        (WITH arrayReduce('BIT_XOR',arrayMap(x->crc32(x),arrayFilter(y->trim(y)<>'',arrayReduce('groupUniqArray',trade_props.value)))) AS p1
        SELECT source, item_id, p1,uuid2 FROM {atbl}
        WHERE pkey  < '{smonth}'
        GROUP BY source, item_id, p1,uuid2) a
        JOIN
        (select * from {ctbl}) b
        on a.uuid2=b.uuid2
        where concat(toString(a.source),toString(a.item_id),toString(a.p1)) in({ids})
        '''.format(atbl=atbl, ctbl=ctbl, smonth=smonth, ids=','.join(["'" + str(v) + "'" for v in map]))

        map_help_old = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[4], v[5]] for v in db.query_all(sql_result_old_uuid)}

        # 当期符合需求的所有原始uuid的机洗结果
        sql_result_new_uuid = '''
         select a.*,b.sp3,b.sp8 from
        (WITH arrayReduce('BIT_XOR',arrayMap(x->crc32(x),arrayFilter(y->trim(y)<>'',arrayReduce('groupUniqArray',trade_props.value)))) AS p1
        SELECT source, item_id, p1, uuid2 FROM {atbl}
        WHERE pkey  >= '{smonth}' and pkey  < '{emonth}' AND sales > 0 AND num > 0
        AND uuid2 NOT IN (SELECT uuid2 FROM {atbl}  WHERE pkey  >= '{smonth}' and pkey  < '{emonth}' AND sign = -1)
        ) a
        JOIN
        (select * from {ctbl}) b
        on a.uuid2=b.uuid2
        where a.uuid2 in ({uuids})
        '''.format(atbl=atbl, ctbl=ctbl, smonth=smonth, emonth=emonth,
                   uuids=','.join(["'" + str(v) + "'" for v in uuids]))

        map_help_new = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[4], v[5], v[3]] for v in
                        db.query_all(sql_result_new_uuid)}

        # 当期之后出的题目（包括当期）
        sql2 = "SELECT distinct snum, tb_item_id, real_p1, id, visible FROM {} where pkey >='{}'  ".format(
            self.cleaner.get_tbl(), smonth)
        ret2 = self.cleaner.db26.query_all(sql2)
        map2 = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret2}

        new_uuids = []
        old_uuids_update = []

        for new_key, new_value in map_help_new.items():
            # 当期之后有出题（包括当期），则忽略
            if new_key in map2.keys():
                continue
            else:
                # 当期的新题
                if new_key not in map_help_old.keys():
                    new_uuids.append(str(new_value[2]))
                else:
                    # 之前有出过题，但某些重要机洗值改变，重新出题
                    for old_key, old_value in map_help_old.items():
                        if new_key == old_key and (new_value[0] != old_value[0] or new_value[1] != old_value[1]):
                            old_uuids_update.append(str(new_value[2]))
                        else:
                            continue

        return new_uuids, old_uuids_update, map_help_new


    def brush(self, smonth, emonth, logId=-1):

        clean_flag = self.cleaner.last_clean_flag() + 1

        # cname, ctbl = self.get_c_tbl()
        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()



        # category为"Soft MPS","Eye Drops"

        alias_sp = "select distinct alias_all_bid from sop_c.entity_prod_90620_ALL \
                    where alias_all_bid in('52143','52390','109463','130598','176723','201237','243071','243393','6860772','311028','2180288','2879494','6153805')\
                    or c_all_bid in('52143','52390','109463','130598','176723','201237','243071','243393','6860772','311028','2180288','2879494','6153805')"

        mpp = {}
        sql = 'select snum, tb_item_id, real_p1 from {} order by date_format(modified,"%Y%m") desc, id desc'.format(
            self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        for i, element in enumerate(ret):
            mpp['{}-{}-{}'.format(element[0], element[1], element[2])] = i
        where = '''
                (source=1 and (shop_type>20 or shop_type<10))
                and uuid2 in(select uuid2 from {ctbl} where pkey>='{smonth}' and pkey<'{emonth}' and c_sp26 !='删除' and c_sp6 in ('Soft MPS','Eye Drops') and alias_all_bid in ({alias_sp}))
                '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth, alias_sp=alias_sp)
        uuids = []
        order = {}
        uuid_by_key = {}
        ret, sales_by_uuid = self.cleaner.process_top_anew(smonth, emonth, where=where)
        for uuid2, source, tb_item_id, p1, cid, bid in ret:
            uuids.append(uuid2)
            key = '{}-{}-{}'.format(source, tb_item_id, p1)
            if key in mpp:
                order[key] = mpp[key]
                uuid_by_key[key] = uuid2
                continue
            mpp[key] = True
        temp = sorted(order.items(), key=lambda x: x[1])
        limit = int(len(order) / 4)
        uuids2 = [uuid_by_key[v[0]] for v in temp[0:limit]]

        # self.cleaner.add_brush(uuids2, clean_flag, visible_check=1, visible=1, sales_by_uuid=sales_by_uuid)


        new_uuids, old_uuids_update, map_help_new = self.skip_helper_76(smonth, emonth, uuids)
        self.cleaner.add_brush(old_uuids_update+new_uuids, clean_flag, visible_check=1, visible=1, sales_by_uuid=sales_by_uuid)

        uuids3 = []
        where = '''
        (source=1 and (shop_type>20 or shop_type<10))
        and uuid2 in(select uuid2 from {ctbl} where pkey>='{smonth}' and pkey<'{emonth}' and c_sp6 in ('Hard MPS','Others','双氧水','Eye Drops(Hard)') and c_sp3 like '%+%' and c_sp26 !='删除')
        '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth)
        ret, sales_by_uuid = self.cleaner.process_top_anew(smonth, emonth, where=where)
        for uuid2, source, tb_item_id, p1, cid, bid in ret:
            uuids3.append(uuid2)

        self.cleaner.add_brush(uuids3, clean_flag, visible_check=1, visible=1, sales_by_uuid=sales_by_uuid)

        sql2 = 'SELECT snum, tb_item_id, real_p1, id, visible, pid FROM {}'.format(self.cleaner.get_tbl())
        ret2 = self.cleaner.db26.query_all(sql2)
        mpp2 = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4], v[5]] for v in ret2}

        uuids3 = []
        where = '''
               (source=1 and (shop_type>20 or shop_type<10))
               and uuid2 in(select uuid2 from {ctbl} where pkey>='{smonth}' and pkey<'{emonth}' and c_sp6 in ('Hard MPS','Others','双氧水','Eye Drops(Hard)') and c_sp3 not like '%+%' and c_sp26 !='删除')
               '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth)
        ret, sales_by_uuid = self.cleaner.process_top_anew(smonth, emonth, where=where)
        for uuid2, source, tb_item_id, p1, cid, bid in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp2:
                continue
            else:
                uuids3.append(uuid2)

        # new_uuids2, old_uuids_update2, map_help_new2 = self.skip_helper_76(smonth, emonth, uuids3)
        #
        # self.cleaner.add_brush(new_uuids2+old_uuids_update2, clean_flag, visible_check=1, visible=1, sales_by_uuid=sales_by_uuid)
        self.cleaner.add_brush(uuids3, clean_flag, visible_check=1, visible=1, sales_by_uuid=sales_by_uuid)

        # print('uuids2---', len(uuids2), '  new_uuids--', len(new_uuids), '  old_uuids_update--', len(old_uuids_update), '  new_uuids2--', len(new_uuids2),'  old_uuids_update2--',  len(old_uuids_update2))

        # cname, ctbl = self.get_c_tbl()
        # aname, atbl = self.get_a_tbl()
        # db = self.cleaner.get_db(aname)
        # sales_by_uuid = {}
        # cc = {}

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        uuids = []
        uuid_real_new = []
        where = '''
                uuid2 in(select uuid2 from {ctbl} where pkey>='{smonth}' and pkey<'{emonth}' and c_sp3 in('10ml','10ml*2','2ml') and alias_all_bid in (2879494))  and source =1 and (shop_type>20 or shop_type<10)
                '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth)
        ret, sales_by_uuid = self.cleaner.process_top_anew(smonth, emonth, where=where)
        for uuid2, source, tb_item_id, p1, cid, bid in ret:
            uuids.append(uuid2)
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            else:
                uuid_real_new.append(uuid2)

        print(len(uuid_real_new))
        self.cleaner.add_brush(uuid_real_new, clean_flag, visible_check=1, visible=5, sales_by_uuid=sales_by_uuid)

        sql = 'SELECT snum, tb_item_id, real_p1, id, visible FROM {}'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in ret}

        uuids = []
        uuid_real_new = []
        where = '''
                                uuid2 in(select uuid2 from {ctbl} where pkey>='{smonth}' and pkey<'{emonth}' and c_sp26 !='删除' and c_sp6 in ('Soft MPS','Eye Drops','双氧水','Eye Drops(Hard)','Hard MPS','Others') and alias_all_bid in (6570904, 431917))  and source =1 and (shop_type>20 or shop_type<10)
                                '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth)
        ret, sales_by_uuid = self.cleaner.process_top_anew(smonth, emonth, where=where)
        for uuid2, source, tb_item_id, p1, cid, bid in ret:
            uuids.append(uuid2)
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            else:
                uuid_real_new.append(uuid2)

        print(len(uuid_real_new))
        self.cleaner.add_brush(uuid_real_new, clean_flag, visible_check=1, visible=5, sales_by_uuid=sales_by_uuid)

        self.cleaner.set_default_val(type=1)

        return True

    def hotfix_new(self, tbl, dba, prefix):
        self.cleaner.add_miss_cols(tbl, {'spcheck-是否人工':'String'})
        sql = '''
            ALTER TABLE {} UPDATE `spcheck-是否人工` = multiIf(
                `sp是否人工答题`='否','非人工',
                `sp是否人工答题`='出题宝贝','人工',
                '人工刷的'
            )
            WHERE 1
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

    # 导旧答题
    # TRUNCATE TABLE artificial_new.`entity_90620_item`;
    # INSERT INTO artificial_new.`entity_90620_item` (
    # id,tb_item_id,source,month,name,sku_name,sku_url,sid,shop_name,shop_type,cid,real_cid,region_str,brand,all_bid,alias_all_bid,super_bid,sub_brand,sub_brand_name,product,prop_all,avg_price,trade,num,sales,visible,p1,real_p1,clean_type,clean_flag,visible_check,
    # spid6,spid8,spid3,spid5,spid4,spid17,spid7,spid18,spid16,spid12,spid19,spid1,spid2,spid9,spid10,spid11,spid13,spid14,spid15,spid20,spid21,spid22,spid23,spid24,spid25,spid26,spid27,spid28,
    # pid,batch_id,flag,uid,check_uid,b_check_uid,tip,img,is_set,created,modified,number
    # ) SELECT id,tb_item_id,source,month,name,'','',sid,shop_name,shop_type,cid,real_cid,region_str,brand,all_bid,alias_all_bid,super_bid,sub_brand,sub_brand_name,product,prop_all,avg_price,trade,num,sales,visible,p1,0,0,0,visible_check,
    # spid1,spid2,spid5,spid6,spid7,spid8,if(spid9 IS NULL,'',spid9),spid10,spid11,spid12,if(spid13 IS NULL,'',spid13),'','','','','','','','','','','','','','','','','',
    # pid,batch_id,flag,uid,0,0,1,img,is_set,created,modified,number
    # FROM artificial_new.entity_90004_item WHERE flag > 0 AND uid > 0 AND month>='2020-04-01';

    # /bin/mysqldump -h 192.168.128.18 -ucleanAdmin -p6DiloKlm artificial_new entity_90620_item > /obsfs/90620.sql
    # /bin/mysql -h 192.168.30.93 -ucleanAdmin -p6DiloKlm product_lib_ali < /obsfs/90620.sql

    # ALTER TABLE product_lib_ali.entity_90620_item ADD COLUMN trade_prop_all text NOT NULL AFTER product;

    # CREATE TABLE product_lib_hw.entity_90620_item LIKE product_lib.entity_90620_item;
    # p3 scripts/conver_brush_ali2hw.py
