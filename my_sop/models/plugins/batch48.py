import sys
import time
import functools
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app

class main(Batch.main):
    # 每月11号跑 p3 scripts/clean_import_brush_new.py -b 48 --process --start_month='2020-04-01' --end_month='2021-02-01'
    def brush(self, smonth, emonth, logId=-1):
        tbl = self.get_entity_tbl()

        sql = 'SELECT bid FROM brush.all_brand WHERE alias_bid IN (5175532, 210) OR bid IN (5175532, 210)'
        ret = self.cleaner.db26.query_all(sql)
        bids = ','.join([str(v) for v, in ret])

        uuids = []
        ret, sales_by_uuid = self.cleaner.process_top_anew(smonth, emonth, where='all_bid IN ({})'.format(bids))
        print('ret',len(ret))
        for uuid2, source, item_id, p1, cid, alias_bid in ret:
            if self.skip_brush(source, item_id, p1):
                continue
            uuids.append(uuid2)

        clean_flag = self.cleaner.last_clean_flag() + 1
        self.cleaner.add_brush(uuids, clean_flag, 1, sales_by_uuid=sales_by_uuid)
        #
        print('add new brush {}'.format(len(uuids)))
        return True

        # select clean_flag, date_format(month, '%Y%m') m, count(*) from product_lib.entity_90501_item group by clean_flag, m;

    # def brush_similarity_test(self, v1, v2):
    #     if str(v1['real_p1']) != str(v2['p1']):
    #         return 0

    #     pa, pb = v1['sales']/v1['num'], v2['sales']/v2['num']
    #     if abs((pa-pb)/pb*100)%100 > 30:
    #         print(11111, pa, pb, (pa-pb)/pb)
    #         print(v1, v2)
    #         exit()
    #         return 0

    #     if str(v1['pkey']) != str(v2['pkey']):
    #     # # if str(v1['uuid2']) != str(v2['uuid2']):
    #         return 1
    #     return 2


    def hotfix(self, tbl, dba, prefix, params):
        super().hotfix(tbl, dba, prefix, params)
        # 蓝牙耳机不洗子品类
        sql = '''
            ALTER TABLE {} UPDATE `c_props.value` = arrayMap((k, v) -> IF(k='子品类', '', v), c_props.name, c_props.value)
            WHERE cid = 50005050
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        # 罗技 游戏 分到 罗技G
        sql = '''
            ALTER TABLE {} UPDATE `c_all_bid` = 5175532, `c_alias_all_bid` = 5175532, `c_props.value` = arrayMap((k, v) -> IF(k='品牌', '罗技G', v), c_props.name, c_props.value)
            WHERE c_props.value[indexOf(c_props.name, '子品类')] = '游戏' and c_alias_all_bid = 210
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)


    def csv(self, mid, smonth, emonth):
        tbl = 'artificial_new.entity_{}'.format(mid)
        self.sheet1(tbl, smonth, emonth)
        cids = self.sheet2(tbl, smonth, emonth)
        self.sheet3(tbl, smonth, emonth, cids)
        self.sheet4(tbl, smonth, emonth)


    def sheet1(self, tbl, smonth, emonth):
        db18 = app.get_db('18_apollo')
        db18.connect()

        data = []
        sql = '''
            select sp1, source, if(sp5='综合', '综合', alias_all_bid), sum(sales)/100 ss, sum(num)
            from {} where date >= '{}' and date < '{}' and sp1 != ''
            group by sp1, source, if(sp5='综合', '综合', alias_all_bid) order by ss desc
        '''.format(tbl, smonth, emonth)
        ret = db18.query_all(sql)

        for sp1, source, alias_all_bid, ssales, snum in ret:
            alias_all_bid = str(alias_all_bid, encoding='utf-8')
            d = [
                smonth, source, sp1, main.get_brand(db18, alias_all_bid), ssales, snum
            ]
            data.append(d)
        data.sort(key=functools.cmp_to_key(main.cmp1))

        fields = ['时间', '平台', '子品类', '品牌', '销售额', '销量']
        filedir = 'batch{} {}{}~{}{}'.format(self.cleaner.bid, smonth.split('-')[0], smonth.split('-')[1], emonth.split('-')[0], emonth.split('-')[1])
        self.export_csv(filedir, '大盘数据.csv', [fields]+data)


    def sheet2(self, tbl, smonth, emonth):
        db18 = app.get_db('18_apollo')
        db18.connect()

        data = []
        sql = '''
            select sp1, source, if(sp5='综合', '综合', alias_all_bid), sum(sales)/100 ss, sum(num)
            from {} where alias_all_bid in (210, 5175532, 108101, 306) and date >= '{}' and date < '{}' and sp1 != ''
            group by sp1, source, if(sp5='综合', '综合', alias_all_bid) order by ss desc
        '''.format(tbl, smonth, emonth)
        ret = db18.query_all(sql)

        miss_bids = [210, 5175532, 108101, 306, '综合']
        miss_sp1s = ['键盘', '有线鼠标', '无线鼠标', '键鼠套装', '摄像头', '耳机/耳麦', '麦克风', '翻页器', '游戏', '电脑配件']
        miss_map  = {}
        for sp1 in miss_sp1s:
            for bid in miss_bids:
                miss_map['{}-{}'.format(sp1, bid)] = False

        for sp1, source, alias_all_bid, ssales, snum in ret:
            alias_all_bid = str(alias_all_bid, encoding='utf-8')
            d = [
                smonth, source, sp1, main.get_brand(db18, alias_all_bid), ssales, snum
            ]
            data.append(d)
            miss_map['{}-{}'.format(sp1, alias_all_bid)] = True

        for k in miss_map:
            if miss_map[k] == False:
                sp1, bid = k.split('-')
                d = [
                    smonth, 'tmall', sp1, main.get_brand(db18, bid), 0, 0
                ]
                data.append(d)

        dbch = app.get_clickhouse('chmaster')
        dbch.connect()
        chsql = app.connect_clickhouse_http('chsql')

        sql = 'SELECT alias_bid, bid FROM ali.brand_has_alias_bid_month WHERE alias_bid in(210, 5175532, 108101, 306)'
        ret = dbch.query_all(sql)
        brands = {str(v[1]):v[0] for v in ret}
        sql = '''
            SELECT gCid() as g_cid, gBid() as g_bid, sales_total/100 as c, num_total as d
            FROM ali.trade
            WHERE w_start_date('{}') AND w_end_date_exclude('{}') AND bidsIn({brand}) AND platformsIn('tmall')
              AND not cidsIn(0, 110210,50012307,50012320,50002415,110508,50003850,1205,50024944,124254004,50003318,50018921,50012068,50012079,50012080,50001810,121616,50024116,50024117,50024121 )
            group by g_cid, g_bid
        '''.format(smonth, emonth, brand=','.join(brands.keys()))
        ret = chsql.query_all(sql)

        # sql = '''
        #     SELECT cid, brand, sum(sales*sign)/100, sum(num*sign)
        #     FROM ali.stat_trade_all
        #     WHERE date >= '{}' AND date < '{}' AND pkey in('{y}-{m}-21','{y}-{m}-22','{y}-{m}-23','{y}-{m}-24','{y}-{m}-25') AND brand in ({brand})
        #       AND cid not in ( 0, 110210,50012307,50012320,50002415,110508,50003850,1205,50024944,124254004,50003318,50018921,50012068,50012079,50012080,50001810,121616,50024116,50024117,50024121 )
        #     GROUP BY cid, brand
        # '''.format(smonth, emonth, y=smonth.split('-')[0], m=smonth.split('-')[1], brand=','.join(brands.keys()))
        # ret = dbch.query_all(sql)

        cids = []
        for v in ret:
            cid, brand, ssales, snum = v['g_cid'], v['g_bid'], v['c'], v['d']
            d = [
                smonth, 'tmall', '*其他类目 '+main.get_category(db18, cid), main.get_brand(db18, brands[str(brand)]), ssales, snum
            ]
            data.append(d)
            cids.append(str(cid))

        data.sort(key=functools.cmp_to_key(main.cmp1))

        fields = ['时间', '平台', '子品类', '品牌', '销售额', '销量']
        filedir = 'batch{} {}{}~{}{}'.format(self.cleaner.bid, smonth.split('-')[0], smonth.split('-')[1], emonth.split('-')[0], emonth.split('-')[1])
        self.export_csv(filedir, '罗技大盘列表.csv', [fields]+data)
        return cids

    def sheet3(self, tbl, smonth, emonth, cids):
        db18 = app.get_db('18_apollo')
        db18.connect()
        front = app.get_db('front')
        front.connect()
        dbch = app.get_clickhouse('chmaster')
        dbch.connect()
        chsql = app.connect_clickhouse_http('chsql')

        sql = '''
            SELECT gCid() as g_cid, gBid() as g_bid, sales_total/100 as c, num_total as d
            FROM ali.trade
            WHERE w_start_date('{}') AND w_end_date_exclude('{}') AND cidsIn({}) AND platformsIn('tmall')
            group by g_cid, g_bid
        '''.format(smonth, emonth, ','.join(cids))
        ret = chsql.query_all(sql)

        # sql = '''
        #     SELECT cid, brand, sum(sales*sign)/100, sum(num*sign)
        #     FROM ali.stat_trade_all
        #     WHERE date >= '{}' AND date < '{}' AND pkey in('{y}-{m}-21','{y}-{m}-22','{y}-{m}-23','{y}-{m}-24','{y}-{m}-25') AND cid in ({})
        #     GROUP BY cid, brand
        # '''.format(smonth, emonth, ','.join(cids), y=smonth.split('-')[0], m=smonth.split('-')[1])
        # ret = dbch.query_all(sql)

        sql = 'SELECT alias_bid, bid FROM ali.brand_has_alias_bid_month WHERE bid in ({})'.format(','.join([str(v['g_bid']) for v in ret]))
        rrr = dbch.query_all(sql)
        brands = {str(v[1]):v[0] for v in rrr}

        data = []
        for v in ret:
            cid, brand, ssales, snum = v['g_cid'], v['g_bid'], v['c'], v['d']
            if str(brand) in brands:
                brand = main.get_brand(db18, brands[str(brand)])
            elif brand > 0:
                sql = 'SELECT name FROM apollo.brand WHERE bid = {}'.format(brand)
                brand = front.query_all(sql)
                brand = brand[0][0]
            else:
                brand = ''
            d = [
                smonth, 'tmall', '*其他类目 '+main.get_category(db18, cid), brand, ssales, snum
            ]
            data.append(d)

        data.sort(key=functools.cmp_to_key(main.cmp1))

        fields = ['时间', '平台', '子品类', '品牌', '销售额', '销量']
        filedir = 'batch{} {}{}~{}{}'.format(self.cleaner.bid, smonth.split('-')[0], smonth.split('-')[1], emonth.split('-')[0], emonth.split('-')[1])
        self.export_csv(filedir, '其他大盘列表.csv', [fields]+data)


    def sheet4(self, tbl, smonth, emonth):
        db18 = app.get_db('18_apollo')
        db18.connect()

        srange = {
            '键盘': [
                [0, 200],
                [200, 400],
                [400, 500],
                [500, 1000],
                [1000, 0]
            ],
            '有线鼠标': [
                [0, 100],
                [100, 150],
                [150, 200],
                [200, 300],
                [300, 500],
                [500, 0]
            ],
            '无线鼠标': [
                [0, 50],
                [50, 100],
                [100, 150],
                [150, 200],
                [200, 300],
                [300, 500],
                [500, 0]
            ],
            '键鼠套装': [
                [0, 100],
                [100, 200],
                [200, 500],
                [500, 0]
            ],
            '耳机/耳麦': [
                [0, 200],
                [200, 300],
                [300, 500],
                [500, 1000],
                [1000, 0]
            ],
            '麦克风': [
                [0, 200],
                [200, 400],
                [400, 600],
                [600, 800],
                [800, 1000],
                [1000, 1500],
                [1500, 2000],
                [2000, 2500],
                [2500, 3000],
                [3000, 4000],
                [4000, 5000],
                [5000, 6000],
                [6000,0]
            ]
        }

        data, data2, data3 = [], [], {}
        sql = '''
            select sp1, source, if(sp5='综合', '综合', alias_all_bid), sp4, shop_name, name, p1, concat('https://detail.tmall.com/item.htm?id=', tb_item_id), sum(sales)/100 ss, sum(num), avg_price/100
            from {} where date >= '{}' and date < '{}' and sp1 != ''
            group by sp1, tb_item_id, p1 order by ss desc
        '''.format(tbl, smonth, emonth)
        ret = db18.query_all(sql)

        for sp1, source, alias_all_bid, sp4, shop_name, name, p1, tb_item_id, ssales, snum, avg_price, in ret:
            alias_all_bid = str(alias_all_bid, encoding='utf-8')
            d = [
                smonth, source, sp1, main.get_brand(db18, alias_all_bid),
                sp4, shop_name, name, p1, tb_item_id, ssales, snum, avg_price
            ]
            if sp1 in srange:
                rng = ''
                for vv in srange[sp1]:
                    if avg_price >= vv[0] and (avg_price < vv[1] or vv[1] == 0):
                        rng = '{} ~ {}'.format(vv[0], vv[1])
                        break
                d.append(rng)

                k = '{}-{}-{}-{}'.format(smonth, source, sp1, rng)
                if k not in data3:
                    data3[k] = [smonth, source, sp1, 0, 0, 0, rng]
                data3[k][3] += ssales
                data3[k][4] += snum
            else:
                d.append('')

            if alias_all_bid in ['综合', '210', '5175532']:
                data.append(d)
            else:
                data2.append(d)

        fields = ['时间','平台','子品类','品牌','型号','店铺名','宝贝名称','交易属性','id','销售额','销量','均价','价格段']
        filedir = 'batch{} {}{}~{}{}'.format(self.cleaner.bid, smonth.split('-')[0], smonth.split('-')[1], emonth.split('-')[0], emonth.split('-')[1])

        data.sort(key=functools.cmp_to_key(main.cmp2))
        self.export_csv(filedir, '罗技型号清洗.csv', [fields]+data)

        data2.sort(key=functools.cmp_to_key(main.cmp2))
        self.export_csv(filedir, '清洗品类型号.csv', [fields]+data2)

        data = []
        for k in data3:
            data3[k][5] = data3[k][3] / data3[k][4]
            data.append(data3[k])

        fields = ['时间', '平台', '子品类', '销售额', '销量', '均价', '价格段']
        self.export_csv(filedir, '大盘价格段.csv', [fields]+data)


    def cmp1(a, b):
        if a[3] != b[3]:
            return 1 if a[3] > b[3] else -1
        if a[0] != b[0]:
            return 1 if a[0] > b[0] else -1
        if a[2] != b[2]:
            return 1 if a[2] > b[2] else -1
        return 0


    def cmp2(a, b):
        if a[3] != b[3]:
            return 1 if a[3] > b[3] else -1
        if a[0] != b[0]:
            return 1 if a[0] > b[0] else -1
        if a[2] != b[2]:
            return 1 if a[2] > b[2] else -1
        if a[5] != b[5]:
            return 1 if a[5] > b[5] else -1
        if a[10] != b[10]:
            return 1 if a[10] < b[10] else -1
        return 0


    def get_brand(dba, alias_all_bid):
        if alias_all_bid == '综合':
            return alias_all_bid
        sql = 'select name from all_site.all_brand where bid = {}'.format(alias_all_bid)
        ret = dba.query_all(sql)
        return '' if len(ret) == 0 else ret[0][0]


    def get_category(dba, cid):
        sql = 'select lv1name,lv2name,lv3name,lv4name,lv5name from dw_entity.item_category_backend where cid = %s'
        ret = dba.query_all(sql, (cid,))
        return '' if len(ret) == 0 else '>'.join([v for v in ret[0] if v != ''])


    def market(self, smonth, emonth):
        mid = 898
        self.cleaner.market(mid, smonth, emonth)

        db14 = app.get_db('14_apollo')
        db14.connect()
        db18 = app.get_db('18_apollo')
        db18.connect()

        sql = '''
            INSERT ignore INTO `artificial_new`.`entity_{mid}_product` (`alias_all_bid`, `brand`, `name`)
            select alias_all_bid, brand, sp4 from `artificial_new`.`entity_{mid}`
            where alias_all_bid > 0 and sp4 != '' and sp4 != '其它' and date >= '{}' group by alias_all_bid, sp4;
        '''.format(smonth, mid=mid)
        db18.execute(sql)

        sql = '''
            UPDATE `artificial_new`.`entity_{mid}` set pid = 0 where date >= '{}'
        '''.format(smonth, mid=mid)
        db18.execute(sql)

        sql = '''
            UPDATE `artificial_new`.`entity_{mid}` a
            JOIN `artificial_new`.`entity_{mid}_product` b on (a.alias_all_bid=b.alias_all_bid and a.sp4=b.name)
            SET a.pid = b.pid where a.date >= '{}'
        '''.format(smonth, mid=mid)
        db18.execute(sql)

        db18.commit()

        sql = 'UPDATE dataway.entity SET micro_flag = 0 WHERE id = {}'.format(mid)
        db14.execute(sql)
        db14.commit()

        self.csv(mid, smonth, emonth)