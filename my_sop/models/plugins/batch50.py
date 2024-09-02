import sys
import time
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app

class main(Batch.main):
    # 每月11号跑 p3 scripts/clean_import_brush_new.py -b 50 --process --start_month='2020-04-01' --end_month='2020-05-01'
    # select a.clean_flag, b.name, count(*) from product_lib.entity_90513_item a join brush.all_brand b on (a.alias_all_bid=b.bid) group by a.clean_flag, b.name order by a.clean_flag

    def each_month(self, s, e):
        i = []
        for y in range(s[0], e[0] + 1):
            if y == s[0]:
                for m in range(s[1], 13):
                    a = ['{}-{}-{}'.format(y, str.zfill(str(m), 2), '01'),
                         '{}-{}-{}'.format((y if m < 12 else y + 1), str.zfill(str(m + 1 if m < 12 else 1), 2), '01')]
                    print(a)
                    i.append(a)
            elif y == e[0]:
                for m in range(1, e[1]):
                    a = ['{}-{}-{}'.format(y, str.zfill(str(m), 2), '01'),
                         '{}-{}-{}'.format((y if m < 12 else y + 1), str.zfill(str(m + 1 if m < 12 else 1), 2), '01')]
                    print(a)
                    i.append(a)
            else:
                for m in range(1, 13):
                    a = ['{}-{}-{}'.format(y, str.zfill(str(m), 2), '01'),
                         '{}-{}-{}'.format((y if m < 12 else y + 1), str.zfill(str(m + 1 if m < 12 else 1), 2), '01')]
                    print(a)
                    i.append(a)
        return i

    def brush_0909(self, smonth, emonth,logId=-1):
        # 默认规则
        # 1105:原来的品牌弃用，改成白家
        # bids = [str(bid) for bid in mapp]
        # sp62 = ['白家']
        cname, ctbl = self.get_c_tbl()
        sp62 = ['三养',
                '五谷道场',
                '今麦郎',
                '农心',
                '南街村',
                '寿桃',
                '幸运',
                '康师傅',
                '拉面说',
                '日清',
                '海富盛',
                '白象',
                '统一',
                '白家',
                '顾大嫂']
        # sql = 'SELECT source, tb_item_id, real_p1 FROM {}'.format(self.cleaner.get_tbl())
        # ret = self.cleaner.db26.query_all(sql)
        # mpp = {'{}-{}-{}'.format(v[0],v[1],v[2]): True for v in ret}
        # 已出题

        clean_flag = self.cleaner.last_clean_flag() + 1
        ret, sales_by_uuid = self.cleaner.process_top_anew(smonth, emonth, rate=0.8)
        uuids = []
        for uuid2, source, tb_item_id, p1, cid, bid in ret:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids.append(uuid2)

        uuids_list = ','.join([str(uuid2) for uuid2 in uuids])
        sql = 'select uuid2 from {} where sp62 in ({}) and uuid2 in ({})'.format(ctbl, ','.join('\''+t+'\'' for t in sp62), uuids_list)
        cdb = self.cleaner.get_db(cname)
        ret1 = cdb.query_all(sql)
        uuids1 = [v[0]for v in ret1]
        self.cleaner.add_brush(uuids1, clean_flag, 1)
        print(','.join('\''+t+'\'' for t in sp62))
        print('add new brush {}, {}'.format(len(uuids),len(uuids1)))
        return True

    def brush(self, smonth, emonth,logId=-1):
        # 默认规则
        # 1105:原来的品牌弃用，改成白家
        # bids = [str(bid) for bid in mapp]
        # sp62 = ['白家']
        cname, ctbl = self.get_all_tbl()
        sp62 = ['三养',
                '今麦郎',
                '农心',
                '寿桃',
                '康师傅',
                '拉面说',
                '日清',
                '白象',
                '统一',
                '白家']
        # sql = 'SELECT source, tb_item_id, real_p1 FROM {}'.format(self.cleaner.get_tbl())
        # ret = self.cleaner.db26.query_all(sql)
        # mpp = {'{}-{}-{}'.format(v[0],v[1],v[2]): True for v in ret}
        # 已出题



        clean_flag = self.cleaner.last_clean_flag() + 1

        where='''
        ((source=1 and shop_type>20) or (source>1))
        '''
        ret, sales_by_uuid = self.cleaner.process_top_anew(smonth, emonth, rate=0.8, where=where)
        uuids = []
        for uuid2, source, tb_item_id, p1, cid, bid in ret:
            if self.skip_brush(source, tb_item_id, p1):
                continue
            uuids.append(uuid2)

        uuids_list = ','.join(['\'{}\''.format(uuid2) for uuid2 in uuids])
        sql = 'select uuid2 from {} where c_sp62 in ({}) and uuid2 in ({})'.format(ctbl, ','.join('\''+t+'\'' for t in sp62), uuids_list)
        cdb = self.cleaner.get_db(cname)
        ret1 = cdb.query_all(sql)
        uuids1 = [v[0]for v in ret1]
        self.cleaner.add_brush(uuids1, clean_flag, 1)
        print(','.join('\''+t+'\'' for t in sp62))
        print('add new brush {}, {}'.format(len(uuids),len(uuids1)))

        # 11号跑的时候带上以下一段话，平时注释掉

        sales_by_uuid = {}
        uuids=[]

        where1 = '''
        uuid2 in (select uuid2 from {ctbl} where c_sp62 =\'日清\' and pkey>=\'{smonth}\' and pkey<\'{emonth}\') and  ((source=1 and shop_type>20) or (source>1))
        and (toString(source), toString(item_id), toString(arrayReduce('BIT_XOR',arrayMap(x->crc32(x),arrayFilter(y->trim(y)<>'',arrayReduce('groupUniqArray',trade_props.value))))))
        not in (SELECT toString(snum), toString(tb_item_id), toString(real_p1) from mysql('192.168.30.93', 'product_lib', 'entity_90513_item', 'cleanAdmin', '6DiloKlm') )
        '''.format(ctbl=ctbl, smonth=smonth, emonth=emonth)
        ret, sales_by_uuid_1 = self.cleaner.process_top_anew(smonth, emonth, where=where1, limit=100)
        sales_by_uuid.update(sales_by_uuid_1)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            uuids.append(uuid2)
        self.cleaner.add_brush(uuids, clean_flag, visible_check=1, visible=1, sales_by_uuid=sales_by_uuid_1)
        print('**********',len(uuids))

        return True



    def brush_modify(self, v, bru_items):
        units = {
            '杯装':'杯',
            '桶装':'桶',
            '盒装':'盒','盒装/干脆面':'盒',
            '碗装':'碗','碗装/自热面':'碗',
            '箱装':'袋',
            '罐装':'罐',
            '袋装':'袋','袋装/一倍半':'袋','袋装/干脆面':'袋'
        }

        for vv in v['split_pids']:
            if v['flag'] == 1:
                continue
            unit = units[vv['sp11']] if vv['sp11'] in units else '袋'
            if vv['sp56'] != '':
                vv['sp52'] = str(round(int(vv['number']) * int(vv['sp56']), 2)) + unit
            if vv['sp47'] != '':
                vv['sp48'] = str(round(int(vv['number']) * eval(vv['sp47'].replace('g', '')), 2)) + 'g'


    def hotfix(self, tbl, dba, prefix, params):
        super().hotfix(tbl, dba, prefix, params)

        sql = 'DROP TABLE IF EXISTS sop_e.entity_90513xxjoin'
        dba.execute(sql)

        sql = '''
            CREATE TABLE sop_e.entity_90513xxjoin
            (
                `uuid2` Int64,
                `number` UInt32
            )
            ENGINE = Join(ANY, LEFT, uuid2);
        '''
        dba.execute(sql)

        sql = '''
            INSERT INTO sop_e.entity_90513xxjoin
            SELECT uuid2 , arraySum(groupArray(IFNull(toUInt32OrNull(extract(`c_props.value`[indexOf(`c_props.name`,'包数')], '\\d+')), c_number)))
            FROM {} GROUP BY uuid2 HAVING count(*) > 1
        '''.format(tbl)
        dba.execute(sql)

        sql = '''
            INSERT INTO sop_e.entity_90513xxjoin
            SELECT uuid2, toUInt32(extract(`c_props.value`[indexOf(`c_props.name`,'包数')], '\\\\d+'))
            FROM {} WHERE isNull(joinGet('sop_e.entity_90513xxjoin', 'number', uuid2))
            AND `c_props.value`[indexOf(`c_props.name`,'包数')] != '其它'
        '''.format(tbl)
        dba.execute(sql)

        sql = '''
            ALTER TABLE {} UPDATE
                `c_props.name` = arrayConcat(
                    arrayFilter((x) -> x NOT IN ['总入数'], `c_props.name`),
                    ['总入数']
                ),
                `c_props.value` = arrayConcat(
                    arrayFilter((k, x) -> x NOT IN ['总入数'], `c_props.value`, `c_props.name`),
                    [
                        ifNull(toString(joinGet('sop_e.entity_90513xxjoin', 'number', uuid2)), '其它')
                    ]
                )
            WHERE 1
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = 'DROP TABLE sop_e.entity_90513xxjoin'
        dba.execute(sql)


    def hotfix_new(self, tbl, dba, prefix):
        self.cleaner.add_miss_cols(tbl, {'sp总入数':'String'})

        sql = 'DROP TABLE IF EXISTS {}_join'.format(tbl)
        dba.execute(sql)

        sql = '''
            CREATE TABLE {}_join
            (
                `uuid2` Int64,
                `number` UInt32
            )
            ENGINE = Join(ANY, LEFT, uuid2)
        '''.format(tbl)
        dba.execute(sql)

        sql = '''
            INSERT INTO {t}_join
            SELECT uuid2 , arraySum(groupArray(IFNull(toUInt32OrNull(extract(`sp包数`, '\\d+')), b_number)))
            FROM {t} GROUP BY uuid2 HAVING count(*) > 1
        '''.format(t=tbl)
        dba.execute(sql)

        sql = '''
            INSERT INTO {t}_join SELECT uuid2, toUInt32(extract(`sp包数`, '\\\\d+'))
            FROM {t} WHERE isNull(joinGet('{t}_join', 'number', uuid2)) AND `sp包数` != '其它'
        '''.format(t=tbl)
        dba.execute(sql)

        sql = '''
            ALTER TABLE {t} UPDATE `sp总入数` = ifNull(toString(joinGet('{t}_join', 'number', uuid2)), '其它')
            WHERE 1
        '''.format(t=tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = 'DROP TABLE {}_join'.format(tbl)
        dba.execute(sql)