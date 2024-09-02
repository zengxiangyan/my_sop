import sys
import time
import ujson
import datetime
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

import models.plugins.batch as Batch
import application as app

class main(Batch.main):


    def filter_brush_props(self):
        return super().filter_brush_props(filter_keys=['颜色分类','颜色','包装规格','规格'])


    # 出题答题用

    def filter_brush_props(self):
        # 答题回填判断相同时使用指定属性
        def format(source, trade_prop_all, item):
            r = self.cleaner.json_decode(trade_prop_all)
            if isinstance(r, dict):
                p1 = r
            else:
                p1 = trade_prop_all.split(',')
                p2 = trade_prop_all.split('|')
                p1 = p1 if len(p1) > len(p2) else p2
                p1 = {i:str(v) for i,v in enumerate(p1)}
            p1 = [p1[p].replace(' ','') for p in p1 if p1[p].replace(' ','')!='' and p in ['颜色分类', '颜色', '包装规格', '规格']]
            p1 = list(set(p1))
            p1.sort()
            return p1
        return format, '''
            arraySort(arrayMap(y->replace(y,' ',''),arrayFilter(y->trim(y)<>'',arrayDistinct(
                arrayMap((x, y)->IF(x != '颜色分类' and x != '颜色' and x != '包装规格' and x != '规格', '', y), `trade_props.name`, `trade_props.value`)
            ))))
        '''

    def filter_brush_props_75(self):
        # 答题回填判断相同时使用指定属性
        def format(source, trade_prop_all):
            r = self.cleaner.json_decode(trade_prop_all)
            if isinstance(r, dict):
                p1 = r
            else:
                p1 = trade_prop_all.split(',')
                p2 = trade_prop_all.split('|')
                p1 = p1 if len(p1) > len(p2) else p2
                p1 = {i:str(v) for i,v in enumerate(p1)}
            p_ls = []
            for key_i in p1.keys():
                if key_i in ['颜色分类', '颜色', '包装规格', '规格']:
                # if key_i in ['颜色分类', '颜色']:
                    p_ls.append(p1[key_i])
                else:
                    pass
            p1 = list(set(p_ls))
            p1.sort()
            return p1
        return format, '''arraySort(arrayFilter(y->trim(y)<>'',arrayDistinct(new_value)))'''

    def process_top_for75(self, smonth, emonth, limit=10000, limigBy='', orderBy='ssales', rate=1, where='', tbl=None, ignore_p1=False, filter_by='by_num'):

        cname, ctbl = self.get_c_tbl()
        aname, atbl = self.get_a_tbl()

        db = self.cleaner.get_db(aname)
        tbl = tbl or atbl

        sql = '''
            SELECT sum(sales*sign) FROM {} WHERE pkey >= '{}' AND pkey < '{}' {}
        '''.format(tbl, smonth, emonth, '' if where == '' else 'AND ({})'.format(where))
        ret = db.query_all(sql)
        total = ret[0][0]

        _, filter_p1 = self.filter_brush_props_75()
        ignore_p1 = 1 if ignore_p1 else 'p1'
        where = '' if where == '' else 'AND ({})'.format(where)
        # WITH arrayMap((x, y)->IF(x != '颜色分类' and x != '颜色' and x != '包装规格' and x != '规格', '', y), `trade_props.name`, `trade_props.value`) as new_value
        sql = '''
            WITH arrayMap((x, y)->IF(x != '颜色分类' and x != '颜色' and x != '包装规格' and x != '规格', '', y), `trade_props.name`, `trade_props.value`) as new_value
            SELECT argMax(uuid2, num), source, item_id, {p1_new} as p1, argMax(cid, date) rcid, sum(sales*sign) ssales, argMax(alias_all_bid, date) alias FROM {tbl}
            WHERE pkey >= '{smonth}' AND pkey < '{emonth}' AND sales > 0 AND num > 0 {where}
              AND uuid2 NOT IN (SELECT uuid2 FROM {tbl} WHERE pkey >= '{smonth}' AND pkey < '{emonth}' AND sign = -1)
            GROUP BY source, item_id, {p1}
            ORDER BY {orderBy} DESC
            LIMIT {limit} {limigBy}
        '''.format(p1_new=filter_p1, where=where, orderBy=orderBy, limit=limit, limigBy=limigBy, tbl=tbl, smonth=smonth, emonth=emonth, p1=ignore_p1)
        ret = db.query_all(sql)

        uuids, total = [], total * rate
        sales_by_uuid = {}
        for uuid2, source, item_id, p1, cid, ss, alias_all_bid in ret:
            if total < 0:
                break
            total -= ss
            uuids.append([uuid2, source, item_id, p1, cid, alias_all_bid])
            sales_by_uuid[uuid2] = ss
        return uuids, sales_by_uuid

    def brush(self, smonth, emonth,logId=-1):




        cname, ctbl = self.get_all_tbl()
        aname, atbl = self.get_a_tbl()
        clean_flag = self.cleaner.last_clean_flag() + 1

        db = self.cleaner.get_db(aname)
        sales_by_uuid = {}

        sql = 'SELECT snum, tb_item_id, p1, id, visible FROM {} where visible = 5'.format(self.cleaner.get_tbl())
        ret = self.cleaner.db26.query_all(sql)
        brush_p1, filter_p1 = self.filter_brush_props_75()
        data = []
        for snum, tb_item_id, p1, id, visible in ret:
            p1 = brush_p1(snum, p1)
            data.append([snum, tb_item_id, p1, id, visible])
        mpp = {'{}-{}-{}'.format(v[0], v[1], v[2]): [v[3], v[4]] for v in data}

        uuids_ttl = []


        for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            uuids1 = []
            where1 = '''
            uuid2 in (select uuid2 from {ctbl} where pkey >='{ssmonth}' and pkey<'{eemonth}' and if(c_all_bid  = 0,alias_all_bid, c_all_bid ) in (52390,201237,243393,6071652)) and (source = 1 and (shop_type > 20 or shop_type < 10)) and cid in (50023751,50023753,126248001,126246001)
            '''.format(ctbl=ctbl, ssmonth=ssmonth, eemonth=eemonth)
            ret, sales_by_uuid1 = self.process_top_for75(ssmonth, eemonth, where=where1, limit=1000000)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                uuids_ttl.append(uuid2)
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    continue
                else:
                    uuids1.append(uuid2)
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 5]

            self.cleaner.add_brush(uuids1, clean_flag, visible=5, sales_by_uuid=sales_by_uuid)

        for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            uuids2 = []
            for each_bid in [130598,176723, 2879494,380611,677737,109463,311028,6153805,243071]:
                for each_cid in [50023751,50023753,126248001,126246001]:
                    where2 = '''
                    uuid2 in (select uuid2 from {ctbl} where pkey >='{ssmonth}' and pkey<'{eemonth}' and if(c_all_bid  = 0,alias_all_bid, c_all_bid ) = '{each_bid}') and cid = '{each_cid}'
                    '''.format(each_bid=each_bid, each_cid=each_cid, ctbl=ctbl, ssmonth=ssmonth, eemonth=eemonth)
                    ret, sales_by_uuid1 = self.process_top_for75(ssmonth, eemonth, where=where2, limit=1000000, rate=0.9)
                    sales_by_uuid.update(sales_by_uuid1)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        uuids_ttl.append(uuid2)
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            continue
                        else:
                            uuids2.append(uuid2)
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 5]

            self.cleaner.add_brush(uuids2, clean_flag, visible=5, sales_by_uuid=sales_by_uuid)

        for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            uuids3 = []
            for each_bid in [5366648]:
                for each_cid in [50023753,126248001]:
                    where3 = '''
                    uuid2 in (select uuid2 from {ctbl} where pkey >='{ssmonth}' and pkey<'{eemonth}' and if(c_all_bid  = 0,alias_all_bid, c_all_bid ) = '{each_bid}') and cid = '{each_cid}'
                    '''.format(each_bid=each_bid, each_cid=each_cid, ctbl=ctbl, ssmonth=ssmonth, eemonth=eemonth)
                    ret, sales_by_uuid1 = self.process_top_for75(ssmonth, eemonth, where=where3, limit=1000000, rate=0.9)
                    sales_by_uuid.update(sales_by_uuid1)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        uuids_ttl.append(uuid2)
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            continue
                        else:
                            uuids3.append(uuid2)
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 5]

            self.cleaner.add_brush(uuids3, clean_flag, visible=5, sales_by_uuid=sales_by_uuid)

        for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            uuids4 = []
            for each_bid in [2178963]:
                for each_cid in [50023751,126246001]:
                    where4 = '''
                    uuid2 in (select uuid2 from {ctbl} where pkey >='{ssmonth}' and pkey<'{eemonth}' and if(c_all_bid  = 0,alias_all_bid, c_all_bid ) = '{each_bid}') and cid = '{each_cid}'
                    '''.format(each_bid=each_bid, each_cid=each_cid, ctbl=ctbl, ssmonth=ssmonth, eemonth=eemonth)
                    ret, sales_by_uuid1 = self.process_top_for75(ssmonth, eemonth, where=where4, limit=1000000, rate=0.9)
                    sales_by_uuid.update(sales_by_uuid1)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        uuids_ttl.append(uuid2)
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            continue
                        else:
                            uuids4.append(uuid2)
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 5]

            self.cleaner.add_brush(uuids4, clean_flag, visible=5, sales_by_uuid=sales_by_uuid)

        for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            uuids5 = []
            for each_bid in [6153805,2879494,243071]:
                for each_cid in [50023751,126246001]:
                    where5 = '''
                    uuid2 in (select uuid2 from {ctbl} where pkey >='{ssmonth}' and pkey<'{eemonth}' and if(c_all_bid  = 0,alias_all_bid, c_all_bid ) = '{each_bid}') and cid = '{each_cid}'
                    '''.format(each_bid=each_bid, each_cid=each_cid, ctbl=ctbl, ssmonth=ssmonth, eemonth=eemonth)
                    ret, sales_by_uuid1 = self.process_top_for75(ssmonth, eemonth, where=where5, limit=1000000)
                    sales_by_uuid.update(sales_by_uuid1)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        uuids_ttl.append(uuid2)
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            continue
                        else:
                            uuids5.append(uuid2)
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 5]

            self.cleaner.add_brush(uuids5, clean_flag, visible=5, sales_by_uuid=sales_by_uuid)


        for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            uuids6 = []
            for each_bid in [6153805]:
                for each_cid in [50023753,126248001]:
                    where6 = '''
                    uuid2 in (select uuid2 from {ctbl} where pkey >='{ssmonth}' and pkey<'{eemonth}' and if(c_all_bid  = 0,alias_all_bid, c_all_bid ) = '{each_bid}') and cid = '{each_cid}'
                    '''.format(each_bid=each_bid, each_cid=each_cid, ctbl=ctbl, ssmonth=ssmonth, eemonth=eemonth)
                    ret, sales_by_uuid1 = self.process_top_for75(ssmonth, eemonth, where=where6, limit=1000000)
                    sales_by_uuid.update(sales_by_uuid1)
                    for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                        uuids_ttl.append(uuid2)
                        if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                            continue
                        else:
                            uuids6.append(uuid2)
                            mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 5]

            self.cleaner.add_brush(uuids6, clean_flag, visible=5, sales_by_uuid=sales_by_uuid)

        for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            uuids7 = []
            where7 = '''
            uuid2 in (select uuid2 from {ctbl} where pkey >='{ssmonth}' and pkey<'{eemonth}' and c_sp1='透明片' and c_sp16 in ('依视明', 'H2O', '氧眼清眸')) and (source = 1 and (shop_type > 20 or shop_type < 10))
            '''.format(ctbl=ctbl, ssmonth=ssmonth, eemonth=eemonth)
            ret, sales_by_uuid1 = self.process_top_for75(ssmonth, eemonth, where=where7, limit=1000000)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                uuids_ttl.append(uuid2)
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    continue
                else:
                    uuids7.append(uuid2)
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 5]

            self.cleaner.add_brush(uuids7, clean_flag, visible=5, sales_by_uuid=sales_by_uuid)



        for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            uuids8 = []
            where8 = '''
            uuid2 in (select uuid2 from {ctbl} where pkey >='{ssmonth}' and pkey<'{eemonth}' and c_sp1='透明片' and c_sp33 in ('依视明', 'H2O', '氧眼清眸', '银采')) and (source = 1 and (shop_type > 20 or shop_type < 10))
            '''.format(ctbl=ctbl, ssmonth=ssmonth, eemonth=eemonth)
            ret, sales_by_uuid1 = self.process_top_for75(ssmonth, eemonth, where=where8, limit=1000000)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                uuids_ttl.append(uuid2)
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    continue
                else:
                    uuids8.append(uuid2)
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 5]

            self.cleaner.add_brush(uuids8, clean_flag, visible=5, sales_by_uuid=sales_by_uuid)

        for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            uuids9 = []
            where9 = '''
            item_id = '678773622183' and (source = 1 and (shop_type > 20 or shop_type < 10))
            '''.format(ctbl=ctbl, ssmonth=ssmonth, eemonth=eemonth)
            ret, sales_by_uuid1 = self.process_top_for75(ssmonth, eemonth, where=where9, limit=1000000)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                uuids_ttl.append(uuid2)
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    continue
                else:
                    uuids9.append(uuid2)
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 5]

            self.cleaner.add_brush(uuids9, clean_flag, visible=5, sales_by_uuid=sales_by_uuid)

        for ssmonth, eemonth in self.cleaner.each_month(smonth, emonth):
            uuids10 = []
            where10 = '''
            alias_all_bid = '431974' and (source = 1 and (shop_type > 20 or shop_type < 10))
            '''.format(ctbl=ctbl, ssmonth=ssmonth, eemonth=eemonth)
            ret, sales_by_uuid1 = self.process_top_for75(ssmonth, eemonth, where=where10, limit=1000000)
            sales_by_uuid.update(sales_by_uuid1)
            for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
                uuids_ttl.append(uuid2)
                if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                    continue
                else:
                    uuids10.append(uuid2)
                    mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 5]

            self.cleaner.add_brush(uuids10, clean_flag, visible=5, sales_by_uuid=sales_by_uuid)

        new_uuids, old_uuids_update, map_help_new = self.skip_helper_75(smonth, emonth, uuids_ttl)
        self.cleaner.add_brush(old_uuids_update, clean_flag, visible_check=1, visible=5, sales_by_uuid=sales_by_uuid)

        sql_itemid = '''
                select distinct tb_item_id from {tbl} where  /*cast(created as date) = cast(now() as date) and*/ visible = 5
                '''.format(tbl=self.cleaner.get_tbl())
        ret_itemid = self.cleaner.db26.query_all(sql_itemid)
        itemids = [str(v[0]) for v in ret_itemid]

        uuids_special = []
        where_item_id = '''
                item_id in ({}) and (indexOf(trade_props.name, '包装规格')>0 or indexOf(trade_props.name, '规格')>0)
                '''.format(','.join(["'" + str(v) + "'" for v in itemids]))

        # where_item_id = '''
        # item_id in ('674027556881','654332282414','659686148287','628541314759','668632320665','675899484675')
        # '''

        ret, sales_by_uuid1 = self.process_top_for75(smonth, emonth, where=where_item_id, limit=1000000)
        sales_by_uuid.update(sales_by_uuid1)
        for uuid2, source, tb_item_id, p1, cid, alias_all_bid in ret:
            if '{}-{}-{}'.format(source, tb_item_id, p1) in mpp:
                continue
            else:
                uuids_special.append(uuid2)
                mpp['{}-{}-{}'.format(source, tb_item_id, p1)] = [0, 5]

        self.cleaner.add_brush(uuids_special, clean_flag, visible=5, sales_by_uuid=sales_by_uuid)
        # print(len(uuid1))

        self.cleaner.set_default_val(type=1)


        return True


    def brand_name(self, bid, mref=False, bref=False):
        if bid == 0:
            return ['Others', 'Others', 'Others']

        k = 'bname{}'.format(bid)
        if k not in self.cache:
            db26 = self.cleaner.get_db(self.cleaner.db26name)
            sql = 'SELECT name, if(name_cn_front!=\'\',name_cn_front,name), if(name_en_front!=\'\',name_en_front,name) FROM brush.all_brand WHERE bid={}'.format(bid)
            name, name_cn, name_en = db26.query_all(sql)[0]
            self.cache[k] = [name, name_cn or name_en, name_en or name_cn]

        manudict = {
            '52390': ['强生', 'J&J'],
            '130598': ['海昌', 'HYDRON'],
            '243393': ['博士伦', 'B+L'],
            '201237': ['爱尔康', 'ALCON'],
            '109463': ['卫康', 'WEICON'],
            '311028': ['库博', 'COOPER'],
            '176723': ['海俪恩', 'HORIEN'],
            '380579': ['科莱博', 'CLB'],
            '380611': ['实瞳', 'SEED'],
            '510424': ['晶硕', 'PEGAVISION'],
            '2178963': ['米如', 'MIRU'],
            '6071652': ['强生', 'J&J']
        }
        if mref and str(bid) in manudict:
            return [self.cache[k][0], manudict[str(bid)][0], manudict[str(bid)][1]]

        branddict = {
            '52390': ['安视优', 'ACUVUE'],
            '130598': ['海昌', 'HYDRON'],
            '243393': ['博士伦', 'B+L'],
            '201237': ['爱尔康', 'ALCON'],
            '109463': ['卫康', 'WEICON'],
            '311028': ['库博', 'COOPER'],
            '176723': ['海俪恩', 'HORIEN'],
            '380579': ['科莱博', 'CLB'],
            '380611': ['实瞳', 'SEED'],
            '510424': ['晶硕', 'PEGAVISION'],
            '2178963': ['米如', 'MIRU'],
            '431917': ['美尼康', 'Menicon'],
            '3312878': ['培克能', 'Bioclen'],
            '52529': ['优能', 'Visine'],
            '48200': ['雅培', 'Abbott'],
            '4954': ['蓝睛灵', 'Blue Fairy'],
            '98219': ['美汐', 'MeiXi'],
            '1367341': ['水氧E清', 'SHUIYANGEQING'],
            '103426': ['思汉普', 'SAP'],
            '288907': ['新视界', 'XINSHIJIE'],
            '469414': ['依视明', 'Yes Moon'],
            '380601': ['艾爵', 'AIJUE'],
            '2378893': ['艾乐视', 'I LOVE EYES'],
            '5336044': ['奥普铁克', 'AOPUTIEKE'],
            '380596': ['保视宁', 'Porslen'],
            '18962': ['蔡司', 'Zeiss'],
            '127760': ['芳姿', 'Fancy'],
            '246718': ['福瑞达', 'Freda'],
            '218544': ['韩后', 'HAN HOU'],
            '1705090': ['韩姬儿', 'HanGee'],
            '1006041': ['睛盾', 'Mydeskmate'],
            '4571284': ['镜特舒', 'JINGTESHU'],
            '3179376': ['咔薇乐', 'KAWEILE'],
            '1617108': ['卡洛尼', 'KALUONI'],
            '380589': ['康视达', 'Constar'],
            '3249282': ['珂俪维', 'Clear Vealer'],
            '243071': ['可啦啦', 'Kilala'],
            '4870842': ['可丽维', 'KELIWEI'],
            '341445': ['葵花药业', 'KUIHUA'],
            '1983289': ['澜柏', 'Lambor'],
            '469419': ['乐敦清', 'Rohto'],
            '3312881': ['美睛尊品', 'MEIJINGZUNPIN'],
            '142391': ['美侬', 'MINO'],
            '1533123': ['魅瞳', 'Merry Dolly'],
            '3312880': ['米路可爱', 'Milook-eye'],
            '82018': ['明亮', 'MINGLIANG'],
            '252467': ['陌陌', 'Mo'],
            '2446994': ['娜他她', 'NAATAA'],
            '4880484': ['培磊能', 'PEILEINENG'],
            '155859': ['七夕', 'QIXI'],
            '929401': ['齐能', 'QINENG'],
            '1051637': ['润', 'RUN'],
            '469416': ['沙福隆', 'SAUFLON'],
            '406005': ['上元堂', 'SHANGYUANTANG'],
            '2769031': ['淘气猴', 'Taoqihou'],
            '1940884': ['媞娜', 'TINA'],
            '4897428': ['微爱', 'WEIAI'],
            '2999371': ['维视达康', 'VISTACON'],
            '487825': ['昕薇女孩', 'XINWEINVHAI'],
            '663032': ['优舒', 'YOUSHU'],
            '143021': ['珍视明', 'ZSM'],
            '20501': ['可视眸', 'NEO'],
            '338371': ['abon', 'abon'],
            '66067': ['GEO', 'GEO'],
            '206718': ['LUCKY EYE', 'LUCKY EYE'],
            '5435727': ['Q-MIX', 'Q-MIX'],
            '182773': ['sweetcolor', 'sweetcolor'],
            '36731': ['爱漾', 'EYEYOUNG'],
            '124444': ['妃爱丽', 'FAIRY'],
            '2531108': ['花魁', '花魁'],
            '484054': ['吉春黄金', '吉春黄金'],
            '2093098': ['珂珂爱', 'COKOEYE'],
            '815506': ['科尔视', '科尔视'],
            '3911890': ['美妆彩片', 'COSME CONTACT'],
            '1669': ['糯米', 'nomi'],
            '25996': ['BD', 'BD'],
            '31692': ['多多', '多多'],
            '6071652': ['啵啵实验室', 'BUBBLE POP LABORATORY'],
            '5838804': ['安目瞳', 'LaPeche'],
            '5887593': ['欧朗睛', 'OLENS'],
            '5259537': ['蕾美', 'ReVIA'],
            '677737': ['美若康', 'Miacare'],
            '5172691': ['绮芙莉', '绮芙莉'],
            '26': ['other', 'other']
        }
        if bref and str(bid) in branddict:
            return [self.cache[k][0], branddict[str(bid)][0], branddict[str(bid)][1]]

        return self.cache[k]


    def sku_info(self, real_bid, real_sku, real_p6):
        if 'skuinfo' not in self.cache:
            data, bids = {}, []
            db18 = self.cleaner.get_db('196_apollo')

            sql = 'SELECT bid,sku_other,sku_bsl,sku_qs,sku_en,packsize,p6,only_bsl FROM artificial_new.entity_90481_sku'
            ret = db18.query_all(sql)

            for bid, sku_other, sku_bsl, sku_qs, sku_en, packsize, pp6, only_bsl, in ret:
                pp6 = ['日抛', '半年抛', '年抛', '月抛', '双周抛', '季抛'] if pp6 == '' else [pp6]
                for sku in sku_other.split('|'):
                    for onep6 in pp6:
                        data['{}{}{}'.format(bid, sku, onep6)] = [sku_bsl, sku_qs, sku_en, str(only_bsl)]
                bids.append(str(bid))

            self.cache['skuinfo'] = data
            self.cache['skuinfo2'] = bids

        k = '{}{}{}'.format(real_bid, real_sku, real_p6)
        return self.cache['skuinfo'][k] if k in self.cache['skuinfo'] else (['其它', '其它', 'OTHERS', ''] if str(real_bid) in self.cache['skuinfo2'] else None)


    def pre_brush_modify(self, v, products, prefix):
        if v['visible'] not in [5]:
            v['flag'] = 0


    def brush_modify(self, v, bru_items):
        specialBid = (52390,243393,130598,201237,311028,109463,176723,243071,380611,2378893,510424,103426,2178963,6071652,5172691,5887593,677737)
        specialBid_qs = (52390, 243393, 130598, 201237, 311028, 109463, 176723, 6071652)
        P6Beauty = {
            '日抛': 'Daily',
            '半年抛': 'Half-yearly',
            '年抛': 'Yearly',
            '月抛': 'Monthly',
            '双周抛': 'Bi-weekly',
            '季抛': 'Quarterly',
            'delete': 'delete',
            'Others': 'Others',
        }
        p6_modality = {
            '日抛': 'DD',
            '月抛': 'RD',
            '双周抛': 'RD',
            '年抛': 'CCL',
            '半年抛': 'CCL',
            '季抛': 'CCL',
        }
        mp1 = {'透明片':'Clear','彩片':'Beauty'}
        for vv in v['split_pids']:
            # vv['sp3'], vv['sp6'], vv['sp7'] = self.brand_name(vv['alias_all_bid'], mref=True, bref=True)
            # _, vv['sp12'], vv['sp13'] = self.brand_name(vv['alias_all_bid'], bref=True)
            vv['sp2'] = mp1[vv['sp1']] if vv['sp1'] in mp1 else vv['sp2']
            # vv['sp4'] = '是' if vv['alias_all_bid'] in specialBid_qs else '否'

            # if vv['alias_all_bid'] in (5172691,5887593,243071,2378893) and v['cid'] in (50023751,126246001):
            #     vv['sp5'] = '否'
            # elif vv['alias_all_bid'] in (2178963,103426,109463) and v['cid'] in (50023753,126248001):
            #     vv['sp5'] = '否'
            # elif vv['alias_all_bid'] in specialBid:
            #     vv['sp5'] = '是'
            # else:
            #     vv['sp5'] = '否'

            vv['sp10'] = P6Beauty[vv['sp9']]
            vv['sp11'] = p6_modality[vv['sp9']] if vv['sp9'] in p6_modality else 'Others'
            vv['sp25'] = '人工'
            vv['sp39'] = vv['sp16']

            info = self.sku_info(vv['alias_all_bid'], vv['sp16'], vv['sp9'])
            if info:
                vv['sp16'], vv['sp17'], vv['sp18'], vv['sp36'] = info
            elif vv['sp16'] not in ['OTHERS','']:
                vv['sp17'] = vv['sp18'] = vv['sp16']
            else:
                vv['sp16'] = '其它'
                vv['sp17'] = '其它'
                vv['sp18'] = 'OTHERS'

            if (vv['sp16'].find('toric') > -1 or vv['sp16'].find('散光') > -1) and vv['alias_all_bid'] == 243393:
                vv['sp19'] = 'toric'


    def hotfix_new(self, tbl, dba, prefix):
        sql = '''
            ALTER TABLE {} UPDATE `clean_all_bid` = multiIf(
                `clean_alias_all_bid`=4826468,201237,
                `clean_alias_all_bid`=289060,52390,
                `clean_alias_all_bid`=4606866,52390,
                `clean_alias_all_bid`=17696,52390,
                `clean_alias_all_bid`=19039,1074902,
                `clean_alias_all_bid`=5698403,20501,
                `clean_alias_all_bid`=5905799,5259537,
                `clean_alias_all_bid`=4686268,5259537,
                `clean_alias_all_bid`=1835669,52390,
                `clean_alias_all_bid`=1820677,5887593,
                `clean_alias_all_bid`=3558266,380611,
                `clean_alias_all_bid`=469414,311028,
                `clean_alias_all_bid`
            ) WHERE 1
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            SELECT bid, name_cn_front, name_en_front, name
            FROM artificial.all_brand
            WHERE bid IN (SELECT DISTINCT(clean_alias_all_bid) FROM {})
        '''.format(tbl)
        ret = dba.query_all(sql)

        bids = [str(v[0]) for v in ret]
        bcns = [(v[1] or v[2] or v[3]).replace('\'', '\\\'') for v in ret]
        bens = [(v[2] or v[1] or v[3]).replace('\'', '\\\'') for v in ret]

        bcns = ['\'{}\''.format(v) for v in bcns]
        bens = ['\'{}\''.format(v) for v in bens]

        sql = '''
            ALTER TABLE {} UPDATE
                `sp品牌（中文）` = transform(clean_alias_all_bid, [{bids}], [{bcns}], 'other'),
                `sp厂商（中文）` = transform(clean_alias_all_bid, [{bids}], [{bcns}], 'other'),
                `sp品牌（英文）` = transform(clean_alias_all_bid, [{bids}], [{bens}], 'other'),
                `sp厂商（英文）` = transform(clean_alias_all_bid, [{bids}], [{bens}], 'other'),
                `sp是否是强生关注品牌` = multiIf(
                    clean_alias_all_bid IN (52390,109463,130598,176723,201237,243393,311028,6071652) AND cid IN (50023751,50023753,126248001,126246001), '是',
                    clean_alias_all_bid IN (201237,243393,130598,176723,311028,52390,109463) AND cid IN (12599,13895,13897), '是',
                    '否'
                ),
                `sp是否是博士伦关注品牌` = multiIf(
                    `spPlatform` = '透明片' AND clean_alias_all_bid IN (6343661,201237,243393,130598,176723,311028,52390,109463,380611,2178963,677737,2879494,243071,6153805) AND cid IN (50023751,50023753,126248001,126246001), '是',
                    `spPlatform` = '彩片' AND clean_alias_all_bid IN (6343661,201237,243393,130598,176723, 52390,380611,243071, 677737,6071652,2879494,5366648,6153805) AND cid IN (50023751,50023753,126248001,126246001), '是',
                    '否'
                ),
                `spisSample 试戴片` = 'N'
            WHERE 1
        '''.format(tbl, bids=','.join(bids), bcns=','.join(bcns), bens=','.join(bens))
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE
                `sp品牌（中文）` = multiIf(
                    `clean_alias_all_bid`=469419, '乐敦清',
                    `clean_alias_all_bid`=929401, '齐能',
                    `clean_alias_all_bid`=338371, 'abon',
                    `clean_alias_all_bid`=18962, '蔡司',
                    `clean_alias_all_bid`=143021, '珍视明',
                    `clean_alias_all_bid`=815506, '科尔视',
                    `clean_alias_all_bid`=4870842, '可丽维',
                    `clean_alias_all_bid`=510424, '晶硕',
                    `clean_alias_all_bid`=3911890, '美妆彩片',
                    `clean_alias_all_bid`=2999371, '维视达康',
                    `clean_alias_all_bid`=380611, '实瞳',
                    `clean_alias_all_bid`=311028, '库博',
                    `clean_alias_all_bid`=1940884, '媞娜',
                    `clean_alias_all_bid`=130598, '海昌',
                    `clean_alias_all_bid`=677737, '美若康',
                    `clean_alias_all_bid`=26, 'other',
                    `clean_alias_all_bid`=1006041, '睛盾',
                    `clean_alias_all_bid`=246718, '福瑞达',
                    `clean_alias_all_bid`=31692, '多多',
                    `clean_alias_all_bid`=380601, '艾爵',
                    `clean_alias_all_bid`=5887593, '欧朗睛',
                    `clean_alias_all_bid`=48200, '雅培',
                    `clean_alias_all_bid`=206718, 'LUCKY EYE',
                    `clean_alias_all_bid`=252467, '陌陌',
                    `clean_alias_all_bid`=4880484, '培磊能',
                    `clean_alias_all_bid`=1051637, '润',
                    `clean_alias_all_bid`=6071652, '啵啵实验室',
                    `clean_alias_all_bid`=4571284, '镜特舒',
                    `clean_alias_all_bid`=4897428, '微爱',
                    `clean_alias_all_bid`=1617108, '卡洛尼',
                    `clean_alias_all_bid`=663032, '优舒',
                    `clean_alias_all_bid`=82018, '明亮',
                    `clean_alias_all_bid`=218544, '韩后',
                    `clean_alias_all_bid`=127760, '芳姿',
                    `clean_alias_all_bid`=406005, '上元堂',
                    `clean_alias_all_bid`=380589, '康视达',
                    `clean_alias_all_bid`=124444, '妃爱丽',
                    `clean_alias_all_bid`=109463, '卫康',
                    `clean_alias_all_bid`=3179376, '咔薇乐',
                    `clean_alias_all_bid`=288907, '新视界',
                    `clean_alias_all_bid`=201237, '爱尔康',
                    `clean_alias_all_bid`=380596, '保视宁',
                    `clean_alias_all_bid`=5259537, '蕾美',
                    `clean_alias_all_bid`=98219, '美汐',
                    `clean_alias_all_bid`=2378893, '艾乐视',
                    `clean_alias_all_bid`=2769031, '淘气猴',
                    `clean_alias_all_bid`=182773, 'sweetcolor',
                    `clean_alias_all_bid`=1669, '糯米',
                    `clean_alias_all_bid`=52390, '安视优',
                    `clean_alias_all_bid`=469414, '依视明',
                    `clean_alias_all_bid`=243393, '博士伦',
                    `clean_alias_all_bid`=380579, '科莱博',
                    `clean_alias_all_bid`=52529, '优能',
                    `clean_alias_all_bid`=1533123, '魅瞳',
                    `clean_alias_all_bid`=2531108, '花魁',
                    `clean_alias_all_bid`=3312878, '培克能',
                    `clean_alias_all_bid`=176723, '海俪恩',
                    `clean_alias_all_bid`=487825, '昕薇女孩',
                    `clean_alias_all_bid`=5172691, '绮芙莉',
                    `clean_alias_all_bid`=484054, '吉春黄金',
                    `clean_alias_all_bid`=3312880, '米路可爱',
                    `clean_alias_all_bid`=341445, '葵花药业',
                    `clean_alias_all_bid`=431917, '美尼康',
                    `clean_alias_all_bid`=3249282, '珂俪维',
                    `clean_alias_all_bid`=4954, '蓝睛灵',
                    `clean_alias_all_bid`=243071, '可啦啦',
                    `clean_alias_all_bid`=5336044, '奥普铁克',
                    `clean_alias_all_bid`=1983289, '澜柏',
                    `clean_alias_all_bid`=3312881, '美睛尊品',
                    `clean_alias_all_bid`=25996, 'BD',
                    `clean_alias_all_bid`=142391, '美侬',
                    `clean_alias_all_bid`=5435727, 'Q-MIX',
                    `clean_alias_all_bid`=155859, '七夕',
                    `clean_alias_all_bid`=103426, '思汉普',
                    `clean_alias_all_bid`=2093098, '珂珂爱',
                    `clean_alias_all_bid`=5838804, '安目瞳',
                    `clean_alias_all_bid`=1705090, '韩姬儿',
                    `clean_alias_all_bid`=2446994, '娜他她',
                    `clean_alias_all_bid`=20501, '可视眸',
                    `clean_alias_all_bid`=36731, '爱漾',
                    `clean_alias_all_bid`=469416, '沙福隆',
                    `clean_alias_all_bid`=66067, 'GEO',
                    `clean_alias_all_bid`=2178963, '米如',
                    `clean_alias_all_bid`=1367341, '水氧E清',
                `sp品牌（中文）`),
                `sp品牌（英文）` = multiIf(
                    `clean_alias_all_bid`=469419, 'Rohto',
                    `clean_alias_all_bid`=929401, 'QINENG',
                    `clean_alias_all_bid`=338371, 'abon',
                    `clean_alias_all_bid`=18962, 'Zeiss',
                    `clean_alias_all_bid`=143021, 'ZSM',
                    `clean_alias_all_bid`=815506, '科尔视',
                    `clean_alias_all_bid`=4870842, 'KELIWEI',
                    `clean_alias_all_bid`=510424, 'PEGAVISION',
                    `clean_alias_all_bid`=3911890, 'COSME CONTACT',
                    `clean_alias_all_bid`=2999371, 'VISTACON',
                    `clean_alias_all_bid`=380611, 'SEED',
                    `clean_alias_all_bid`=311028, 'COOPER',
                    `clean_alias_all_bid`=1940884, 'TINA',
                    `clean_alias_all_bid`=130598, 'HYDRON',
                    `clean_alias_all_bid`=677737, 'Miacare',
                    `clean_alias_all_bid`=26, 'other',
                    `clean_alias_all_bid`=1006041, 'Mydeskmate',
                    `clean_alias_all_bid`=246718, 'Freda',
                    `clean_alias_all_bid`=31692, '多多',
                    `clean_alias_all_bid`=380601, 'AIJUE',
                    `clean_alias_all_bid`=5887593, 'OLENS',
                    `clean_alias_all_bid`=48200, 'Abbott',
                    `clean_alias_all_bid`=206718, 'LUCKY EYE',
                    `clean_alias_all_bid`=252467, 'Mo',
                    `clean_alias_all_bid`=4880484, 'PEILEINENG',
                    `clean_alias_all_bid`=1051637, 'RUN',
                    `clean_alias_all_bid`=6071652, 'BUBBLE POP LABORATORY',
                    `clean_alias_all_bid`=4571284, 'JINGTESHU',
                    `clean_alias_all_bid`=4897428, 'WEIAI',
                    `clean_alias_all_bid`=1617108, 'KALUONI',
                    `clean_alias_all_bid`=663032, 'YOUSHU',
                    `clean_alias_all_bid`=82018, 'MINGLIANG',
                    `clean_alias_all_bid`=218544, 'HAN HOU',
                    `clean_alias_all_bid`=127760, 'Fancy',
                    `clean_alias_all_bid`=406005, 'SHANGYUANTANG',
                    `clean_alias_all_bid`=380589, 'Constar',
                    `clean_alias_all_bid`=124444, 'FAIRY',
                    `clean_alias_all_bid`=109463, 'WEICON',
                    `clean_alias_all_bid`=3179376, 'KAWEILE',
                    `clean_alias_all_bid`=288907, 'XINSHIJIE',
                    `clean_alias_all_bid`=201237, 'ALCON',
                    `clean_alias_all_bid`=380596, 'Porslen',
                    `clean_alias_all_bid`=5259537, 'ReVIA',
                    `clean_alias_all_bid`=98219, 'MeiXi',
                    `clean_alias_all_bid`=2378893, 'I LOVE EYES',
                    `clean_alias_all_bid`=2769031, 'Taoqihou',
                    `clean_alias_all_bid`=182773, 'sweetcolor',
                    `clean_alias_all_bid`=1669, 'nomi',
                    `clean_alias_all_bid`=52390, 'ACUVUE',
                    `clean_alias_all_bid`=469414, 'Yes Moon',
                    `clean_alias_all_bid`=243393, 'B+L',
                    `clean_alias_all_bid`=380579, 'CLB',
                    `clean_alias_all_bid`=52529, 'Visine',
                    `clean_alias_all_bid`=1533123, 'Merry Dolly',
                    `clean_alias_all_bid`=2531108, '花魁',
                    `clean_alias_all_bid`=3312878, 'Bioclen',
                    `clean_alias_all_bid`=176723, 'HORIEN',
                    `clean_alias_all_bid`=487825, 'XINWEINVHAI',
                    `clean_alias_all_bid`=5172691, '绮芙莉',
                    `clean_alias_all_bid`=484054, '吉春黄金',
                    `clean_alias_all_bid`=3312880, 'Milook-eye',
                    `clean_alias_all_bid`=341445, 'KUIHUA',
                    `clean_alias_all_bid`=431917, 'Menicon',
                    `clean_alias_all_bid`=3249282, 'Clear Vealer',
                    `clean_alias_all_bid`=4954, 'Blue Fairy',
                    `clean_alias_all_bid`=243071, 'Kilala',
                    `clean_alias_all_bid`=5336044, 'AOPUTIEKE',
                    `clean_alias_all_bid`=1983289, 'Lambor',
                    `clean_alias_all_bid`=3312881, 'MEIJINGZUNPIN',
                    `clean_alias_all_bid`=25996, 'BD',
                    `clean_alias_all_bid`=142391, 'MINO',
                    `clean_alias_all_bid`=5435727, 'Q-MIX',
                    `clean_alias_all_bid`=155859, 'QIXI',
                    `clean_alias_all_bid`=103426, 'SAP',
                    `clean_alias_all_bid`=2093098, 'COKOEYE',
                    `clean_alias_all_bid`=5838804, 'LaPeche',
                    `clean_alias_all_bid`=1705090, 'HanGee',
                    `clean_alias_all_bid`=2446994, 'NAATAA',
                    `clean_alias_all_bid`=20501, 'NEO',
                    `clean_alias_all_bid`=36731, 'EYEYOUNG',
                    `clean_alias_all_bid`=469416, 'SAUFLON',
                    `clean_alias_all_bid`=66067, 'GEO',
                    `clean_alias_all_bid`=2178963, 'MIRU',
                    `clean_alias_all_bid`=1367341, 'SHUIYANGEQING',
                `sp品牌（英文）`),
                `sp厂商（中文）` = multiIf(
                    `clean_alias_all_bid`=469419, '乐敦清',
                    `clean_alias_all_bid`=929401, '齐能',
                    `clean_alias_all_bid`=338371, 'abon',
                    `clean_alias_all_bid`=18962, '蔡司',
                    `clean_alias_all_bid`=143021, '珍视明',
                    `clean_alias_all_bid`=815506, '科尔视',
                    `clean_alias_all_bid`=4870842, '可丽维',
                    `clean_alias_all_bid`=510424, '晶硕',
                    `clean_alias_all_bid`=3911890, '美妆彩片',
                    `clean_alias_all_bid`=2999371, '维视达康',
                    `clean_alias_all_bid`=380611, '实瞳',
                    `clean_alias_all_bid`=311028, '库博',
                    `clean_alias_all_bid`=1940884, '媞娜',
                    `clean_alias_all_bid`=130598, '海昌',
                    `clean_alias_all_bid`=677737, '美若康',
                    `clean_alias_all_bid`=26, 'other',
                    `clean_alias_all_bid`=1006041, '睛盾',
                    `clean_alias_all_bid`=246718, '福瑞达',
                    `clean_alias_all_bid`=31692, '多多',
                    `clean_alias_all_bid`=380601, '艾爵',
                    `clean_alias_all_bid`=5887593, '欧朗睛',
                    `clean_alias_all_bid`=48200, '雅培',
                    `clean_alias_all_bid`=206718, 'LUCKY EYE',
                    `clean_alias_all_bid`=252467, '陌陌',
                    `clean_alias_all_bid`=4880484, '培磊能',
                    `clean_alias_all_bid`=1051637, '润',
                    `clean_alias_all_bid`=6071652, '强生',
                    `clean_alias_all_bid`=4571284, '镜特舒',
                    `clean_alias_all_bid`=4897428, '微爱',
                    `clean_alias_all_bid`=1617108, '卡洛尼',
                    `clean_alias_all_bid`=663032, '优舒',
                    `clean_alias_all_bid`=82018, '明亮',
                    `clean_alias_all_bid`=218544, '韩后',
                    `clean_alias_all_bid`=127760, '芳姿',
                    `clean_alias_all_bid`=406005, '上元堂',
                    `clean_alias_all_bid`=380589, '康视达',
                    `clean_alias_all_bid`=124444, '妃爱丽',
                    `clean_alias_all_bid`=109463, '卫康',
                    `clean_alias_all_bid`=3179376, '咔薇乐',
                    `clean_alias_all_bid`=288907, '新视界',
                    `clean_alias_all_bid`=201237, '爱尔康',
                    `clean_alias_all_bid`=380596, '保视宁',
                    `clean_alias_all_bid`=5259537, '蕾美',
                    `clean_alias_all_bid`=98219, '美汐',
                    `clean_alias_all_bid`=2378893, '艾乐视',
                    `clean_alias_all_bid`=2769031, '淘气猴',
                    `clean_alias_all_bid`=182773, 'sweetcolor',
                    `clean_alias_all_bid`=1669, '糯米',
                    `clean_alias_all_bid`=52390, '强生',
                    `clean_alias_all_bid`=469414, '依视明',
                    `clean_alias_all_bid`=243393, '博士伦',
                    `clean_alias_all_bid`=380579, '科莱博',
                    `clean_alias_all_bid`=52529, '优能',
                    `clean_alias_all_bid`=1533123, '魅瞳',
                    `clean_alias_all_bid`=2531108, '花魁',
                    `clean_alias_all_bid`=3312878, '培克能',
                    `clean_alias_all_bid`=176723, '海俪恩',
                    `clean_alias_all_bid`=487825, '昕薇女孩',
                    `clean_alias_all_bid`=5172691, '绮芙莉',
                    `clean_alias_all_bid`=484054, '吉春黄金',
                    `clean_alias_all_bid`=3312880, '米路可爱',
                    `clean_alias_all_bid`=341445, '葵花药业',
                    `clean_alias_all_bid`=431917, '美尼康',
                    `clean_alias_all_bid`=3249282, '珂俪维',
                    `clean_alias_all_bid`=4954, '蓝睛灵',
                    `clean_alias_all_bid`=243071, '可啦啦',
                    `clean_alias_all_bid`=5336044, '奥普铁克',
                    `clean_alias_all_bid`=1983289, '澜柏',
                    `clean_alias_all_bid`=3312881, '美睛尊品',
                    `clean_alias_all_bid`=25996, 'BD',
                    `clean_alias_all_bid`=142391, '美侬',
                    `clean_alias_all_bid`=5435727, 'Q-MIX',
                    `clean_alias_all_bid`=155859, '七夕',
                    `clean_alias_all_bid`=103426, '思汉普',
                    `clean_alias_all_bid`=2093098, '珂珂爱',
                    `clean_alias_all_bid`=5838804, '安目瞳',
                    `clean_alias_all_bid`=1705090, '韩姬儿',
                    `clean_alias_all_bid`=2446994, '娜他她',
                    `clean_alias_all_bid`=20501, '可视眸',
                    `clean_alias_all_bid`=36731, '爱漾',
                    `clean_alias_all_bid`=469416, '沙福隆',
                    `clean_alias_all_bid`=66067, 'GEO',
                    `clean_alias_all_bid`=2178963, '米如',
                    `clean_alias_all_bid`=1367341, '水氧E清',
                `sp厂商（中文）`),
                `sp厂商（英文）` = multiIf(
                    `clean_alias_all_bid`=469419, 'Rohto',
                    `clean_alias_all_bid`=929401, 'QINENG',
                    `clean_alias_all_bid`=338371, 'abon',
                    `clean_alias_all_bid`=18962, 'Zeiss',
                    `clean_alias_all_bid`=143021, 'ZSM',
                    `clean_alias_all_bid`=815506, '科尔视',
                    `clean_alias_all_bid`=4870842, 'KELIWEI',
                    `clean_alias_all_bid`=510424, 'PEGAVISION',
                    `clean_alias_all_bid`=3911890, 'COSME CONTACT',
                    `clean_alias_all_bid`=2999371, 'VISTACON',
                    `clean_alias_all_bid`=380611, 'SEED',
                    `clean_alias_all_bid`=311028, 'COOPER',
                    `clean_alias_all_bid`=1940884, 'TINA',
                    `clean_alias_all_bid`=130598, 'HYDRON',
                    `clean_alias_all_bid`=677737, 'Miacare',
                    `clean_alias_all_bid`=26, 'other',
                    `clean_alias_all_bid`=1006041, 'Mydeskmate',
                    `clean_alias_all_bid`=246718, 'Freda',
                    `clean_alias_all_bid`=31692, '多多',
                    `clean_alias_all_bid`=380601, 'AIJUE',
                    `clean_alias_all_bid`=5887593, 'OLENS',
                    `clean_alias_all_bid`=48200, 'Abbott',
                    `clean_alias_all_bid`=206718, 'LUCKY EYE',
                    `clean_alias_all_bid`=252467, 'Mo',
                    `clean_alias_all_bid`=4880484, 'PEILEINENG',
                    `clean_alias_all_bid`=1051637, 'RUN',
                    `clean_alias_all_bid`=6071652, 'J&J',
                    `clean_alias_all_bid`=4571284, 'JINGTESHU',
                    `clean_alias_all_bid`=4897428, 'WEIAI',
                    `clean_alias_all_bid`=1617108, 'KALUONI',
                    `clean_alias_all_bid`=663032, 'YOUSHU',
                    `clean_alias_all_bid`=82018, 'MINGLIANG',
                    `clean_alias_all_bid`=218544, 'HAN HOU',
                    `clean_alias_all_bid`=127760, 'Fancy',
                    `clean_alias_all_bid`=406005, 'SHANGYUANTANG',
                    `clean_alias_all_bid`=380589, 'Constar',
                    `clean_alias_all_bid`=124444, 'FAIRY',
                    `clean_alias_all_bid`=109463, 'WEICON',
                    `clean_alias_all_bid`=3179376, 'KAWEILE',
                    `clean_alias_all_bid`=288907, 'XINSHIJIE',
                    `clean_alias_all_bid`=201237, 'ALCON',
                    `clean_alias_all_bid`=380596, 'Porslen',
                    `clean_alias_all_bid`=5259537, 'ReVIA',
                    `clean_alias_all_bid`=98219, 'MeiXi',
                    `clean_alias_all_bid`=2378893, 'I LOVE EYES',
                    `clean_alias_all_bid`=2769031, 'Taoqihou',
                    `clean_alias_all_bid`=182773, 'sweetcolor',
                    `clean_alias_all_bid`=1669, 'nomi',
                    `clean_alias_all_bid`=52390, 'J&J',
                    `clean_alias_all_bid`=469414, 'Yes Moon',
                    `clean_alias_all_bid`=243393, 'B+L',
                    `clean_alias_all_bid`=380579, 'CLB',
                    `clean_alias_all_bid`=52529, 'Visine',
                    `clean_alias_all_bid`=1533123, 'Merry Dolly',
                    `clean_alias_all_bid`=2531108, '花魁',
                    `clean_alias_all_bid`=3312878, 'Bioclen',
                    `clean_alias_all_bid`=176723, 'HORIEN',
                    `clean_alias_all_bid`=487825, 'XINWEINVHAI',
                    `clean_alias_all_bid`=5172691, '绮芙莉',
                    `clean_alias_all_bid`=484054, '吉春黄金',
                    `clean_alias_all_bid`=3312880, 'Milook-eye',
                    `clean_alias_all_bid`=341445, 'KUIHUA',
                    `clean_alias_all_bid`=431917, 'Menicon',
                    `clean_alias_all_bid`=3249282, 'Clear Vealer',
                    `clean_alias_all_bid`=4954, 'Blue Fairy',
                    `clean_alias_all_bid`=243071, 'Kilala',
                    `clean_alias_all_bid`=5336044, 'AOPUTIEKE',
                    `clean_alias_all_bid`=1983289, 'Lambor',
                    `clean_alias_all_bid`=3312881, 'MEIJINGZUNPIN',
                    `clean_alias_all_bid`=25996, 'BD',
                    `clean_alias_all_bid`=142391, 'MINO',
                    `clean_alias_all_bid`=5435727, 'Q-MIX',
                    `clean_alias_all_bid`=155859, 'QIXI',
                    `clean_alias_all_bid`=103426, 'SAP',
                    `clean_alias_all_bid`=2093098, 'COKOEYE',
                    `clean_alias_all_bid`=5838804, 'LaPeche',
                    `clean_alias_all_bid`=1705090, 'HanGee',
                    `clean_alias_all_bid`=2446994, 'NAATAA',
                    `clean_alias_all_bid`=20501, 'NEO',
                    `clean_alias_all_bid`=36731, 'EYEYOUNG',
                    `clean_alias_all_bid`=469416, 'SAUFLON',
                    `clean_alias_all_bid`=66067, 'GEO',
                    `clean_alias_all_bid`=2178963, 'MIRU',
                    `clean_alias_all_bid`=1367341, 'SHUIYANGEQING',
                `sp厂商（英文）`)
            WHERE 1
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        for key in [['品牌（中文）', '品牌（英文）'], ['厂商（中文）', '厂商（英文）'], ['品牌（英文）', '品牌（中文）'], ['厂商（英文）', '厂商（中文）']]:
            sql = '''
                ALTER TABLE {tbl} UPDATE `sp{check_word}` = `sp{replace_word}`
                WHERE `sp{check_word}` = 'other'
            '''.format(tbl=tbl, check_word=key[0], replace_word=key[1])
            dba.execute(sql)
            while not self.cleaner.check_mutations_end(dba, tbl):
                time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `spisSample 试戴片` = 'Y'
            WHERE (clean_alias_all_bid, cid, `spsku bsl`, `sp片数`, `sp抛弃周期英文`) IN (
                (243393,50023751,'博乐纯','2','Daily'),
                (243393,50023751,'清朗一日','2','Daily'),
                (243393,50023753,'蕾丝炫眸+蕾丝明眸','7','Daily'),
                (243393,50023753,'蕾丝明眸','2','Daily'),
                (243393,50023753,'蕾丝明眸日抛Crystal同款','2','Daily'),
                (243393,50023753,'蕾丝炫眸','2','Daily'),
                (243393,50023753,'蕾丝炫眸','4','Daily')
            )
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

        sql = '''
            ALTER TABLE {} UPDATE `spsku bsl` = multiIf( `sp原始SKU-列表机洗`!='', `sp原始SKU-列表机洗`, `sp原始SKU-规整后`='', '其它', `sp原始SKU-规整后` )
            WHERE (clean_brush_id = 0 OR `spsku bsl` = '') AND (
                clean_alias_all_bid not in (201237,243393,130598,52390,109463,2178963,176723,311028,380611,677737,243071,6071652, 2879494,5366648,6153805)
                OR ( clean_alias_all_bid in (5366648) and cid in (50023751,126246001) )
                OR ( clean_alias_all_bid in (2178963) and cid in (50023753,126248001) )
            )
        '''.format(tbl)
        dba.execute(sql)

        while not self.cleaner.check_mutations_end(dba, tbl):
            time.sleep(5)

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


    def finish_new(self, tbl, dba, prefix):

        for sp3,sp1 in [['卫康', '彩片'], ['可啦啦', '透明片'], ['欧朗睛', '透明片'], ['米如', '彩片'], ['艾乐视', '透明片']]:
            sql = '''
            alter table {tbl} update `sp是否是博士伦关注品牌` = '否'
            where `spPlatform` = '{sp1}' and `sp品牌` = '{sp3}'
            '''.format(tbl=tbl, sp1=sp1, sp3=sp3)
            dba.execute(sql)
            while not self.cleaner.check_mutations_end(dba, tbl):
                time.sleep(5)

# truncate TABLE artificial_new.`entity_90613_item`;
# INSERT INTO artificial_new.`entity_90613_item` (
# tb_item_id,source,sid,shop_name,shop_type,real_cid,super_bid,sub_brand,trade,visible,real_p1,spid2,spid3,spid4,spid5,spid6,spid7,spid10,spid11,spid12,spid13,spid17,spid18,spid19,spid21,spid22,spid23,spid25,spid26,spid27,spid28,pid,
# p1,month,name,spid1,cid,brand,all_bid,alias_all_bid,spid9,spid8,spid16,spid24,avg_price,sales,num,spid14,spid15,img,uid,modified,spid20,
# batch_id,flag,is_set,tip
# ) SELECT
# tb_item_id,'tmall',0,'',0,0,0,0,0,1,0,'','','','','','','','','','','','','','','','','','','','',0,
# p3,date,name,cid,cid,brand,alias_all_bid,alias_all_bid,p6,sku_new,sku_new2,is_set,sales/num,sales,num,p_num,pack_num,img,mid,modified,si,
# 0,1,0,1
# FROM artificial_new.entity_90481_manual WHERE mid NOT IN (0,777);

# UPDATE artificial_new.entity_90613_item SET spid1 = '透明片' WHERE spid1 IN ('50023751','126246001');
# UPDATE artificial_new.entity_90613_item SET spid1 = '彩片' WHERE spid1 IN ('50023753','126248001');
# UPDATE artificial_new.entity_90613_item SET spid1 = '护理液' WHERE spid1 = '50023752';
# UPDATE artificial_new.entity_90613_item SET spid1 = '其它' WHERE spid1 = '0';
# UPDATE artificial_new.entity_90613_item SET spid20 = IF(spid20 = '1', '是', '否');
# UPDATE artificial_new.entity_90613_item SET spid24 = IF(spid24 = '2', '镜护联合', IF(spid24 = '1', 'Y', 'N'));

# /bin/mysqldump -h 192.168.128.18 -ucleanAdmin -p6DiloKlm artificial_new entity_90613_item > /obsfs/90613.sql
# /bin/mysql -h 192.168.30.93 -ucleanAdmin -p6DiloKlm product_lib_ali < /obsfs/90613.sql

# ALTER TABLE product_lib_ali.entity_90613_item ADD COLUMN trade_prop_all text NOT NULL AFTER product;

# CREATE TABLE product_lib_hw.entity_90613_item LIKE product_lib.entity_90613_item;
# p3 scripts/conver_brush_ali2hw.py