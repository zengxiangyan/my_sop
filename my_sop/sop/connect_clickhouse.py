import json
import pandas as pd
# from create_table import create_table
import asyncio
from concurrent.futures import ThreadPoolExecutor
from clickhouse_sqlalchemy import make_session
from sqlalchemy import create_engine,text

# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')

# import models.plugins.batch as Batch
import sys
import re
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='utf8')
import common
import application as app
# import pymysql
# import re
# 创建一个全局的线程池
executor = ThreadPoolExecutor()


def get_link(self, source=None, item_id=None):
    mp = {
        'tb': "http://item.taobao.com/item.htm?id={}",
        'tmall': "http://detail.tmall.com/item.htm?id={}",
        'jd': "http://item.jd.com/{}.html",
        'beibei': "http://www.beibei.com/detail/00-{}.html",
        'gome': "http://item.gome.com.cn/{}.html",
        'jumei': "http://item.jumei.com/{}.html",
        'kaola': "http://www.kaola.com/product/{}.html",
        'suning': "http://product.suning.com/{}.html",
        'vip': "http://archive-shop.vip.com/detail-0-{}.html",
        'yhd': "http://item.yhd.com/item/{}",
        'tuhu': "https://item.tuhu.cn/Products/{}.html",
        'jx': "http://www.jiuxian.com/goods-{}.html",
        'dy': "https://haohuo.jinritemai.com/views/product/detail?id={}",
        'cdf': "https://www.cdfgsanya.com/product.html?productId={}&goodsId={}",
        'dewu': "https://m.dewu.com/router/product/ProductDetail?spuId={}&skuId={}",
        'sunrise': "-",
        'lvgou': "-",
    }

    if source in ['cdf', 'dewu']:
        id_array = item_id.split("/")
        return mp[source].format(id_array[0], id_array[1])

    if source:
        return mp[source].format(item_id)
    return mp

def sop_e(eid):
    # col_name = action.replace('获取表中','')
    # clean_props_name = ['品牌', '子品类', '是否套包', '是否无效链接', '是否人工答题', '套包宝贝', '店铺分类', '疑似新品',
    #  '店铺分类（子渠道）', '刀头数', '刀架数', '套包类型', '渠道（新）', '系列', '辅助_第一层级', '辅助_第二层级',
    #  '部位-第一层级', '部位-第二层级', 'Segment', 'packsize（眉刀）', '辅助_第二层级（新）', '部位-第二层级（新）',
    #  '辅助_一次性系列', '是否大皂头', '子品类（女刀月报）']
    #
    # if col_name in ['品牌','店铺']:
    #     sql = f"""SELECT alias_all_bid,clean_props.value[indexOf(clean_props.name,'{col_name}')] as `{col_name}`, sum(num) as `销量`,sum(sales)/100 as `销售额` from sop_e.entity_prod_92162_E_0523 group by alias_all_bid,`{col_name}` order by `销售额` desc"""
    # elif col_name in clean_props_name:
    #     sql = f"""SELECT clean_props.value[indexOf(clean_props.name,'{col_name}')] as `{col_name}`, sum(num) as `销量`,sum(sales)/100 as `销售额` from sop_e.entity_prod_92162_E_0523 group by `{col_name}` order by `销售额` desc"""
    print(sql)
    return sql

def sql_create(form):
    eid = form.get('eid')
    table = form.get('table')
    action = form.get('action')
    view_sp = form['view_sp'].split(',')
    default_col = ['国内跨境','平台','店铺类别','sid','alias_all_bid','cid','item_id']
    select, groupby, where = [],[],[""" date>='{}' AND date<'{}' """.format(form['date1'],form['date2'])]
    groupby = []
    limitby = []
    print(view_sp)
    if '获取表中' in action:
        form = form.copy()
        form['分' + action.replace('获取表中', '')] = '分'
        form[action.replace('获取表中', '')] = ''
        form['limit'] = '999999'
        if action.replace('获取表中','') in default_col:
            default_col = [action.replace('获取表中','')]
            view_sp = []
        if action.replace('获取表中', '') in view_sp:
            default_col = []
            view_sp = [action.replace('获取表中', '')]
        where = [' 1 ']
        form['top'] = ['取所有']
        action = 'search'
    if action == 'search':
        if form.get('top') == '取top品牌(alias_all_bid)':
            select += ["alias_all_bid","dictGetOrDefault('all_brand', 'name', tuple(toUInt32(alias_all_bid)), '') `品牌名` "]
            groupby.append('alias_all_bid')

        if form.get('top') == '取top店铺(未清洗)':
            select += ["sid","dictGet('all_shop', 'title', tuple(`source`,sid)) `店铺` "]
            groupby.append('`{}`'.format('店铺'))

        if form.get('top') == '取top宝贝(未清洗)':
            select += ['item_id',"argMax(name,date) AS `宝贝名称`,argMax(img,date) AS `图片` "]
            groupby.append('item_id')


        if form.get('top') == '取top宝贝(交易属性)':
            select += ['item_id', "argMax(name,date) AS `宝贝名称`,arrayStringConcat(`trade_props.value`, '|||') `交易属性`","argMax(img,date) AS `图片` "]
            groupby.append('item_id,"交易属性"')

        if form.get('top') in ['取top宝贝(未清洗)','取top宝贝(交易属性)']:
            select.append(""" 
                multiIf(
                    source = 1 and (shop_type < 20 and shop_type > 10 ), concat('http://item.taobao.com/item.htm?id=', item_id),
                    source = 1, concat('http://detail.tmall.com/item.htm?id=', item_id),
                    source = 2, concat('http://item.jd.com/', item_id, '.html'),
                    source = 3, concat('http://item.gome.com.cn/', item_id, '.html'),
                    source = 4, concat('http://item.jumei.com/', item_id, '.html'),
                    source = 5, concat('http://www.kaola.com/product/', item_id, '.html'),
                    source = 6, concat('http://product.suning.com/', item_id, '.html'),
                    source = 7, concat('http://archive-shop.vip.com/detail-0-', item_id, '.html'),
                    source = 8, concat('https://yangkeduo.com/goods.html?goods_id=',item_id),
                    source = 9, concat('http://www.jiuxian.com/goods-', item_id, '.html'),
                    source = 10, concat('https://item.tuhu.cn/Products/', item_id, '.html'),
                    source = 11, concat('https://haohuo.jinritemai.com/views/product/detail?id=', item_id),
                    source = 12, concat('https://www.cdfgsanya.com/product.html?productId=', splitByString('/', item_id)[1], '&goodsId=', splitByString('/', item_id)[2]),
                    source = 14, concat('https://m.dewu.com/router/product/ProductDetail?spuId=', splitByString('/', item_id)[1], '&skuId=', splitByString('/', item_id)[2]),
                    source = 24, concat('https://app.kwaixiaodian.com/page/kwaishop-buyer-goods-detail-outside?id=',item_id),
                    '-'
                ) AS url""")
            groupby.append('url')
        for sp0 in default_col:
            if form.get('分' + sp0) == '分':
                groupby.append('`{}`'.format(sp0))
                limitby.append('`{}`'.format(sp0))
                if sp0 == '国内跨境':
                    select.append(""" IF(source*100+shop_type IN [109,112,122,124,127,221,222,321,322,412,521,522,621,622,712,821,822,1121,1122], '海外', '国内')  AS "国内跨境" """)
                if sp0 == '平台':
                    select.append(""" transform(IF(source = 1 and (shop_type < 20 and shop_type > 10),0,source),{},{},'')  AS "平台" """.format(list(common.get_source_en().keys()),list(common.get_source_en().values())))
                if sp0 == 'sid':
                    select.append("""sid,dictGet('all_shop', 'title', tuple(`source`,sid)) "店铺" """.format(list(common.get_source_en().keys()),list(common.get_source_en().values())))
                    groupby.append('`{}`'.format('店铺'))
                if sp0 == 'alias_all_bid':
                    select.append("""alias_all_bid,dictGetOrDefault('all_brand', 'name', tuple(toUInt32(alias_all_bid)), '') `品牌名` """.format(list(common.get_source_en().keys()),list(common.get_source_en().values())))
                    groupby.append('`{}`'.format('品牌名'))
                if sp0 == 'cid':
                    select.append(""" cid """.format(list(common.get_source_en().keys()),list(common.get_source_en().values())))
            if form.get(sp0):
                if form.get('是否' + sp0) == '是':
                    where.append( ' `sp{}` in {} '.format(sp,form.get(sp0).split(',')))
                where.append(' `sp{}` not in {} '.format(sp, form.get(sp0).split(',')))

        for sp in view_sp:
            if form.get('分' + sp) == '分':
                groupby.append('`{}`'.format(sp))
                limitby.append('`{}`'.format(sp))
                select.append(' `sp{sp}` AS `{sp}`'.format(sp=sp))
            if form.get(sp):
                if form.get('是否' + sp) == '是':
                    where.append(' `sp{}` in {} '.format(sp,form.get(sp).split(',')))
                if form.get('是否' + sp) == '不是':
                    where.append(' `sp{}` not in {} '.format(sp, form.get(sp).split(',')))
    select = list(set(select + ['sum(num) as `销量`','sum(sales)/100 as `销售额`']))
    if groupby:
        groupby = 'group by ' + ','.join(list(set(groupby)))
    else:
        groupby = ''
    if limitby:
        limitby = 'by ' + ','.join(list(set(limitby)))
    else:
        limitby= ''
    sql = """ 
            SELECT {select}
            from {table} 
            where {where} 
            {groupby}
            order by "销售额" desc
            limit {limit} {limitby};
        """.format(select=','.join(select),table=table,where='\nAND '.join(where),groupby=groupby,limit=form.get('limit'),limitby=limitby)
    return sql

def sql_create_old(form):
    eid = form.get('eid')
    table = form.get('table')
    print(eid,form['action'])
    if form.get('action'):
        if form['action'] not in ['set_view_sp','search']:
            col_name = form['action'].replace('获取表中', '')
            if col_name in ['品牌','店铺','类目']:
                sql = f"""SELECT alias_all_bid,dictGetOrDefault('all_brand', 'name', tuple(toUInt32(alias_all_bid)), '') as `{col_name}`, sum(num) as `销量`,sum(sales)/100 as `销售额` from sop_e.entity_prod_{eid}_E group by alias_all_bid,`{col_name}` order by `销售额` desc"""
            else:
                sql = f"""SELECT `sp{col_name}` as `{col_name}`, sum(num) as `销量`,sum(sales)/100 as `销售额` from {table} group by `{col_name}` order by `销售额` desc"""
        elif form['action'] == 'set_view_sp':
            sql = f"""SELECT `clean_props.name` FROM {table} LIMIT 1 """
        elif form['action'] == 'search':
            limit = json.loads(form['limit'])
            select,group,partition_by,where = '','','',''
            date1 = form['date1']
            date2 = form['date2']
            group_by = form['group_by']
            if (date1 != "") and (date1 != None):
                where += "(`date`>='"+date1+"')"
            if (date2 != "") and (date1 != None):
                if where !='':
                    where += " and (`date`<'" +date2 + "')"
                else:
                    where += "(`date`<'" + date2 + "')"
            if form['分平台'] == '分':
                select += f"""case
            when source = 1 and (shop_type < 20 and shop_type > 10 ) then 'tb'
            when source*100+shop_type in (109,121,122,123,124,125,126,127,128) then 'tmall'
            when source = 2 then 'jd'
            when source = 3 then 'gome'
            when source = 4 then 'jumei'
            when source = 5 then 'kaola'
            when source = 6 then 'suning'
            when source = 7 then 'vip'
            when source = 8 then 'pdd'
            when source = 9 then 'jiuxian'
            when source = 11 then 'dy'
            when source = 12 then 'cdf'
            when source = 14 then 'dw'
            when source = 15 then 'hema'
            when source = 24 then 'ks'
            else '其他' end as `平台`,"""
                if group == '':
                    group += f"`平台`"
                else:
                    group += f",`平台`"
                if partition_by == "":
                    partition_by += "`平台`"
                else:
                    partition_by += ",`平台`"
            if form['分sid'] == '分':
                if 'source' not in select:
                    select += f"""source,sid,dictGet('all_shop', 'title', tuple(toUInt8(`source`), toUInt32(sid))) AS `店铺名`,"""
                else:
                    select += f"""sid,dictGet('all_shop', 'title', tuple(toUInt8(`source`), toUInt32(sid))) AS `店铺名`,"""
                if group == '':
                    group += f"sid,source"
                else:
                    group += f",sid,source"
                if partition_by == "":
                    partition_by += "sid,source"
                else:
                    partition_by += ",sid,source"
            if form['分alias_all_bid'] == '分':
                select += f"""alias_all_bid,dictGetOrDefault('all_brand', 'name', tuple(toUInt32(alias_all_bid)), '') `品牌名`,"""
                if group == '':
                    group += f"alias_all_bid"
                else:
                    group += f",alias_all_bid"
                if partition_by == "":
                    partition_by += "alias_all_bid"
                else:
                    partition_by += ",alias_all_bid"
            if len(form['source'])>=1:
                s_rule = source(form['source'])
                print(form['source'],s_rule)
                if where != '':
                    where += f" AND {s_rule}"
                else:
                    where += f"(` {s_rule})"
            for sp in form['view_sp'].split(','):
                if form.get('分'+sp) == '分':
                    select += f"""`sp{sp}`as `{sp}`,"""
                    if group == '':
                        group += f"`{sp}`"
                    else:
                        group += f",`{sp}`"
                    if partition_by == "":
                        partition_by += f"`{sp}`"
                    else:
                        partition_by += f",`{sp}`"
            if group_by == "取宝贝(交易属性)":
                select = select + """item_id,argMax(name,num) `name`,`trade_props.value` as `交易属性`,
    case
        when source = 1 and shop_type < 20 then CONCAT('https://item.taobao.com/item.htm?id=',item_id)
        when source = 1 and shop_type > 20 then CONCAT('https://detail.tmall.com/item.htm?id=',item_id)
        when source = 2 then CONCAT('https://item.jd.com/',item_id,'.html')
        when source = 3 then CONCAT('https://item.gome.com.cn/',item_id,'.html')
        when source = 4 then CONCAT('//item.jumeiglobal.com/',item_id,'.html')
        when source = 5 then CONCAT('https://goods.kaola.com/product/',item_id,'.html')
        when source = 6 then CONCAT('https://product.suning.com/',item_id,'.html')
        when source = 7 then CONCAT('https://detail.vip.com/detail-',item_id,'.html')
        when source = 8 then CONCAT('https://mobile.yangkeduo.com/goods.html?goods_id=',item_id)
        when source = 9 then CONCAT('www.jiuxian.com/goods.',item_id,'.html')
        when source = 11 then CONCAT('https://haohuo.jinritemai.com/views/product/detail?id=',item_id)
        when source = 24 then CONCAT('https://app.kwaixiaodian.com/page/kwaishop-buyer-goods-detail-outside?id=',item_id)
        else '其他' end as url,argMax(img,num) `img`,"""
                if group == '':
                    group = group + "url,item_id,`交易属性`"
                else:
                    group = group + ",url,item_id,`交易属性`"
                try:
                    if form[sp] != '':
                        if form['是否'+sp] == '是':
                            if where != '':
                                where += f" AND (`sp{sp}` in ('" + form[sp].replace(',',"','") + "'))"
                            else:
                                where += f"(`sp{sp}` in ('" + form[sp].replace(',', "','") + "'))"
                        else:
                            if where != '':
                                where += f" AND (`sp{sp}` not in (" + form[sp] + "))"
                            else:
                                where += f"(`sp{sp}` not in (" + form[sp] + "))"
                except:
                    print("未添加清洗字段查询")
            if where == '' or group=='':
                sql = f"""SELECT {select} sum(num) as `销量`,sum(sales)/100 as `销售额` from {table}"""
            if group_by == "取所有":
                if group == "":
                    sql = f"""SELECT {select} sum(num) as `销量`,sum(sales)/100 as `销售额` from {table} where ({where})"""
                else:
                    sql = f"""SELECT {select} sum(num) as `销量`,sum(sales)/100 as `销售额` from {table} where ({where}) group by {group}"""
            if group_by == "取宝贝(交易属性)":
                sql = f"""SELECT * FROM(
    SELECT {select} sum(num) as `销量`,sum(sales)/100 as `销售额`,ROW_NUMBER() OVER (PARTITION BY {partition_by} ORDER BY `销售额` DESC) AS row_num 
    from {table} 
    where ({where}) 
    group by {group}) subqery
WHERE row_num <= {limit} """
        sql += " order by `销售额` desc"
    print(sql)
    return sql

async def async_connect(n,sql):

    # connection_mysql = pymysql.connect(host='10.21.90.130',
    #                                    user='zxy',
    #                                    password='13639054279zxy',
    #                                    db='my_sop')
    #
    # # 创建游标对象
    # cursor_mysql = connection_mysql.cursor()
    conf = [{
        "user": "admin",
        "password": "7kvx4GTg",
        "server_host": "127.0.0.1",
        "port": "10081",
        "db": "sop_e"
    },
        {
            "user": "yinglina",
            "password": "xfUW5GMr",
            "server_host": "127.0.0.1",
            "port": "10192",
            "db": "sop"
        }]
    connection = 'clickhouse://{user}:{password}@{server_host}:{port}/{db}'.format(**conf[n])
    engine = create_engine(connection, pool_size=100, pool_recycle=3600, pool_timeout=20)
    session = make_session(engine)
    cursor = session.execute(sql)
    try:
        fields = cursor._metadata.keys
        mydata = pd.DataFrame([dict(zip(fields, item)) for item in cursor.fetchall()])
        return mydata
    finally:
        cursor.close()
        session.close()
def connect(db,sql,as_dict=True):
    if db in ['cleaner','brush','channel','dy2','jd']:
        try:
            db = app.connect_db('default')
            res = db.query_all(sql, as_dict=as_dict)
        except Exception as e:
            db = app.connect_db('default')
            res = db.execute(sql)
            return res
        except Exception as res:
            print(res)
    elif db in ['oulaiya','wq']:
        try:
            db = app.connect_db('oulaiya')
            res = db.query_all(sql, as_dict=as_dict)
        except Exception as e:
            db = app.connect_db('oulaiya')
            res = db.execute(sql)
            return res
        except Exception as res:
            print(res)
    else:
        try:
            chsop = app.connect_clickhouse(db)
            res = chsop.query_all(sql,as_dict=as_dict)
        except Exception as e:
            chsop = app.connect_clickhouse(db)
            res = chsop.execute(sql)
            return res
        except Exception as res:
            print(res)
    return res

def source(pt):
    list = {'tb':"111,112",
            'tmall':"109,121,122,123,124,125,126,127,128",
            'jd':"211,212,221,222",
            'dy':"1111",
            'gome':"311,312,321,322",
            'jumei':"411,412",
            'kaola':"511,512,521,522",
            'suning':"611,612,621,622",
            'vip':"711,712",
            'pdd':"811,812,821,822",
            'jiuxian':"911,912",
            'tuhu':"1011",
            'cdf':"1211",
            'dewu':"1411",
            'hema':"1511"
            }
    return "(source*100+shop_type in ("+ ",".join([list[s] for s in pt])+"))"
#
# sql = f"""show tables  LIKE '%92376\_E%'"""
#
# print(connect(0,sql))
# async def run():
#     cursor = await connect(0,sql)
#     print("table_list",cursor['name'])
#
#
# asyncio.run(run())
if __name__ == '__main__':
    form = {
        'csrfmiddlewaretoken': 'HKpq5ZFvQd2QYKCFfNu9lb3ssfMqEqnlifOlHFTdYAoc19qQPBmGUXpO8Wq3av1z',
        'top': '取所有',
        'limit': '20',
        '分时间': '不分',
        'date1': '2023-01-01',
        'date2': '2023-11-01',
        '分国内跨境': '分',
        '分平台': '分',
        '分店铺类别': '不分店铺类别',
        '分sid': '分',
        '是否sid': '是',
        'sid': '',
        '分alias_all_bid': '分',
        '是否alias_all_bid': '是',
        'alias_all_bid': '',
        '分cid': '不分',
        '是否cid': '是',
        'cid': '',
        'name_words': '',
        'and_or': ['且', '且'],
        'p1_words': '',
        'name_not_words': ['', ''],
        'p1_not_words': '',
        '分功效': '不分',
        '是否功效': '是',
        '功效': '',
        '分子品类': '分',
        '是否子品类': '是',
        '子品类': '',
        '分是否无效链接': '不分',
        '是否是否无效链接': '是',
        '是否无效链接': '',
        '分是否人工答题': '不分',
        '是否是否人工答题': '是',
        '是否人工答题': '',
        '分品牌定位': '不分',
        '是否品牌定位': '是',
        '品牌定位': '',
        '分SKU（不出数）': '不分',
        '是否SKU（不出数）': '是',
        'SKU（不出数）': '',
        '分馥绿德雅取数限制': '不分',
        '是否馥绿德雅取数限制': '是',
        '馥绿德雅取数限制': '',
        '分功效-修复修护': '不分',
        '是否功效-修复修护': '是',
        '功效-修复修护': '',
        '分功效-去屑止痒': '不分',
        '是否功效-去屑止痒': '是',
        '功效-去屑止痒': '',
        '分功效-固色护色': '不分',
        '是否功效-固色护色': '是',
        '功效-固色护色': '',
        '分功效-强韧头发': '不分',
        '是否功效-强韧头发': '是',
        '功效-强韧头发': '',
        '分功效-柔顺保湿/柔顺滋润': '不分',
        '是否功效-柔顺保湿/柔顺滋润': '是',
        '功效-柔顺保湿/柔顺滋润': '',
        '分功效-深层清洁': '不分',
        '是否功效-深层清洁': '是',
        '功效-深层清洁': '',
        '分功效-清爽控油': '不分',
        '是否功效-清爽控油': '是',
        '功效-清爽控油': '',
        '分功效-留香去异味': '不分',
        '是否功效-留香去异味': '是',
        '功效-留香去异味': '',
        '分功效-舒缓抗炎': '不分',
        '是否功效-舒缓抗炎': '是',
        '功效-舒缓抗炎': '',
        '分功效-蓬松丰盈': '不分',
        '是否功效-蓬松丰盈': '是',
        '功效-蓬松丰盈': '',
        '分功效-防脱生发': '不分',
        '是否功效-防脱生发': '是',
        '功效-防脱生发': '',
        'eid': '91559',
        'action': 'search',
        'table': 'entity_prod_91559_E',
        'source': '',
        'view_sp': '功效,子品类,是否无效链接,是否人工答题,品牌定位,SKU（不出数）,馥绿德雅取数限制,功效-修复修护,功效-去屑止痒,功效-固色护色,功效-强韧头发,功效-柔顺保湿/柔顺滋润,功效-深层清洁,功效-清爽控油,功效-留香去异味,功效-舒缓抗炎,功效-蓬松丰盈,功效-防脱生发'
    }
    sql_create(form)