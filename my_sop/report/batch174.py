# -*- coding: utf-8 -*-
def get_uuid2(start_date,end_date):
    start_date = start_date
    end_date = end_date
    sql_list = [f"""SELECT * FROM(
    SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY `平台`,`sp一级品类`,`sp二级品类`,`sp品牌定位` ORDER BY `销售额` DESC) AS row_num
        FROM (
            SELECT 
            case 
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
                when source = 16 then 'xinlvgou'
                else '其他' end as `平台`,`sp一级品类`,`sp二级品类`,`sp品牌定位`,item_id,argMax(name,num) as `name`,argMax(uuid2,num) as uuid2,sum(num) as `销量`,sum(sales)/100 as `销售额`
            FROM sop_e.entity_prod_91363_E_20210804
            WHERE (pkey>='{start_date}' and pkey<'{end_date}'
            and (source*100+shop_type in (109,121,122,123,124,125,126,127,128) or source in (2,11) or item_id in ('696928476220'))
            and `sp一级品类` in ['Face','Lip','Makeup set','Eye','Nails'])
            GROUP BY `平台`,`sp一级品类`,`sp二级品类`,`sp品牌定位`,item_id) subquery)
        WHERE row_num<=15
        ORDER BY `平台`,`sp一级品类`,`sp二级品类`,`sp品牌定位` ASC,`销售额` DESC"""
    ,f"""SELECT * FROM(
    SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY `平台`,`sp一级品类`,`sp二级品类`,`sp品牌定位` ORDER BY `销售额` DESC) AS row_num
        FROM (
            SELECT 
            case 
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
                when source = 16 then 'xinlvgou'
                else '其他' end as `平台`,`sp一级品类`,`sp二级品类`,`sp品牌定位`,item_id,argMax(name,num) as `name`,argMax(uuid2,num) as uuid2,sum(num) as `销量`,sum(sales)/100 as `销售额`
            FROM sop_e.entity_prod_91363_E_20210804
            WHERE (pkey>='{start_date}' and pkey<'{end_date}'
            and source in (2,12,16)
            and `sp一级品类` in ['Face','Lip','Makeup set','Eye','Nails','Fragrance']
            and `sp品牌定位` in ['Prestige'])
            GROUP BY `平台`,`sp一级品类`,`sp二级品类`,`sp品牌定位`,item_id) subquery)
        WHERE row_num<=15
        ORDER BY `平台`,`sp一级品类`,`sp二级品类`,`sp品牌定位` ASC,`销售额` DESC"""
    ,f"""SELECT * FROM(
    SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY `平台`,`sp一级品类`,`sp品牌定位`,alias_all_bid ORDER BY `销售额` DESC) AS row_num
        FROM (
            SELECT 
            case 
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
                when source = 16 then 'xinlvgou'
                else '其他' end as `平台`,`sp一级品类`,`sp二级品类`,`sp品牌定位`,alias_all_bid,dictGetOrDefault('all_brand', 'name', tuple(toUInt32(alias_all_bid)), 'Others') as `品牌`,item_id,argMax(name,num) as `name`,argMax(uuid2,num) as uuid2,sum(num) as `销量`,sum(sales)/100 as `销售额`
            FROM sop_e.entity_prod_91363_E_20210804
            WHERE (pkey>='{start_date}' and pkey<'{end_date}'
            and source*100+shop_type in (109,121,122,123,124,125,126,127,128)
            and `sp一级品类` in ['Fragrance']
            and `sp品牌定位` in ['Prestige']
            and alias_all_bid in (SELECT alias_all_bid FROM(
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY `平台` ORDER BY `销售额` DESC) AS row_num
        FROM (
            SELECT 
            case 
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
                when source = 16 then 'xinlvgou'
                else '其他' end as `平台`,alias_all_bid,sum(num) as `销量`,sum(sales)/100 as `销售额` FROM sop_e.entity_prod_91363_E_20210804
            WHERE (pkey>='{start_date}' and pkey<'{end_date}'
            and source*100+shop_type in (109,121,122,123,124,125,126,127,128)
            and `sp一级品类` in ['Fragrance']
            and `sp品牌定位` in ['Prestige'])
            GROUP BY `平台`,alias_all_bid) brand_tb)
        WHERE row_num<=10))
        GROUP BY `平台`,`sp一级品类`,`sp二级品类`,`sp品牌定位`,alias_all_bid,item_id) subquery)
    WHERE row_num<=10
    ORDER BY `平台`,`sp一级品类`,`sp二级品类`,`sp品牌定位`,alias_all_bid ASC,`销售额` DESC"""
    ,f"""SELECT * FROM(
    SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY `平台`,`sp一级品类`,`sp品牌定位`,alias_all_bid ORDER BY `销售额` DESC) AS row_num
        FROM (
            SELECT 
            case 
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
                when source = 16 then 'xinlvgou'
                else '其他' end as `平台`,`sp一级品类`,`sp二级品类`,`sp品牌定位`,alias_all_bid,dictGetOrDefault('all_brand', 'name', tuple(toUInt32(alias_all_bid)), 'Others') as `品牌`,item_id,argMax(name,num) as `name`,argMax(uuid2,num) as uuid2,sum(num) as `销量`,sum(sales)/100 as `销售额`
            FROM sop_e.entity_prod_91363_E_20210804
            WHERE (pkey>='{start_date}' and pkey<'{end_date}'
            and source=11
            and `sp一级品类` in ['Fragrance']
            and `sp品牌定位` in ['Prestige']
            and alias_all_bid in (SELECT alias_all_bid FROM(
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY `平台` ORDER BY `销售额` DESC) AS row_num
        FROM (
            SELECT 
            case 
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
                when source = 16 then 'xinlvgou'
                else '其他' end as `平台`,alias_all_bid,sum(num) as `销量`,sum(sales)/100 as `销售额` FROM sop_e.entity_prod_91363_E_20210804
            WHERE (pkey>='{start_date}' and pkey<'{end_date}'
            and source=11
            and `sp一级品类` in ['Fragrance']
            and `sp品牌定位` in ['Prestige'])
            GROUP BY `平台`,alias_all_bid) brand_tb)
        WHERE row_num<=10))
        GROUP BY `平台`,`sp一级品类`,`sp二级品类`,`sp品牌定位`,alias_all_bid,item_id) subquery)
    WHERE row_num<=10
    ORDER BY `平台`,`sp一级品类`,`sp二级品类`,`sp品牌定位`,alias_all_bid ASC,`销售额` DESC"""
    ,f"""SELECT * FROM(
    SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY `平台`,`sp一级品类`,`sp品牌定位` ORDER BY `销售额` DESC) AS row_num
        FROM (
            SELECT 
            case 
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
                when source = 16 then 'xinlvgou'
                else '其他' end as `平台`,`sp一级品类`,`sp品牌定位`,item_id,argMax(name,num) as `name`,argMax(uuid2,num) as uuid2,sum(num) as `销量`,sum(sales)/100 as `销售额`
            FROM sop_e.entity_prod_91363_E_20210804
            WHERE (pkey>='{start_date}' and pkey<'{end_date}'
            and (source*100+shop_type in (109,121,122,123,124,125,126,127,128) or source in (2,11,8,25))
            and `sp一级品类` in ['Cleansing','Cream','Devices','Emulsion& Fluids','Essence & Serum','Eye & Lip Care','Lotion& Toner','Mask','Men Skincare','Skincare Set','Suncare']
            and `sp品牌定位`='Prestige')
            GROUP BY `平台`,`sp一级品类`,`sp品牌定位`,item_id) subquery)
        WHERE row_num<=15
        ORDER BY `平台`,`sp一级品类`,`sp品牌定位` ASC,`销售额` DESC
    """]
    # -*- coding: utf-8 -*-
    from clickhouse_sqlalchemy import make_session
    from sqlalchemy import create_engine,text
    import pandas as pd
    import concurrent.futures
    conf = {
        "user": "admin",
        "password": "7kvx4GTg",
        "server_host": "10.21.90.15",
        "port": "10081",
        "db": "sop"
    }
    connection = 'clickhouse://{user}:{password}@{server_host}:{port}/{db}'.format(**conf)
    engine = create_engine(connection, pool_size=100, pool_recycle=3600, pool_timeout=20)

    def execute_sql(sql):
        session = make_session(engine)
        cursor = session.execute(sql)
        try:
            fields = cursor._metadata.keys
            mydata = pd.DataFrame([dict(zip(fields, item)) for item in cursor.fetchall()])
            if mydata.shape[0] == 0:
                mydata['uuid2'] = []
            return mydata['uuid2']
        finally:
            cursor.close()
            session.close()

    # 使用线程池执行 SQL 查询
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(execute_sql, text(sql)) for sql in sql_list]
        results = [future.result() for future in concurrent.futures.as_completed(futures)]
    # 将结果合并
    uuid2_list = pd.concat(results, axis=0, ignore_index=True)
    return uuid2_list.to_list()
#测试输出
# print(len(get_uuid2('2024-04-01','2024-05-01')))