import asyncio
import re
from datetime import datetime, timedelta
import os
import sys

from component.DBPool import DBPool
from component.SqlCache import SqlCache
from component.BrandDict import brand_dict2
from component.Log import Log

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

async def generate_brand_dict():
    brand_list = await db_pool.queryAll('db_basic', "select id, name, alias from tag_brand where id not in (304)")
    for brand in brand_list:
        names = [word_format(brand['name'])]
        if brand['name'] != brand['alias']:
            names.append(word_format(brand['alias']))
        # 特殊品牌特殊处理
        if brand['name'] == '逆色':
            names.append('nyx')
        if brand['name'] == '魅可':
            names.append('mac')
        week_brand = ['小美盒', '肌活', '肌水', '凡士林', '欧诗漫']
        names = [word_format(v) for v in names if v != '']
        week_flag = False
        if brand['name'] in week_brand:
            week_flag = True
        brand_dict.append(brand['id'], names, week_flag)


def word_format(string):
    return re.sub(u'\s{2,}', ' ', re.sub(u'[^\w\s\.\ô\Ô\-\'’\&\+\:\˚\é]', '', string.lower()))


async def deal_industry_items(semaphore, mid):
    async with semaphore:
        if mid == 1351:
            await do_deal_industry_items_qsi()
        else:
            await do_deal_industry_items_indus(mid)


async def do_deal_industry_items_qsi():
    logger.info('QSI deal items start')
    sql = """select shop_id from basic_brand_shop group by shop_id"""
    ignore_shop_list = await db_pool.queryColumn('db_basic', sql)
    ignore_shop_list_str = ','.join([str(i) for i in ignore_shop_list])
    now = datetime.now()
    start_date = (now - timedelta(days=15)).strftime('%Y%m%d')
    end_date = (now - timedelta(days=1)).strftime('%Y%m%d')
    item_id_iterator = ''
    while True:
        sql = f"""select item_id, shop_id, max(si.name) as item_name from folder f
                    join qsi_solidify_item si force index (idx_folder) on f.id = si.folder_id
                where (f.name = '唯一总库' or f.name like '%%机器清洗%%') and shop_id not in ({ignore_shop_list_str})
                    and date between {start_date} and {end_date}
                    and item_id > '{item_id_iterator}'
                group by item_id
                order by item_id
                limit 500"""
        item_list = await db_pool.queryAll('db_qsi', sql)
        if item_list is None:
            return
        for item in item_list:
            item_id_iterator = item['item_id']
            shop_name = await cache.fillOne('shop', item['shop_id'])
            if shop_name is None:
                shop_name = 'not found'
            bids, bnames = brand_dict.match2(word_format(item['item_name']))
            if bids:
                insert_data = [(item['item_id'], item['item_name'], item['shop_id'], shop_name, b, k) for k, b in
                               enumerate(bids)]
                sql = """insert into item_brand_from_machine_origin2 (item_id, item_name, shop_id, shop_name, brand_id, level) 
                values (%s, %s, %s, %s, %s, %s) as col 
                on duplicate key update item_name = col.item_name,shop_name = col.shop_name"""
                await db_pool.execute('db_test', sql, insert_data)
            else:
                insert_data = (item['item_id'], item['item_name'], item['shop_id'], shop_name, 0, 0)
                sql = """insert into item_brand_from_machine_origin2 (item_id, item_name, shop_id, shop_name, brand_id, level) 
                values (%s, %s, %s, %s, %s, %s) as col 
                on duplicate key update item_name = col.item_name,shop_name = col.shop_name"""
                await db_pool.execute('db_test', sql, insert_data)


async def do_deal_industry_items_indus(mid):
    logger.info(f'{mid} deal items start')
    sql = """select shop_id from basic_brand_shop group by shop_id"""
    ignore_shop_list = await db_pool.queryColumn('db_basic', sql)
    ignore_shop_list_str = ','.join([str(i) for i in ignore_shop_list])
    now = datetime.now()
    start_date = (now - timedelta(days=15)).strftime('%Y%m%d')
    end_date = (now - timedelta(days=1)).strftime('%Y%m%d')
    item_id_iterator = ''
    while True:
        sql = f"""select item_id, shop_id, max(si.name) as item_name from folder f
                    join industry_solidify_item si on f.id = si.folder_id and f.mid = si.mid
                where f.name = '唯一总库' and shop_id not in ({ignore_shop_list_str})
                    and date between {start_date} and {end_date}
                    and item_id > '{item_id_iterator}'
                    and f.mid = {mid}
                group by item_id
                order by item_id
                limit 500"""
        item_list = await db_pool.queryAll('db_indus', sql)
        if item_list is None:
            return
        for item in item_list:
            item_id_iterator = item['item_id']
            shop_name = await cache.fillOne('shop', item['shop_id'])
            if shop_name is None:
                shop_name = 'not found'
            bids, bnames = brand_dict.match2(word_format(item['item_name']))
            if bids:
                insert_data = [(item['item_id'], item['item_name'], item['shop_id'], shop_name, b, k) for k, b in
                               enumerate(bids)]
                sql = """insert into item_brand_from_machine_origin2 (item_id, item_name, shop_id, shop_name, brand_id, level) 
                values (%s, %s, %s, %s, %s, %s) as col 
                on duplicate key update item_name = col.item_name,shop_name = col.shop_name"""
                await db_pool.execute('db_test', sql, insert_data)
            else:
                insert_data = (item['item_id'], item['item_name'], item['shop_id'], shop_name, 0, 0)
                sql = """insert into item_brand_from_machine_origin2 (item_id, item_name, shop_id, shop_name, brand_id, level) 
                values (%s, %s, %s, %s, %s, %s) as col 
                on duplicate key update item_name = col.item_name,shop_name = col.shop_name"""
                await db_pool.execute('db_test', sql, insert_data)


async def main():
    industry_list = await db_pool.queryColumn('db_basic', 'select * from sync_industry')
    # 生成品牌字典
    await generate_brand_dict()
    # 最大并发数
    semaphore = asyncio.Semaphore(4)
    tasks = []

    for industry in industry_list:
        task = asyncio.create_task(deal_industry_items(semaphore, industry))
        tasks.append(task)
    await asyncio.gather(*tasks)
    logger.info('tasks all finished')


loop = asyncio.get_event_loop()
logger = Log('BasicBrand')
db_pool = DBPool()
cache = SqlCache()
brand_dict = brand_dict2()

if __name__ == '__main__':
    loop.run_until_complete(main())
