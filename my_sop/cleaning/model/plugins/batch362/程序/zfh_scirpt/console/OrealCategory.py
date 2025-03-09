import asyncio
import copy
import re
import math
import aiomysql
from collections import defaultdict
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(current_dir, '../'))
from component.DBPool import DBPool
from component.Tool import excel_to_list
from component.Tool import choose_file
from component.Log import Log


# file = None
file='../data/美发四级关键词.xlsx'
# 文件可以代码里指定 例如 file='../data/美发四级关键词.xlsx'
table_name = 'makeupall2'# 测试：'makeupall22'；正式：'makeupall2'
#date = None
date = '2025-02-01'  # 时间不写就是跑全量


async def main():
    rule_list = get_rule_list(file)
    tasks = []
    semaphore = asyncio.Semaphore(8)
    for rule in rule_list:
        tasks.append(asyncio.create_task(rule_search(semaphore, rule)))
    await asyncio.gather(*tasks)
    logger.info('all finished')


def get_rule_list(file=None):
    if file:
        excel_path = file
    else:
        excel_path = choose_file('选择规则文件')
    excel_data = excel_to_list(excel_path)
    rule_list = []
    c4 = ''
    c6 = ''
    c6_trade = ''
    process_set = []
    and_condition = []
    for row_data in excel_data:
        c4_new = row_data['原分类C4']
        c6_new = row_data['name清洗后三级类目c6']
        c6_trade_new = row_data['交易属性清洗后三级类目c6_trade']
        if c4 != c4_new or c6 != c6_new or c6_trade != c6_trade_new:
            if not (c4 == '' and c6 == '' and c6_trade == ''):
                rule_list.append({
                    "c4": c4,
                    "c6": c6,
                    "c6_trade": c6_trade,
                    "rule_list": process_set
                })
                process_set = []
            c4 = c4_new
            c6 = c6_new
            c6_trade = c6_trade_new

        rule_value = []
        for column, cell in row_data.items():
            if column.startswith('value') and cell != '':
                rule_value.append(cell)
        value = '|'.join(rule_value)
        and_condition.append({"value": value, "type": row_data['规则判断']})
        rule_group_id = row_data['规则组']
        rule_order = row_data['规则顺序']
        if row_data["继续处理"] != '是':
            process_set.append({
                "result": row_data['分类结果'],
                "rule_group_id": rule_group_id,
                "rule_order": rule_order,
                "rules": and_condition,
            })
            and_condition = []

    rule_list.append({
        "c4": c4,
        "c6": c6,
        "c6_trade": c6_trade,
        "rule_list": process_set
    })

    # 竟然要去重
    merged_data = defaultdict(lambda: {"c4": None, "c6": None, "c6_trade": None, "rule_list": []})

    for item in rule_list:
        key = (item["c4"], item["c6"], item["c6_trade"])
        if merged_data[key]["c6"] is None:
            merged_data[key]["c4"] = item["c4"]
            merged_data[key]["c6"] = item["c6"]
            merged_data[key]["c6_trade"] = item["c6_trade"]
        merged_data[key]["rule_list"].extend(item["rule_list"])

    rule_list = list(merged_data.values())

    return rule_list


async def rule_search(semaphore, rule):
    async with semaphore:
        await do_rule_search(rule)


async def do_rule_search(rule):
    logger.info(f"c4='{rule['c4']}' c6='{rule['c6']}' c6_trade='{rule['c6_trade']}' 开始处理......")
    name = ''
    conn = await db_pool.get_connection('db_wq')
    cursor = await conn.cursor(aiomysql.DictCursor)
    await cursor.execute('SET SESSION group_concat_max_len = 100000')
    while True:
        param = [name]
        where = 'name > %s'
        if date:
            param.append(date)
            where += ' and time = %s'
        if rule['c4'] != '':
            param.append(rule['c4'])
            where += ' and c4 = %s'
        if rule['c6'] != '':
            param.append(rule['c6'])
            where += ' and c6 = %s'
        if rule['c6_trade'] != '':
            param.append(rule['c6_trade'])
            where += ' and c6_trade = %s'

        sql = f"""
        select max(c4) as c4, max(c6) as c6,max(c6_trade) as c6_trade,name,GROUP_CONCAT(newno) as newnos
            from {table_name}
        where {where}
        group by name
        order by name
        limit 10000"""
        await cursor.execute(sql, tuple(param))
        result = await cursor.fetchall()

        if not result:
            break

        for res_one in result:
            name = res_one['name']

            decision = rule_decision(name, rule)
            if decision:
                sql = f"update {table_name} set category_2 = %s where newno in ({res_one['newnos']})"
                await db_pool.execute('db_wq', sql, (decision['result']))
                logger.debug(
                    f"c4='{rule['c4']}' c6='{rule['c6']}' c6_trade='{rule['c6_trade']}' name={name} ------> 规则组={decision['rule_group_id']} 规则组顺序={decision['rule_order']} 结果={decision['result']}")
    db_pool.close_connection('db_wq', conn)
    logger.info(f"c4='{rule['c4']}' c6='{rule['c6']}' c6_trade='{rule['c6_trade']}' 处理完成######")


def rule_decision(name, rule):
    for rule_one in rule['rule_list']:
        flag = True
        for condition in rule_one['rules']:
            if condition['value'] == '':
                break
            if condition['type'] == '包含':
                flag = re.match(rf"^.*?{condition['value']}.*?$", name) is not None
            elif condition['type'] == '不包含':
                flag = re.match(rf"^(?!.*?{condition['value']}).*?$", name) is not None
            if not flag:
                break
        if flag:
            return rule_one
    return False


logger = Log('OrealCategory')
loop = asyncio.get_event_loop()
db_pool = DBPool()

if __name__ == '__main__':
    loop.run_until_complete(main())
