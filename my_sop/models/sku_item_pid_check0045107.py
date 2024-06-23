# coding=utf-8
import sys
from os.path import abspath, join, dirname, exists

sys.path.insert(0, join(abspath(dirname('__file__')), '../'))

import sys
from os.path import abspath, join, dirname, exists
from models.report import common
from extensions.utils import transfer_list_to_dict
import application as app
from extensions import utils
import time
import pandas as pd
import ast
import re

pid_check_list = set()
re_skip = re.compile("(^___)")  # 以__开头的
re_range = re.compile("([\.0-9]+\-[\.0-9]+[*]{1})")  # 范围的

# 91357奶粉规格检查
re_naifen_product = re.compile("([\.0-9]+[g]{1})")
re_naifen = re.compile("([\.0-9]+[*]{1})")  # 数量词

# 将单位都统一
def uni_standard(testg: str):
    testg = testg.replace('g', '*')
    testg = testg.replace('克', '*')
    testg = testg.replace('G', '*')
    testg = testg.replace('ml', '*')
    testg = testg.replace('毫升', '*')
    return testg


# 将结果转换为dataframe
def rlt2df(rlt, clm):
    # rlt是个元组，clm是列名
    # name特异化文件命名
    tmp = pd.DataFrame(columns=clm, data=rlt)
    return tmp


# 将结果输出为csv
def rlt2csv(rlt, clm, name):
    # rlt是个元组，clm是列名
    # name特异化文件命名
    tmp = pd.DataFrame(columns=clm, data=rlt)
    name = map(str, name)
    name_merge = '_'.join(name)
    # tmp.head()
    tmp.to_csv('./{}.csv'.format(name_merge), encoding='utf-8', index=False)


## 检查sku库 ##
# 根据选择的spid
# 检查name字段和对应的spid是否一致
# 返回不一致的tuple
# pid_check_list添加的一定是有规格信息的
def product_check(db_, eid: int, spid: str):
    error_list = set()
    if eid == 91357:
        # 奶粉数据的规格检测需要考虑spid8
        sql = 'select pid, name, {0}, spid8 from product_lib.product_{1}'.format(spid, eid)
        product_info_list = db_.query_all(sql)
        for product_info_true in product_info_list:
            product_info = list(product_info_true)
            if re_skip.match(product_info[1]):
                continue
            # sku比对不需要单位统一了
            if re_naifen_product.match(product_info[2]) and product_info[2] in re_naifen_product.findall(
                    product_info[1]):
                pid_check_list.add(product_info[0])
                continue
            elif re_naifen_product.match(product_info[3]) and product_info[3] in re_naifen_product.findall(
                    product_info[1]):
                pid_check_list.add(product_info[0])
                continue
            else:
                if re_naifen_product.match(product_info[2]) or re_naifen_product.match(
                        product_info[3]) or re_naifen_product.findall(product_info[1]):
                    pid_check_list.add(product_info[0])
                    product_info.append('规格错误')
                else:
                    product_info.append('未提及规格')
                error_list.add(tuple(product_info))

        columns = ('pid', 'name', spid, 'spid8', '错误原因')
    else:
        sql = 'select pid, name, {0} from product_lib.product_{1}'.format(spid, eid)
        product_info_list = db_.query_all(sql)
        for product_info_true in product_info_list:
            product_info = list(product_info_true)
            if re_naifen_product.match(product_info[1]):
                continue
            if re_naifen_product.match(product_info[2]) and product_info[2] in re_naifen_product.findall(
                    product_info[1]):
                pid_check_list.add(product_info[0])
                continue
            else:
                if re_naifen_product.match(product_info[2]) or re_naifen_product.findall(product_info[1]):
                    pid_check_list.add(product_info[0])
                    product_info.append('规格错误')
                else:
                    product_info.append('未提及规格')
                error_list.add(tuple(product_info))
        columns = ('pid', 'name', spid, '错误原因')
    return error_list, columns


### 检查宝贝答题item表&检查pid ###
## 宝贝答题部分，先判断flag分为洗属性还是洗sku
# 再根据split分为套包或非套包
# 四种默认为对的情况，1、product表name和属性都没有规格
# 2、product表name以下划线开头
# 3、product有规格，item表name和交易属性都没有规格
# 4、product有规格，item表中有范围规格
## 检查pid部分
# 相同pid的item有一个提及了product中的规格，这个pid就算对
def item_pid_check(db_, eid: int, spid: str):
    item_error_list = set()
    item_columns = tuple()

    sql = 'select id, name, trade_prop_all, pid, {0}, flag, split, img, month, source, tb_item_id from product_lib.entity_{1}_item'.format(
        spid, eid)
    artificial_info_list = db_.query_all(sql)

    sql = 'select pid, name, {0} from product_lib.product_{1}'.format(spid, eid)
    product_info_list = db_.query_all(sql)
    # 按照pid建立索引字典
    product_info_dict = transfer_list_to_dict(product_info_list, use_row=True, simple=True)

    sql = 'select entity_id, pid from product_lib.entity_{0}_item_split'.format(eid)
    entityid_pid = db_.query_all(sql)
    # 按照entity_id建立索引字典
    entityid_pid_dict = transfer_list_to_dict(entityid_pid, use_row=False, simple=True)

    for artificial_info_true in artificial_info_list:
        artificial_info = list(artificial_info_true)
        artificial_info[1] = uni_standard(artificial_info[1])
        artificial_info[2] = uni_standard(artificial_info[2])
        artificial_ = artificial_info[1] + ',' + artificial_info[2]
        if artificial_info[5] == 1:  # flag=1 洗属性
            if artificial_info[4] and artificial_info[4] in re_naifen.findall(artificial_info[2]):
                continue
            elif not re_naifen.findall(artificial_info[2]) and artificial_info[4] in re_naifen.findall(
                    artificial_info[1]):
                continue
            else:
                bind_info = artificial_info[0:10]
                bind_info.append(common.get_link(artificial_info[-2], artificial_info[-1]))
                item_error_list.add(tuple(bind_info))
            item_columns = (
                'id', 'name', 'trade_prop_all', 'pid', spid, 'flag', 'split', 'img', 'month', 'source', 'item_link')
        else:  # 洗sku, flag=2
            if artificial_info[6] == 0:  # split=0 不是套包
                # pid: artificial_info[3]
                pid_ = artificial_info[3]
            else:  # split=1 是套包
                # product_info_dict: {pid:(pid, name, spid18)}
                # entityid_pid_dict: {entity_id: pid}
                pid_ = entityid_pid_dict.get(artificial_info[0])

            # 都没有提到规格或者有范围，就算对
            if not re_naifen.findall(artificial_) or re_range.findall(artificial_):
                continue
            # 以下划线开头的跳过
            if re_skip.match(product_info_dict.get(pid_)[1]):
                continue
            # product_info_dict: {pid:(pid, name, spid18)}
            product_info = product_info_dict.get(pid_)[1] + ',' + product_info_dict.get(pid_)[2]
            product_info = uni_standard(product_info)
            if not re_naifen.findall(product_info):  # 假如宝贝信息没有规格，就直接下一个了
                continue
            else:
                if re_naifen.findall(artificial_info[2]):
                    if re_naifen.findall(product_info)[-1] in re_naifen.findall(artificial_info[2]):
                        pid_check_list.discard(pid_)
                        continue
                    else:
                        bind_info = list(artificial_info_true[0:3])
                        bind_info.append(pid_)
                        bind_info.extend(list(artificial_info_true[5:10]))
                        bind_info.append(common.get_link(artificial_info[-2], artificial_info[-1]))
                        bind_info.extend(list(product_info_dict.get(pid_)[1:]))
                        item_error_list.add(tuple(bind_info))
                else:
                    if re_naifen.findall(product_info)[-1] in re_naifen.findall(artificial_info[1]):
                        pid_check_list.discard(pid_)
                        continue
                    else:
                        bind_info = list(artificial_info_true[0:3])
                        bind_info.append(pid_)
                        bind_info.extend(list(artificial_info_true[5:10]))
                        bind_info.append(common.get_link(artificial_info[-2], artificial_info[-1]))
                        bind_info.extend(list(product_info_dict.get(pid_)[1:]))
                        item_error_list.add(tuple(bind_info))
            item_columns = (
                'id', 'item_name', 'trade_prop_all', 'pid', 'flag', 'split', 'img', 'month', 'source', 'item_link',
                'product_name', spid)

    pid_list = tuple(pid_check_list)
    pid_data = []
    # 可能会有长度限制
    sql = 'select pid, name, {0} from product_lib.product_{1} where pid in {2}'.format(spid, eid, pid_list)
    product_info_list = db_.query_all(sql)
    product_info_dict = transfer_list_to_dict(product_info_list, use_row=True, simple=True)
    sql = 'select pid, num, name, trade_prop_all, img, source, tb_item_id from product_lib.entity_{0}_item where pid in {1}'.format(
        eid, pid_list)
    item_info_list = db_.query_all(sql)
    item_info_dict = transfer_list_to_dict(item_info_list, use_row=True, simple=True)
    for pid_ in pid_list:
        tmp = list(product_info_dict.get(pid_))
        if pid_ in item_info_dict.keys():
            tmp.extend(item_info_dict.get(pid_)[1:5])
            tmp.append(common.get_link(item_info_dict.get(pid_)[-2], item_info_dict.get(pid_)[-1]))
        pid_data.append(tmp)
    pid_columns = ('pid', 'name', spid, '宝贝数', '名称', '交易属性', '图片', '链接')

    return item_error_list, item_columns, pid_data, pid_columns


def main(eid: int, spid: int):

    spid_ = 'spid' + str(spid)
    product_error, product_column = product_check(eid, spid_)
    if product_error:
        file_name = ('product', eid, spid_, 'error')
        rlt2csv(product_error, product_column, file_name)
        # product_error_dataframe = rlt2df(product_error, product_column)
    else:
        print("未检测到sku库中宝贝信息不一致的文件")

    item_error, item_column, pid_error, pid_column = item_pid_check(eid, spid_)
    if item_error:
        file_name = ('item_check', eid, spid_, 'error')
        rlt2csv(item_error, item_column, file_name)
        # artificial_error_dataframe = rlt2df(artificial_error, artificial_column)
    if pid_error:
        file_name = ('pid_check', eid, spid_)
        rlt2csv(pid_error, pid_column, file_name)
    # return product_error_dataframe, artificial_error_dataframe

if __name__ == '__main__':
    db_default = app.get_db('default')
    db_default.connect()
    test_eid = 91357
    test_spid = 18
    main(test_eid, test_spid)
