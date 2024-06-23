# coding=utf-8
import collections
import json
import sys, io
import time
from os.path import abspath, join, dirname

import requests

sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf8')

from extensions import utils
import application as app
import pandas as pd
from models.report import query_item


# 对同一价格段的数进行合并操作:
def merge_same_price_range(data, min_value, max_value):
    output = collections.defaultdict(lambda:[])
    tmp_sum0 = 0
    tmp_sum1 = 0
    for key, item_value in data.items():
        for pr, value_list in item_value.items():
            tmp_sum0 += int(value_list[0])
            tmp_sum1 += int(value_list[1])
    output['{}-{}'.format(min_value, max_value)] = [tmp_sum0, tmp_sum1]
    return output


def get_data(db, common_p, overseas, internal):
    '''
    :return: dict  如：{'2020-01-01': {'玉芝琳': [37290, 15279]},...} 列表中的第一元素是2020-01-01的销量，第二个元素是2019-01-01的销量（同比）
    '''
    sp, cp_index = common_p['fields']  # sp: 比较的维度（品牌/品类/属性）  cp_index: 比较的对象（销量/销售额）
    price_range = common_p['price_range'] # 价格段
    oversea = common_p['overseas']  # 国内/海外
    platform = common_p['where']['source']  # 平台：tb, tmall, jd,...

    if oversea == 0:  # 国内
        where_dict = {'source':platform, ('source', 'shop_type'): internal}
    else:
        where_dict = {'source':platform,  ('source', 'shop_type'): overseas}
    # where_list = {'(source, shop_type)':"not in {}".format(overseas)}
    if sp == 'price':
        data = {}
        index = 0
        for price_list in price_range:
            min_price, max_price = price_list

            where_dict['price'] = {'compare':{'min':min_price*100, 'max_exclude':max_price*100}}
            p = utils.merge(common_p, {
                'fields': [
                    'date',
                    # "transform(multiIf(source!=1,source,(shop_type<20 and shop_type>10),0,1), [0,1,2,3,4,5,6,7,8,9,10], ['tb','tmall','jd','gome','jumei','kaola','suning','vip','pdd','jx','tuhu'],'' ) as source2",
                    # "clean_props.value[indexOf(clean_props.name, '{sp}')] as sp".format(sp=sp),
                    "multiIf(price>={a}*100 and price<{b}*100, '{a}-{b}','') as sp".format(a=min_price,b=max_price),  # 价格段
                    "sum(sign*{index}) as total".format(index=cp_index)
                ],
                'group_by': ['date', 'sp'],
                'order_by': ['total desc'],
                'where': where_dict,
                'top': 10000
            })
            data0 = query_item.query_trend_report(db, p)
            if index == 0:
                data = data0
            else:
                for key, item in data0.items():
                    name = list(item.keys())[0]
                    data[key][name] = item[name]
            index += 1

        return data
    else:
        p = utils.merge(common_p, {
            'fields': [
                'date',
                # "transform(multiIf(source!=1,source,(shop_type<20 and shop_type>10),0,1), [0,1,2,3,4,5,6,7,8,9,10], ['tb','tmall','jd','gome','jumei','kaola','suning','vip','pdd','jx','tuhu'],'' ) as source2",
                "clean_props.value[indexOf(clean_props.name, '{sp}')] as sp".format(sp=sp),
                "sum(sign*{index}) as total".format(index=cp_index)
            ],
            'group_by': ['date', 'sp'],
            'order_by': ['total desc'],
            'where': where_dict,
            'top': 10000
        })
        data = query_item.query_trend_report(db, p)
        return data


# 数据处理----按销量或销售额降序取top10；求总体基期值；环比/同比增长率；拉动增长率
def data_processing(data, common_p):
    cp_index = common_p['fields'][1]

    tmp_dict = collections.defaultdict(lambda: [])
    base_on_total = 0  # 总体的基期值
    for key, item_dict in data.items():
        for i, item in item_dict.items():
            tmp_dict[i].append(item)
            base_on_total += item[1]

    total_dict = collections.defaultdict(lambda: [])  # 按照比较的维度（品牌/品类等）分类汇总
    for p1, p1_list in tmp_dict.items():
        if p1 not in ('其他','其它',''):
            t1 = 0
            t2 = 0
            for pp1 in p1_list:
                t1 += pp1[0]
                t2 += pp1[1]
            total_dict[p1] = [t1, t2]

    # 按照当期的销量或销售额取top 10
    top_dt = pd.DataFrame(total_dict).T.sort_values(0, axis=0, ascending=False)[:10]

    top_list = top_dt.index.to_list()
    incre_rate = collections.defaultdict(lambda: []) # 同比/环比增速

    if cp_index == 'num':
        for top in top_list:
            top_item = total_dict[top]
            rate = (top_item[0]-top_item[1])/top_item[1]*100 if top_item[1] != 0 else '-'  # 同比/环比增长率
            ld_rate = top_item[0]/base_on_total*100  # 拉动增长率
            incre_rate[top] = [top_item[0], rate, ld_rate]  # 返回的结果：dict {品牌/品类/属性:[销售额/销量， 环比/同比增长率, 拉动增长率]}
    else:  # 销售额的数值除以100  以‘元’为单位
        for top in top_list:
            top_item = total_dict[top]
            rate = (top_item[0]-top_item[1])/top_item[1]*100 if top_item[1] != 0 else '-'  # 同比/环比增长率
            ld_rate = top_item[0]/base_on_total*100  # 拉动增长率
            incre_rate[top] = [top_item[0]/100, rate, ld_rate]  # 返回的结果：dict {品牌/品类/属性:[销售额/销量， 环比/同比增长率, 拉动增长率]}
    return incre_rate

def main(h_db, p):

    overseas = [[1, 12], [1, 22], [1, 24], [2, 21], [2, 22], [3, 21], [3, 22], [4, 12],
                [5, 21], [5, 22], [6, 21], [6, 22], [7, 12], [8, 21], [8, 22]]  # 海外的(source, shop_type)全部组合
    internal = [[1, 11], [1, 21], [1, 23], [1, 25], [2, 11], [2, 12], [3, 11], [3, 12], [4, 11], [5, 11],
                [5, 12], [6, 11], [6, 12], [7, 11], [8, 11], [8, 12],[9,11],[9,12],[10,11]]  # 国内的(source, shop_type)全部组合

    compare_dimension = ['品牌', '子品类', '分类', 'price']
    all_sp_result_dict = collections.defaultdict(lambda: [])
    for sp0 in compare_dimension:

        common_p = utils.merge(p,{
            'fields': ['{}'.format(sp0), 'sales'],
        })

        raw_data = get_data(h_db, common_p, overseas, internal)
        result = data_processing(raw_data, common_p)  # 返回的结果：dict {品牌/品类/属性:[销售额/销量， 环比/同比增长率, 拉动增长率]}
        all_sp_result_dict[sp0] = result

    # 页面左侧的四个维度各自的top1
    all_dimension_top1 = collections.defaultdict(lambda: [])
    for sp1, item1 in all_sp_result_dict.items():
        key0 = list(item1.keys())[0]  # 销售额/销量排名第一的
        total, growth, ld_growth = item1[key0]  # 拉动增长率
        all_dimension_top1[sp1] = [key0, ld_growth/100]

    return all_dimension_top1, all_sp_result_dict

if __name__ == '__main__':
    start_time0 = time.time()
    c_195 = app.get_clickhouse('chmaster')
    utils.easy_call([c_195], 'connect')
    h_db = {'chmaster': c_195}
    p = {
        'type': 6,
        'eid': 90591,
        'basis': 1,  # 1：同比 2：环比
        'start_date': '2019-06-01',
        'end_date': '2020-05-01',
        'where': {'source': ['tb']},
        # 'group_by': ["item_id"],
        'overseas': 0,  # 1:海外  0：国内
        'price_range': [[0, 100], [100, 200], [200, 300]]
    }
    # r = requests.post("http://127.0.0.1:8000/api/report_get_trend/",data={'params':json.dumps(p)})
    # r = requests.post("http://119.3.132.13:8000/api/get_growth_rate/",data={'params':json.dumps(p)}, auth=('cleaning', '8YybeTKJ'))
    # r = r.json()
    # print(r)

    main(h_db, p)

    end_time0 = time.time()
    print('all done used:', (end_time0 - start_time0))




