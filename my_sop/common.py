# -*- coding: utf-8 -*-
import pandas as pd
import requests

def get_source_en(source=None, shop_type=None):
    mp = {
        0: 'tb',
        1: 'tmall',
        2: 'jd',
        3: 'gome',
        4: 'jumei',
        5: 'kaola',
        6: 'suning',
        7: 'vip',
        8: 'pdd',
        9: 'jx',
        10: 'tuhu',
        11: 'dy',
        12: 'cdf',
        13: 'lvgou',
        14: 'dewu',
        15: 'hema',
        16: 'sunrise',
        17: 'test17',
        18: 'test18',
        19: 'test19',
        24: 'ks',
        999: '',
    }
    if source is not None:
        if int(source) == 1 and shop_type and int(shop_type) < 20 and int(shop_type) > 10:
            source = 0
        return mp[int(source)]
    return mp

def get_source_cn(source=None, shop_type=None):
    mp = {
        0: '淘宝',
        1: '天猫',
        2: '京东',
        3: '国美',
        4: '聚美',
        5: '考拉',
        6: '苏宁',
        7: '唯品会',
        8: '拼多多',
        9: '酒仙',
        10: '途虎',
        11: '抖音',
        12: 'cdf',
        13: '旅购日上',
        14: '得物',
        15: '盒马',
        16: '新旅购',
        17: 'test17',
        18: 'test18',
        19: 'test19',
        24: '快手',
        999: '',
    }
    if source:
        if int(source) == 1 and shop_type and int(shop_type) < 20 and int(shop_type) > 10:
            source = 0
        return mp[int(source)]
    return mp

def get_shoptype(source=None, shop_type=None):
    mp = {
        1: {11: '淘宝', 12: '淘宝', 21: '天猫', 22: '天猫国际', 23: '天猫超市', 24: '天猫国际', 25: '天猫',26: '猫享自营', 27: '猫享自营国际', 28: '阿里健康国内', 9: '阿里健康国际'},
        2: {11: '京东国内自营', 12: '京东国内POP', 21: '京东海外自营', 22: '京东海外POP'},
        3: {11: '国美国内自营', 12: '国美国内POP', 21: '国美海外自营', 22: '国美海外POP'},
        4: {11: '聚美国内自营', 12: '聚美海外自营'},
        5: {11: '考拉国内自营', 12: '考拉国内POP', 21: '考拉海外自营', 22: '考拉海外POP'},
        6: {11: '苏宁国内自营', 12: '苏宁国内POP', 21: '苏宁海外自营', 22: '苏宁海外POP'},
        7: {11: '唯品会国内自营', 12: '唯品会海外自营'},
        8: {11: '拼多多国内自营', 12: '拼多多国内POP', 21: '拼多多海外自营', 22: '拼多多海外POP'},
        9: {11: '酒仙自营', 12: '酒仙非自营'},
        10: {11: '途虎自营'},
        11: {11: '抖音国内普通', 12: '抖音国内自营', 21: '抖音国际普通', 22: '抖音国际自营'},
        12: {11: 'cdf'},
        13: {11: '旅购日上优选', 12: '旅购日上上海'},
        14: {11: '得物'},
        15: {11: '盒马'},
        16: {11: '新旅购国内购', 21: '新旅购跨境购'},
        17: {11: 'test17'},
        18: {11: 'test18'},
        19: {11: 'test19'},
        24: {11: '快手'},
        999: {},
    }
    if source:
        return mp[int(source)][int(shop_type)]
    return mp

def get_link(source=None, item_id=None):
    mp = {
        'tb'     : "http://item.taobao.com/item.htm?id={}",
        'tmall'  : "http://detail.tmall.com/item.htm?id={}",
        'jd'     : "http://item.jd.com/{}.html",
        'beibei' : "http://www.beibei.com/detail/00-{}.html",
        'gome'   : "http://item.gome.com.cn/{}.html",
        'jumei'  : "http://item.jumei.com/{}.html",
        'kaola'  : "http://www.kaola.com/product/{}.html",
        'suning' : "http://product.suning.com/{}.html",
        'vip'    : "http://archive-shop.vip.com/detail-0-{}.html",
        'yhd'    : "http://item.yhd.com/item/{}",
        'tuhu'   : "https://item.tuhu.cn/Products/{}.html",
        'jx'     : "http://www.jiuxian.com/goods-{}.html",
        'dy'     : "https://haohuo.jinritemai.com/views/product/detail?id={}",
        'cdf'    : "https://www.cdfgsanya.com/product.html?productId={}&goodsId={}",
        'dewu'   : "https://m.dewu.com/router/product/ProductDetail?spuId={}&skuId={}",
        'pdd'    : "https://yangkeduo.com/goods.html?goods_id={}",
        'sunrise':"-",
        'lvgou': "-",
    }

    if source in ['cdf','dewu']:
        id_array = item_id.split("/")
        return mp[source].format(id_array[0], id_array[1])

    if source:
        return mp[source].format(item_id)
    return mp

# 获取DataFrame中的平台中文名称示例
def source_cn(data):

    if isinstance(data, pd.DataFrame):
        pass
    else:
        data = pd.DataFrame(data)

    if ('source' in data.columns) and ('shop_type' in data.columns):
        if '平台' not in data.columns:
            data['平台'] = ''
        source_shoptype = data[['source', 'shop_type']].drop_duplicates().to_numpy()
        for source, shop_type in source_shoptype:
            data.loc[(data['source'] == source) & (data['shop_type'] == shop_type), '平台'] = get_source_cn(source=source,shop_type=shop_type)

    return data

# 获取DataFrame中的平台英文名称示例
def source_en(data):

    if isinstance(data, pd.DataFrame):
        pass
    else:
        data = pd.DataFrame(data)

    if ('source' in data.columns) and ('shop_type' in data.columns):
        if '平台' not in data.columns:
            data['平台'] = ''
        source_shoptype = data[['source', 'shop_type']].drop_duplicates().to_numpy()
        for source, shop_type in source_shoptype:
            data.loc[(data['source'] == source) & (data['shop_type'] == shop_type), '平台'] = get_source_en(source=source,shop_type=shop_type)

        return data

# 获取DataFrame中的对应平台item_id的链接示例，默认获取链接时会先获取平台英文名称
def source_link(data):

    data = source_en(data)
    if ('source' in data.columns) and ('shop_type' in data.columns) and ('item_id' in data.columns):
        if 'url' not in data.columns:
            data['url'] = ''
        source_itemid = data[['平台', 'item_id']].drop_duplicates().to_numpy()
        for source, item_id in source_itemid:
            data.loc[(data['source'] == source) & (data['item_id'] == item_id), 'url'] = get_link(source=source,item_id=item_id)

        return data

# 获取指定数据库范围下的cid类目名称示例
def get_cid_name(data):

    headers = {'name': 'nint', 'password': 'chen.weihong'}
    url = 'http://10.21.90.130:8888/report/lvname/'
    lvname= requests.post(url, json=data, headers=headers).json()

    return lvname.get('data')

# 获取DataFrame中的对应平台cid的类目名称示例，默认获取链接时会先获取平台英文名称
def cid_name(data,json,retry=0,retry_max=2):

    data = source_en(data)
    while retry<retry_max:
        try:
            if 'cid' in data.columns:
                lv_name = pd.DataFrame(get_cid_name(json))
                data = pd.merge(data, lv_name, on=['平台', 'cid'], how='left')

            return data
        except:
            retry+=1
    return data


