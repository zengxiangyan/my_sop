import time
from datetime import datetime
import argparse
import json
import requests
import sys
from os.path import abspath, join, dirname

sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
import application as app
import models.entity_manager as entity_manager
from extensions import utils
from models.nlp.common import text_normalize
from models.analyze.trie import Trie

url = "http://10.21.200.128:8081/taskflow/cls" if utils.is_windows() else 'http://gpu11.sh.nint.com/taskflow/cls'
headers = {"Content-Type": "application/json"}
db_sop = app.get_clickhouse('chsop')

multi = 1  # 多进程
where = 'alias_all_bid in (7012,3779,2503,11092,20645,10744,2968842,5324,223274,266017,11697,4873,6758,5121986,4473207,1639060,2335055,53946,609944,53384,529151,1933134,3984881,219205,219258,433041,3242356,529153,529155,118403) and source=14'  # 条件比如 cid = xxx
total = 0
result_table = 'sop_c.dlmodel_91783_category_2'


def each_partation(where, params, prate):
    self, tbl, dba, = params

    batch_cnt = 0
    uuid2_list_str = ''
    data = []

    # 取按交易属性排重后的数据
    ret = self.get_data(dba, '', '', tbl, where)
    batch_name = []
    # batch_item_id = []
    batch_uuid2s = []
    batch_pkeys = []
    for item in ret:
        # 排重过了 uuid2需要展开
        uuids, pkeys, dates, saless, nums, prices, org_prices, item = self.format_data(self, item)

        # 清洗一个item, item是如下格式的json
        '''
         {'uuid2': 7062735236213651878, 'pkey': '2022-01-11', 'item_id': '100012796485', 'date': '2022-01-24', 
         'cid': 15601, 'id': 7062735236213651878, 'name': '雷盛677霞多丽干白葡萄酒750ml*6 整箱装 智利进口红酒', 
         'product': '', 'snum': 2, 'month': '2022-01-24', 'all_bid': 6669121, 'brand': '538002', 'sub_brand': 0, 
         'rbid': 0, 'avg_price': 240000.0, 'sales': 2880000, 'num': 12, 'price': 240000, 'org_price': 240000, 
         'region_str': '', 'tb_item_id': '100012796485', 'sid': 0, 'shop_type': 11, 'source': 'jd', 'source_cn': '京东', 
         'all_bid_info': {'bid': 6669121, 'name': '雷盛（Leeson）', 'name_cn': '雷盛', 'name_en': 'Leeson', 'name_cn_front': '雷盛', 'name_en_front': 'Leeson', 'alias_bid': 0}, 
         'alias_all_bid': 6669121, 'alias_all_bid_info': {'bid': 6669121, 'name': '雷盛（Leeson）', 'name_cn': '雷盛', 'name_en': 'Leeson', 'name_cn_front': '雷盛', 'name_en_front': 'Leeson', 'alias_bid': 0}, 
         'sub_brand_name': '', 'shop_type_ch': '京东国内自营', 'shop_name': '京东自营', 'shopkeeper': '', 'extend': None, 'trade_prop_all': {}, 
         'prop_all': {'商品名称': ['雷盛葡萄酒'], '商品编号': ['100012796485'], '商品毛重': ['7.5kg'], '商品产地': ['智利'], '容量': ['750ml'], '类型': ['干型葡萄酒'],
            '包装形式': ['箱装'], '新/旧世界': ['新世界'], '葡萄品种': ['霞多丽（Chardonnay）'], '甜度': ['干型'], '口感': ['其它'], '瓶塞': ['橡木塞'], '国产/进口': ['进口'], 
            '保质期': ['3650天'], '包装清单': ['智利进口红酒 雷盛677霞多丽干白葡萄酒750ml*6 整箱装x1']}}
        '''
        batch_cnt += 1

        # prepare item info needed
        # uuid2 = item['uuid2']
        # item_id = item['item_id']
        name = item['name']
        trade_prop = item['trade_prop_all']
        prop = item['prop_all']

        _name = text_normalize(name)
        _trade_prop = text_normalize(','.join([f"{tpn}:{tpv}" for tpn, tpv in trade_prop.items()]))
        _prop = text_normalize(','.join(
            [f"{pn}:{prop[pn]}" for pn, pv in prop.items() if pn not in trade_prop and trie.search_entity(pn)]))

        input_text = "|".join([_name, _trade_prop, _prop])
        # call model predict, res_list format: [ [sku, pid, dl_similarity, softmax_with_temperature_scaling], ...]

        # batch_item_id.append(item_id)
        batch_name.append(input_text)
        batch_uuid2s.append(uuids)
        batch_pkeys.append(pkeys)

        # # 一个item对应多个uuid2，分uuid2准备插入以及删除结果
        # for i, uuid2 in enumerate(uuids):
        #     # prepare uuids
        #     uuid2_list_str += str(uuid2) + ','
        #     # prepare write back values
        #     data.append([uuid2, item_id, category, score])

    while len(batch_name):
        # process_item_id = batch_item_id[:1000]
        process_pkeys = batch_pkeys[:1000]
        process_name = batch_name[:1000]
        process_uuid2s = batch_uuid2s[:1000]

        input_data = {"data": {"text": process_name}}
        r = requests.post(url=url, headers=headers, data=json.dumps(input_data))
        result_json = json.loads(r.text)
        result_pair = [[r['predictions'][0]['label'], r['predictions'][0]['score']] for r in result_json['result']]
        batch_category, batch_score = zip(*result_pair)

        data = [[datetime.strptime(pkey, '%Y-%m-%d'), uuid, '子品类', category, score]
                for pkeys, uuids, category, score in zip(process_pkeys, process_uuid2s, batch_category, batch_score)
                for pkey, uuid in zip(pkeys, uuids)]

        uuid2_list_str = ''.join([f"{uuid}," for uuids in process_uuid2s for uuid in uuids])
        # 对整个batch的结果做读写操作
        utils.easy_call([db_sop], 'connect')
        # delete old result
        sql_delete_old = f"""ALTER TABLE {result_table} DELETE WHERE uuid2 IN ( {uuid2_list_str} )"""
        db_sop.execute(sql_delete_old)

        while not self.check_mutations_end(db_sop, result_table):
            time.sleep(5)

        # write back result
        sql_insert = f"""
            INSERT INTO {result_table}
            (pkey, uuid2, sp_name, sp_value, score)
            VALUES
        """
        db_sop.execute(sql_insert, data)

        # batch_item_id = batch_item_id[1000:]
        batch_pkeys = batch_pkeys[1000:]
        batch_name = batch_name[1000:]
        batch_uuid2s = batch_uuid2s[1000:]

        print(f"remain {len(batch_name)}")

    # log
    global total
    total += batch_cnt
    end_time = time.time()
    global g_start_time
    used = end_time - g_start_time
    print('current batch count: {} total: {} used:{:.2f}'.format(batch_cnt, total, used))
    sys.stdout.flush()
    if args.test:
        exit()


def test():
    texts = ["我的美丽日记台湾制众星闪耀补水面膜礼盒组16片装 带防伪纾缓保湿晶亮换白（男女通用 送礼礼盒款）"] * 10
    data = {"data": {"text": texts}}
    r = requests.post(url=url, headers=headers, data=json.dumps(data))
    result_json = json.loads(r.text)
    print(result_json["result"])


def main(args):
    batch_id = args.batch_id
    limit = args.limit
    smonth, emonth = args.start_month, args.end_month
    # eid = args.eid
    # month = args.month
    # where = args.where
    global g_start_time
    g_start_time = time.time()

    global trie
    trie = Trie()
    for p in args.use_prop:
        trie.insert(p)

    ent = entity_manager.get_clean(batch_id)  # batch id, 和eid一一对应，但不一样
    dba, tbl = ent.get_plugin().get_all_tbl()
    dba = ent.get_db(dba)

    params = [ent, tbl, dba]
    ent.each_partation(smonth, emonth, each_partation, params, tbl=tbl, where=where, limit=limit, multi=multi, cols=['sid'])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Params for ')
    parser.add_argument('-batch_id', type=str, default='-1', help='batch id for current project')
    parser.add_argument('-start_month', type=str, default='2022-01-01')
    parser.add_argument('-end_month', type=str, default='2023-01-01')
    parser.add_argument('-limit', type=int, default='10000')
    parser.add_argument('-where', type=str, default='1')

    parser.add_argument('-use_trade', type=bool, default='True', help='使用交易属性')
    parser.add_argument('-use_prop', nargs='+', type=str, default=[], help='使用指定属性')

    parser.add_argument('-test', action="store_true", help='test')
    args = parser.parse_args()

    start_time = time.time()
    # main(args)
    test()
    end_time = time.time()
    print('all done used:{:.2f}'.format(end_time - start_time))
# 2022-01 ~ 2022-12
