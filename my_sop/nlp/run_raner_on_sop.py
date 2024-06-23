import time
import argparse
import json
import requests
import sys
from os.path import abspath, join, dirname

sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
import application as app
from extensions import utils
import models.entity_manager as entity_manager
from models.nlp.common import text_normalize
from models.analyze.trie import Trie

host = "http://10.21.200.128:8082/" if utils.is_windows() else 'http://gpu12.sh.nint.com/'
headers = {"Content-Type": "application/json"}
db_sop = app.get_clickhouse('chsop')
multi = 1  # 多进程
where = 'alias_all_bid in (7012,3779,2503,11092,20645,10744,2968842,5324,223274,266017,11697,4873,6758,5121986,4473207,1639060,2335055,53946,609944,53384,529151,1933134,3984881,219205,219258,433041,3242356,529153,529155,118403)'  # 条件比如 cid = xxx
total = 0
result_table = 'sop_c.dlmodel_91783_raner'


def full_item_info(name, trade_prop, prop, columns=[]):
    trie = Trie()
    for p in columns:
        trie.insert(p)

    _name = text_normalize(name)
    _trade_prop = text_normalize(','.join([f"{tpn}:{tpv}" for tpn, tpv in trade_prop.items()]))
    _prop = text_normalize(','.join(
        [f"{pn}:{prop[pn]}" for pn, pv in prop.items() if pn not in trade_prop and trie.search_entity(pn)]))

    return "|".join([_name, _trade_prop, _prop])


def test():
    texts = ["我的美丽日记台湾制众星闪耀补水面膜礼盒组16片装 带防伪纾缓保湿晶亮换白（男女通用 送礼礼盒款）",
             "我的美丽日记台湾制众星闪耀补水面膜礼盒组16片装 带防伪纾缓保湿晶亮换白（男女通用 送礼礼盒款）"]

    data = {"text": texts}
    r = requests.post(host + "api/ner_raner", headers=headers, data=json.dumps(data))
    print(r)
    result_json = json.loads(r.text)
    print(result_json)
    result_pair = [[[e['type'] for e in r], [e['span'] for e in r]] for r in result_json['result']]
    batch_label, batch_mention = zip(*result_pair)

    print(batch_label)
    print(batch_mention)
    # for r in result_json["result"]:
    #     print(r)


def each_partation(where, params, prate):
    self, tbl, dba, = params

    batch_cnt = 0
    uuid2_list_str = ''
    data = []

    # 取按交易属性排重后的数据
    ret = self.get_data(dba, '', '', tbl, where)
    batch_item_id, batch_name, batch_uuid2s, batch_pkeys = [], [], [], []
    for item in ret:
        # 排重过了 uuid2需要展开
        uuids, pkeys, dates, saless, nums, prices, org_prices, item = self.format_data(self, item)

        batch_cnt += 1
        item_id = item['item_id']
        name = item['name']
        trade_prop = item['trade_prop_all']
        prop = item['prop_all']

        full_text = full_item_info(name, trade_prop, prop, columns=args.use_prop)

        batch_item_id.append(item_id)
        batch_name.append(full_text)
        batch_uuid2s.append(uuids)
        batch_pkeys.append(pkeys)

    uuid2_list_str = ''.join([f"{uuid}," for uuids in batch_uuid2s for uuid in uuids])
    while len(batch_name):
        process_item_id = batch_item_id[:512]
        process_pkeys = batch_pkeys[:512]
        process_name = batch_name[:512]
        process_uuid2s = batch_uuid2s[:512]

        input_data = {"text": process_name}
        r = requests.post(host + "api/ner_raner", headers=headers, data=json.dumps(input_data))
        result_json = json.loads(r.text)
        result_pair = [[[e['type'] for e in r], [e['span'] for e in r]] for r in result_json['result']]
        batch_label, batch_mention = zip(*result_pair)

        data += [[pkey, uuid, item_id, '子品类', category, score]
                 for pkeys, uuids, item_id, category, score in
                 zip(process_pkeys, process_uuid2s, process_item_id, batch_label, batch_mention)
                 for pkey, uuid in zip(pkeys, uuids)]

        batch_item_id = batch_item_id[512:]
        batch_pkeys = batch_pkeys[512:]
        batch_name = batch_name[512:]
        batch_uuid2s = batch_uuid2s[512:]

        print(f"remain {len(batch_name)}")

    utils.easy_call([db_sop], 'connect')
    # remove
    sql_delete_old = f"""ALTER TABLE {result_table} DELETE WHERE uuid2 IN ( {uuid2_list_str} )"""
    db_sop.execute(sql_delete_old)

    while not self.cleaner.check_mutations_end(db_sop, result_table):
        time.sleep(5)

    # insert
    sql_insert = f"""
        INSERT INTO {result_table}
        (pkey, uuid2, item_id, `ai_clean_props.name`, `ai_clean_props.value`)
        VALUES
    """
    db_sop.execute(sql_insert, data)

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
    ent.each_partation(smonth, emonth, each_partation, params, tbl=tbl, where=where, limit=limit, multi=multi,
                       cols=['sid'])


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
    main(args)
    # test()
    end_time = time.time()
    print('all done used:{:.2f}'.format(end_time - start_time))
