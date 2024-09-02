
import collections
import getopt
import sys, io
from os.path import abspath, join, dirname

sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf8')

from models.trie import Trie
from extensions import utils
import application as app
import csv


def data_pre_process(name):
    if name:
        if utils.is_chinese(name):
            name = utils.keyword_process(name, lang='cn')
            name = ','.join(name)
        elif utils.is_eng(name):
            name = utils.keyword_process(name, lang='en')
            name = ','.join(name)
        else:
            name = name
    else:
        name = ''
    return name

# 输入batch_id，从clean表中找到对应的eid
def get_eid(db, batch_id):
    sql = 'select eid from cleaner.clean_batch where batch_id={a} and deleteFlag = 0;'.format(a=batch_id)
    print(sql)
    data = db.query_all(sql)
    eid = int(data[0][0])
    return eid

# 根据获得的eid，从对应的e表中找到topN的品牌alias_all_bid（对品牌按照销售额进行降序，取topN）
def get_alias_all_bid(c_195, eid, N):
    alias = []
    sql = "select alias_all_bid from sop_e.entity_prod_{a}_E group by alias_all_bid order by sum(sales) desc limit {b}".format(a=eid, b=N)
    data = c_195.query_all(sql)
    for row in data:
        alias.append(int(row[0]))
    return alias

# 根据获得的alias_all_bid，从brush.all_brand中找到相应的品牌名（name, name_cn, name_en）
def get_brand_name(db, alias_all_bid):
    brand_name_cn_en = {}
    name_cn_bid = {}
    name_en_bid = {}
    sql = 'select bid, name, name_cn, name_en from brush.all_brand where bid in (%s)'
    data = utils.easy_query(db, sql, list(alias_all_bid))
    for row in data:
        bid, name, name_cn, name_en = list(row)
        bid = int(bid)
        name_cn = data_pre_process(name_cn)
        name_en = data_pre_process(name_en)
        brand_name_cn_en[bid] = [name, name_cn, name_en] # brand_name_cn_en-{bid:[name, name_cn, name_en]}
        name_cn_bid[name_cn] = bid # name_cn_bid-{name_cn:bid}
        name_en_bid[name_en] = bid
    return brand_name_cn_en, name_cn_bid, name_en_bid

# 首先取出graph.brand left join brush.all_brand中的name_cn，name_en, bid
# 用于insert创建索引
def get_name_cn_en(db):
    name_cn_t = collections.defaultdict(lambda: [])
    name_en_t = collections.defaultdict(lambda: [])
    bid_name_cn_en = collections.defaultdict(lambda: [])
    sql1 = "select b.name_cn, b.name_en, b.bid from graph.brand b left join brush.all_brand ab on b.bid0=ab.bid where b.confirmType in (0,1) and b.deleteFlag=0;"
    data1 = db.query_all(sql1)
    for row1 in data1:
        name_cn, name_en, bid = list(row1)
        name_cn = data_pre_process(name_cn)
        name_en = data_pre_process(name_en)
        bid_name_cn_en[int(bid)] = [name_cn, name_en]
        if name_cn != '':
            name_cn_t[name_cn].append(int(bid)) # name_cn_t-{name_cn:bid}
        if name_en != '':
            name_en_t[name_en].append(int(bid))
    return name_cn_t, name_en_t, bid_name_cn_en

# 判断brand中的name_cn和name_en是否分别在graph.brand left join brush.all_brand中的name_cn，name_en中
# 通过对后者建立trie，在前者中进行search
def creat_trie(name_cn_bid, name_en_bid, name_cn_t, name_en_t):
    # 英文名精确查找
    all_to_object_dict_en = collections.defaultdict(lambda: [])
    head2 = Trie()
    c2 = 0
    for name_en in list(name_en_bid.keys()):  # 目标品牌英文名 # name_en_bid-{name_en:bid}
        c2 += 1
        head2.insert(name_en, c2)
    for item2 in list(name_en_t.keys()):  # all brand name_en   # name_en_t-{name_en:bid}
        mid_dict = head2.search(item2)  # 返回：[num:num]
        for i in mid_dict:
            key = int(i[0])
            tmp = list(name_en_bid.keys())[key - 1]
            if item2 == tmp:
                for j in name_en_t[item2]:
                    all_to_object_dict_en[int(j)] = [item2, tmp]  # 所有品牌的name_cn各自包含的目标品牌的name_cn
    # 反转字典
    output = []
    for bid2, item22 in all_to_object_dict_en.items():
        output.append([name_en_bid[item22[1]], item22[1], bid2, item22[0]])

    # 中文名模糊查找
    all_to_object_dict_cn = collections.defaultdict(lambda: [])
    head1 = Trie()
    c1 = 0
    for name_cn in list(name_cn_bid.keys()): # 目标品牌中文名 # name_cn_bid-{name_cn:bid}
        c1 += 1
        head1.insert(name_cn, c1)
    for item1 in list(name_cn_t.keys()): # all brand name_cn   # name_cn_t-{name_cn:bid}
        mid_dict = head1.search(item1) # 返回：[num:num]
        for i in mid_dict:
            key = int(i[0])
            tmp0 = list(name_cn_bid.keys())[key-1]
            for j in name_cn_t[item1]:
                all_to_object_dict_cn[int(j)] = [item1, tmp0] # 所有品牌的name_cn各自包含的目标品牌的name_cn
            # {248728: ['科沃斯', '科沃斯'], 133362: ['科沃斯机器人', '科沃斯'], ...}
    # 反转字典
    for bid1, item11 in all_to_object_dict_cn.items():
        # bid1是所有品牌各自的bid，item11[0]是模糊查找出来的品牌，item11[1]是目标品牌， name_cn_bid[item11[1]]是目标品牌的bid
        output.append([name_cn_bid[item11[1]], item11[1], bid1, item11[0]])

    # 合并output和output2，相同品牌的(bid相同的)放在一起
    final_output = collections.defaultdict(lambda: [])
    for i in output:
        bid, name_goal, similar_bid, similar_name = i
        mid_output = {}
        if name_goal not in mid_output:
            mid_output[name_goal] = []
        mid_output[name_goal] = [similar_bid, similar_name]
        final_output[bid].append(mid_output)
        # {60605: [{'ecovacs': [248728, 'ecovacs']}], {'科沃斯': [248728, '科沃斯']}, {'科沃斯': [133362, '科沃斯机器人']}, ...}
    return final_output

# 根据bid_list获取name, bid0, alias_bid0, alias_bid0_name, alias_bid0_status
def get_all_info_by_bid(db, bid_list):
    bid_to_status = collections.defaultdict(lambda: [])
    sql = "select b.bid, b.name, b.bid0, b.alias_bid0, ab.name, b.alias_bid0_status from graph.brand b left join brush.all_brand ab on b.alias_bid0=ab.bid where b.confirmType in (0,1) and b.deleteFlag=0 and b.bid in (%s)"
    data = utils.easy_query(db, sql, list(bid_list))
    for row in data:
        bid, name, bid0, alias_bid0, alias_bid0_name, alias_bid0_status = list(row)
        bid = int(bid)
        bid_to_status[bid].append(row)
    return bid_to_status

# 根据bid_list获取对应的cid及cid_name
def get_all_cid_name(db, bid_list):
    bid_cid_name = collections.defaultdict(lambda: [])
    sql = "select c2.bid, c2.cid, c.name from graph.categorybrand c2 left join graph.category c on c2.cid=c.cid where c2.confirmType in (0,1) and c2.deleteFlag=0 and c.confirmType in (0,1) and c.deleteFlag=0 and c2.bid in (%s);"
    data = utils.easy_query(db, sql, list(bid_list))
    for row in data:
        bid, cid, name = list(row)
        bid = int(bid)
        bid_cid_name[bid].append(row)
    return bid_cid_name  # {bid:[[bid, cid, name], ]}

# 判断是否已被处理过
def read_log(conn, sheet):
    # brand根据all_bid区分
    # category和propname按name区分
    tbl_map = {'brand': 'graph.extract_cleaner_brand',
               'category': 'graph.extract_cleaner_category',
               'propname': 'graph.extract_cleaner_propname'}
    sql = 'select name, gid from {}'.format(tbl_map[sheet])
    gid_by_clean_name = {v[0]: v[1] for v in conn.query_all(sql)}
    return gid_by_clean_name

def main_process(db, alias_all_bid):
    brand_name_cn_en, name_cn_bid, name_en_bid = get_brand_name(db, alias_all_bid)
    name_cn_t, name_en_t, bid_name_cn_en = get_name_cn_en(db)
    final_output = creat_trie(name_cn_bid, name_en_bid, name_cn_t, name_en_t)
    # final_output-{60605: [{'科沃斯': [248728, '科沃斯']}, {'科沃斯': [133362, '科沃斯机器人']}, {'ecovacs': [248728, 'ecovacs']}],...}
    bid_list = []
    for bid0 in final_output:
        item = final_output[bid0]
        for item1 in item:
            for name0, item2 in item1.items():
                bid_list.append(item2[0])

    bid_to_status = get_all_info_by_bid(db,bid_list)  # {bid:[(bid, name, bid0, alias_bid0, alias_bid0_name, alias_bid0_status)]}
    cid_cname = get_all_cid_name(db, bid_list)  # {bid:[[bid, cid, name], ]}

    all_info = collections.defaultdict(lambda: [])
    # 合并第二行标题的输出内容
    for bid in bid_to_status:
        bid, name, bid0, alias_bid0, alias_bid0_name, alias_bid0_status = bid_to_status[bid][0]

        try:
            name_cn = bid_name_cn_en[bid][0]  # bid_name_cn_en-{bid:[ name_cn, name_en]}
            name_en = bid_name_cn_en[bid][1]
            cid_t = []
            cname_t = []
            for item3 in cid_cname[bid]:
                cid_t.append(item3[1])
                cname_t.append(item3[2])
            if bid not in all_info:
                all_info[bid].append(['', '', bid, name, bid0, alias_bid0, alias_bid0_name, alias_bid0_status, name_cn,
                                      name_en, '\n'.join(str(i) for i in cid_t), '\n'.join(str(j) for j in cname_t)])
        except KeyError:
            pass

    # 判断是否已被处理过
    gid_by_clean_name = read_log(db, 'brand')

    # 汇总结果
    output = []
    for brand in brand_name_cn_en:  # brand_name_cn_en-{bid:[name, name_cn, name_en]}
        bid0 = brand
        item = brand_name_cn_en[brand]  # [name, name_cn, name_en]
        name0, name_cn0, name_en0 = item

        if bid0 in gid_by_clean_name.values():  # 判断是否已被处理过，是则标记★
            output.append(['★', bid0, name0, name_cn0, name_en0, bid0, name0])  # 输出的第一行: bid0, name0, name_cn0, name_en0, alias_bid00, alias_bid00_name
        else:
            output.append(['', bid0, name0, name_cn0, name_en0, bid0, name0])

        item4 = final_output[bid0]
        #final_output: [{'科沃斯': [248728, '科沃斯']}, {'科沃斯': [133362, '科沃斯机器人']}, {'ecovacs': [248728, 'ecovacs']}]
        for tmp in item4:  # {'科沃斯': [248728, '科沃斯']}
            for tmp2 in tmp.values():
                bid = tmp2[0]
                if all_info[bid][0] not in output:
                    output.append(all_info[bid][0])
    return output

def main(h_db, batch_id, topN, date):
    batch_id = int(batch_id)
    eid = get_eid(h_db['mixdb'], batch_id)  # 通过batch_id找到对应的eid
    alias_all_bid = get_alias_all_bid(h_db['chmaster'], eid, topN)  # 按照销售额降序取topN品牌的alias_all_bid

    # 根据获得的alias_all_bid，输出对应品牌的name以及中英文名
    output = main_process(h_db['mixdb'], alias_all_bid)

    col_names1 = ['是否已被处理', 'bid0', 'name', 'name_cn', 'name_en', 'alias_bid', 'alias_name', '', '', '', '', '',
                  '关联gbid', '是否更新graph中的alias']
    col_names2 = ['', '', 'bid', 'name', 'bid0', 'alias_bid0', 'alias_bid0_name', 'alias_bid_status', 'name_cn',
                  'name_en', 'cid', 'cname']

    # with open(app.output_path('recommended_brand_for_batch_id_{}_top_{}_{}.csv'.format(batch_id, topN, date)), 'w',
    #           newline='', encoding='gb18030') as f:
    #     writer = csv.writer(f)
    #     writer.writerow(col_names1)
    #     writer.writerow(col_names2)
    #
    #     for i in output:
    #         writer.writerow(i)

    output.insert(0, col_names2)
    output.insert(0, col_names1)
    return (output)

if __name__ == '__main__':
    options, args = getopt.getopt(sys.argv[1:], 'd:c:b:')
    for name, value in options:
        if name in ('-d'):  # 自定义输出文件的日期
            date = value
        if name in ('-c'):  # 按销售额降序取topN
            topN = value
        if name in ('-b'):  # batch_id
            batch_id = value
    c_195 = app.get_clickhouse('chmaster')
    chslave = app.get_clickhouse('chslave')
    # mixdb = app.get_db('26_apollo')
    mixdb = app.get_db('default')
    h_db = {'chmaster': c_195, 'mixdb': mixdb, 'chslave': chslave}
    utils.easy_call([c_195, chslave, mixdb], 'connect')
    main(h_db, batch_id, topN, date)

# date = '0807_1'
# batch_id = 1
# topN = 10
# c_195 = app.get_clickhouse('chmaster')
# chslave = app.get_clickhouse('chslave')
# mixdb = app.get_db('26_apollo')
# h_db = {'chmaster': c_195, 'mixdb': mixdb, 'chslave': chslave}
# utils.easy_call([c_195, chslave, mixdb], 'connect')
# main(h_db, batch_id, topN, date)



