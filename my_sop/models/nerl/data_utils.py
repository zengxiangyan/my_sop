import random
import json
import pandas as pd
from collections import Counter
import sys
from os.path import abspath, join, dirname, exists
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.traversal import P

sys.path.insert(0, join(abspath(dirname('__file__')), '../'))
import application as app


def entity_clear(entity):
    """
    将一些特殊字符替换
    :param entity: 一个实体名字
    :return: 替换后的实体
    """
    pun = {'，': ',',
           '·': '•',
           '：': ':',
           '！': '!',
           }
    for p in pun:
        if p in entity:
            entity = entity.replace(p, pun[p])
    return entity


def get_len(text_lens, max_len=510, min_len=30):
    """
    戒断过长文本你的长度，小于30不在戒断，大于30按比例戒断

    :param text_lens: 列表形式 data 字段中每个 predicate+object 的长度
    :param max_len: 最长长度
    :param min_len: 最段长度
    :return: 列表形式 戒断后每个 predicate+object 保留的长度
            如 input：[638, 10, 46, 9, 16, 22, 10, 9, 63, 6, 9, 11, 34, 10, 8, 6, 6]
             output：[267, 10, 36, 9, 16, 22, 10, 9, 42, 6, 9, 11, 31, 10, 8, 6, 6]

    """
    new_len = [min_len] * len(text_lens)
    sum_len = sum(text_lens)
    del_len = sum_len - max_len
    del_index = []
    for i, l in enumerate(text_lens):
        if l > min_len:
            del_index.append(i)
        else:
            new_len[i] = l
    del_sum = sum([text_lens[i] - min_len for i in del_index])
    for i in del_index:
        new_len[i] = text_lens[i] - int(((text_lens[i] - min_len) / del_sum) * del_len) - 1
    return new_len


def get_text(en_data, max_len=510, min_len=30):
    """
    根据data字段数据生成描述文本，将 predicate项与object项相连，在将过长的依据规则戒断

    :param en_data: kb里面的每个实体的data数据
    :param max_len: 每个 predicate+object 的最大长度
    :param min_len: 每个 predicate+object 的最小长度
    :return: 每个实体的描述文本
    """
    texts = []
    text = ''
    for data in en_data:
        texts.append(data['predicate'] + ':' + data['object'] + '，')
    text_lens = []
    for t in texts:
        text_lens.append(len(t))
    if sum(text_lens) < max_len:
        for t in texts:
            text = text + t
    else:
        new_text_lens = get_len(text_lens, max_len=max_len, min_len=min_len)
        for t, l in zip(texts, new_text_lens):
            text = text + t[:l]
    return text[:max_len]


def output_data(file, data):
    with open(file, 'w', encoding='utf-8') as f:
        for line in data:
            f.write(line + '\n')


def get_train_data(data_dir, artificial=False):
    db = app.connect_clickhouse('chsop')

    sample_sql = "select name, trade_prop_all, label from artificial.test_sample where label!='' "
    if artificial:
        sample_sql += "and confirmType=1"

    data = db.query_all(sample_sql)
    data = [json.dumps({"text_id": i, "text": d[0], "mention_data": json.loads(d[-1])}, ensure_ascii=False)
            for i, d in enumerate(data)]
    random.shuffle(data)

    # app.mkdirs(app.output_path(data_dir))
    output_data(app.output_path(join(data_dir, 'train.json')), data[:int(.8 * len(data))])
    output_data(app.output_path(join(data_dir, 'develop.json')), data[int(.8 * len(data)):])


def get_kb_data(data_dir):
    """
    获取知识库中的实体

    :param data_dir:
    :return:
    """
    g = app.connect_graph('default')
    db_sop = app.connect_clickhouse('chsop')

    all_gid2mention = dict()
    for (l,) in db_sop.query_all("select label from artificial.test_sample where label!='';"):
        for e in json.loads(l):
            if all_gid2mention.get(e['gid']) is None:
                all_gid2mention[e['gid']] = set()
            all_gid2mention[e['gid']].add(e['mention'])
    all_gid = [gid for gid in all_gid2mention if gid > 0]
    q = g.V(all_gid).as_('entity', 'pn').select('entity', 'pn')
    q = q.by(__.valueMap(True)).by(
        __.in_('categoryPropValue').in_('categoryPropNameL').dedup().values('pnname').fold()).toList()

    print(all_gid)

    f = open(app.output_path(join(data_dir, 'kb_data')), 'w', encoding='utf-8')
    for e in q:
        info = {"alias": [], "subject_id": "", "subject": "", "type": [], "data": []}
        data = {"predict": "", "object": ""}

        if e['entity']['type'][0] == 1:
            info["type"].append("Category")
            _name = 'cname'
        elif e['entity']['type'][0] == 2:
            info["type"].append("Brand")
            _name = 'bname'
        else:
            info["type"] = [f"Prop-{p}" for p in e['pn']]
            _name = 'pvname'

        all_gid2mention[e['entity']['id']].add(e['entity'][_name][0])
        info["subject"] = e['entity'][_name][0]
        info["subject_id"] = str(e['entity']['id'])
        info["alias"] = list(all_gid2mention[e['entity']['id']])
        f.write(json.dumps(info, ensure_ascii=False) + '\n')
    f.close()


def kb_processing(config):
    """
    知识库处理

    :return:
    """
    # new_entity_alias = new_alias()
    id_text = {}
    entity_id = {}
    type_index = {}
    type_index['NAN'] = 0
    type_i = 1
    id_type = {}
    id_entity = {}

    with open(app.output_path(join(config.data.raw_dir, 'kb_data')), 'r', encoding='utf-8') as f:
        for line in f:
            temDict = json.loads(line)
            subject = temDict['subject']
            subject_id = temDict['subject_id']
            alias = set()
            for a in temDict['alias']:
                alias.add(a)
                alias.add(a.lower())
            alias.add(subject.lower())
            alias.add(entity_clear(subject))
            # if subject in new_entity_alias:
            #     alias = alias | new_entity_alias[subject]
            en_data = temDict['data']
            en_type = temDict['type']
            entity_name = set(alias)
            entity_name.add(subject)
            for t in en_type:
                if not t in type_index:
                    type_index[t] = type_i
                    type_i += 1
            for n in entity_name:
                # n = del_bookname(n)
                if n in entity_id:
                    entity_id[n].append(subject_id)
                else:
                    entity_id[n] = []
                    entity_id[n].append(subject_id)
            id_type[subject_id] = en_type
            text = get_text(en_data)
            id_text[subject_id] = text
            id_entity[subject_id] = subject

    pd.to_pickle(entity_id, app.output_path(join(config.data.data_dir, 'entity_id.pkl')))
    pd.to_pickle(id_entity, app.output_path(join(config.data.data_dir, 'id_entity.pkl')))
    pd.to_pickle(type_index, app.output_path(join(config.data.data_dir, 'type_index.pkl')))
    pd.to_pickle(id_type, app.output_path(join(config.data.data_dir, 'id_type.pkl')))
    pd.to_pickle(id_text, app.output_path(join(config.data.data_dir, 'id_text.pkl')))


def generate_description(config):
    """
    生成实体描述文本，用来做EL

    :return:
    """
    entity_relate_count = dict()
    with open(app.output_path(join(config.data.raw_dir, 'train.json')), 'r', encoding='utf-8') as f:
        for line in f.readlines():
            info = json.loads(line)
            mention_data = info['mention_data']
            mention_gid = [entity['gid'] for entity in mention_data if entity['gid'] > 0]
            for gid in set(mention_gid):
                entity_relate_count[gid] = entity_relate_count.get(gid, Counter()) + Counter(mention_gid)

    print(entity_relate_count)
    entities = dict()
    with open(app.output_path(join(config.data.raw_dir, 'kb_data')), 'r', encoding='utf-8') as f:
        for line in f.readlines():
            entity = json.loads(line)
            entities[entity['subject_id']] = entity

    for gid, entity in entities.items():
        entity['data'].append({"predict": "keyword",
                               "object": " ".join(entity["alias"])})
        if entity_relate_count.get(int(gid)) is not None:
            entity['data'].append({"predict": "freq_appear",
                                   "object": ' '.join(entities[str(_gid)]['subject']
                                                      for _gid, _ in entity_relate_count[int(gid)].most_common(5))})
        # entity[] = []

    f = open(app.output_path(join(config.data.raw_dir, 'kb_data_desc')), 'w', encoding='utf-8')
    for entity in entities.items():
        f.write(json.dumps(entity, ensure_ascii=False) + '\n')
    f.close()
# get_kb_data(r"nerl/origin_data")
# kb_processing()
