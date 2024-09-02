import re
import ast

import numpy as np

space_replace_symbol = "_"


def remove_unused_character(content):
    """
    去除无用字符
    :param content: 待处理文本
    :return:
    """
    content = content.replace('{', ' ')
    content = content.replace('}', ' ')
    content = content.replace('"', '')
    content = content.replace(' ', '_')
    unused_pattern1 = r'[\t\r\n\v]|\\r|\\n'  # \r\n会导致换行
    unused_pattern2 = r'[【】（）()、；“”‘’？￥｛｝&\\#★ ]|{<br>}'  # 特殊中文字符
    return re.sub('[ ]+', ' ',
                  re.sub(unused_pattern2, ' ',
                         re.sub(unused_pattern1, ' ', content))).lower()


def connect_name_props(name, trade_name, trade_value, props_name, props_value):
    trade_tuple = list(zip(trade_name, trade_value))
    props_tuple = list(zip(props_name, props_value))
    trade_context = ','.join([f'{p}:{v}' for (p, v) in trade_tuple
                              if '是否' not in p and v not in ['是', '否']])
    props_context = ','.join([f'{p}:{v}' for (p, v) in props_tuple
                              if '是否' not in p and v not in ['是', '否'] and (p, v) not in trade_tuple])
    return '|'.join([remove_unused_character(name),
                     remove_unused_character(trade_context),
                     remove_unused_character(props_context)])


def to_nel_string(name, trade='', props=''):
    _name = (name)
    try:
        trade_dict = ast.literal_eval(trade)
    except:
        trade_dict = dict()
    try:
        props_dict = ast.literal_eval(props)
    except:
        props_dict = dict()
    print('|'.join(f'{p}:{v}' for p, v in ast.literal_eval(trade).items()))
    return


def lv0cid2cid(lv0cid):
    _lv0cid2cid = {2011010110: 25594, 2011010111: 25595, 2011010112: 25596, 2011010113: 25597, 2011010114: 25598,
                   2011010115: 25599, 2011010116: 25600, 2011010117: 25601, 2011010118: 25602, 2011010119: 25603,
                   2011010120: 25604, 2011010121: 25605, 2011010122: 25606, 2011010123: 25607, 2011010124: 25608}
    if _lv0cid2cid.get(lv0cid) is None:
        raise ValueError(
            'Not a lv0cid, input a lv0cid in {}'.format(','.join([str(_cid) for _cid in _lv0cid2cid.keys()])))
    return _lv0cid2cid[lv0cid]


def encode_string(s):
    return s.replace(' ', space_replace_symbol).lower()


def generate_location_weight(length):
    w = np.arange(length)
    w = np.log(1 + w) + 1
    w = w / np.max(w)
    return w


def match_answer(string, result):
    """
    匹配ner结果

    :param string:
    :param result:
    :return:
    """
    try:
        word_type = None
        offset = -1
        word = ''
        all_words = dict()
        for i, _ in enumerate(string):
            if not word:
                if result[i][0] == 'B':
                    word_type = result[i][2:]
                    offset = i
                    word = string[i]
                elif result[i][0] == 'I':
                    continue
                    # raise ValueError(f'返回错误, {result}')
            else:
                if result[i][0] == 'B':
                    all_words[(word, offset)] = word_type
                    word_type = result[i][2:]
                    offset = i
                    word = string[i]
                elif result[i][0] == 'I':
                    try:
                        assert word_type == result[i][2:]
                        word += string[i]
                    except AssertionError:
                        # todo fix
                        word += string[i]
                elif result[i][0] == 'O':
                    all_words[(word, offset)] = word_type
                    word_type = None
                    offset = -1
                    word = ''
        if word and word_type:
            all_words[(word, offset)] = word_type
        return all_words
    except:
        return dict()


def get_batch_data(all_data, batch_size=1000):
    start = 0
    while start < len(all_data):
        yield all_data[start: min(start + batch_size, len(all_data))]
        start += batch_size


if __name__ == '__main__':
    _context = '【双11预售】力士植萃沙龙香小苍兰+蓝风铃与烟酰胺沐浴露550g*2'
    _trade_name = ['组合套餐']
    _trade_value = ['(亮泽滋润型香浴乳850g)*1+(微凉爽肤型香浴乳850g)*1']
    _props_name = ['组合套餐', '型号', '产地', '净含量', '净含量', '净含量', '净含量', '净含量', '保质期', '功能', '功能', '功能', '功能', '是否为特殊用途化妆品',
                   '是否量贩装']
    _props_value = ['(亮泽滋润型香浴乳850g)*1+(微凉爽肤型香浴乳850g)*1', '台湾PonPon澎澎香浴乳', '台湾省', '微凉爽肤型香浴乳850g', '巴黎小香风沐浴乳 850g',
                    '亮泽滋润型香浴乳850g', '焕肤亮白型香浴乳850g', '水嫩净爽型香浴乳850g', '5年', '滋润', '清爽', '清洁', '美白', '否', '否']

    processed_context = connect_name_props(_context, _trade_name, _trade_value, _props_name, _props_value)
    print(len(processed_context))
    print(processed_context)
