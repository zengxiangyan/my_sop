import re
import numpy as np
import pandas as pd
from nlp.pnlp import wkz_pattern, wky_pattern
from collections import Counter
from models.analyze.analyze_tool import used_time

en_letter = '[\u0041-\u005a|\u0061-\u007a]+'  # 大小写英文字母
no_zh_char = '[^\u4e00-\u9fa5]+'  # 非中文字符
zh_char = '[\u4e00-\u9fa5]+'  # 中文字符


def split_brand(brand):
    try:
        if wkz_pattern.search(brand):
            brand = wky_pattern.sub('', wkz_pattern.sub('/', brand))
        split_1 = set([b.lower() for b in brand.split('/')])
        split_2 = set()
        for spl in split_1:
            split_2 |= set(re.findall(zh_char, spl) + re.findall(no_zh_char, spl))
    except:
        return set([brand])
    return {s.strip() for s in split_2 if len(s.strip()) >= 2}


@used_time
def main(filename):
    df = pd.read_excel(filename + '.xlsx', dtype={'品牌名': str})
    # df = pd.read_csv('91315.csv', dtype={'品牌名': str})
    df['总销售额'] = df['销售额'].astype(np.int64)
    # df = df.loc[df['销售额'] >= 1000, :]
    df['split'] = df['品牌名'].apply(lambda b: split_brand(b))

    c = Counter()
    for split in df['split']:
        c += Counter(split)
    word_set = {p for p, v in c.items() if 1 < v < 10}
    df['split'] = df['split'].apply(lambda sp: sp & word_set)
    df = df.loc[df['split'] != set(), :]

    stack = []
    index = []
    for i in range(len(df)):
        if i not in index:
            stack.append(i)
        j = 0
        while j < len(stack):
            for k in range(i + 1, len(df)):
                if k not in index + stack and df.iloc[stack[j]]['split'] & df.iloc[k]['split']:
                    stack.append(k)
            j += 1
        if len(stack) > 1:
            index += stack
            print('index: ', index[-5:], len(index))
        stack = []

    result_df = df.iloc[index][['alias_all_bid', '品牌名']].reset_index(drop=True)
    result_df.to_excel(filename + '_merge.xlsx', index=False)


if __name__ == '__main__':
    main('91363 coty彩护香 品牌 - 2021-08-27')
