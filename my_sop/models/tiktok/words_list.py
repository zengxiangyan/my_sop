import pandas as pd
import numpy as np
from tqdm import tqdm
import math
import ahocorasick
from collections import Counter
import re
from tqdm.auto import tqdm
from collections import defaultdict


def calculate_idf_optimized(documents, words):
    N = len(documents)
    DF_dict = Counter()

    # Preprocess documents into sets of words
    for doc in documents:
        if isinstance(doc, str):
            DF_dict.update(set(doc.split()))

    # Convert input list to set to remove duplicates
    words_set = set(words)

    # Calculate IDF values
    idf_values = {word: math.log(N / (DF_dict[word] + 1)) for word in words_set}

    return idf_values


# def choose_data( df, year, month):
#     # 先把create_time转为日期时间格式，方便筛选
#     df['create_time'] = pd.to_datetime(df['create_time'], format='%Y/%m/%d %H:%M')

#     # 根据年月筛选数据
#     df = df[(df['create_time'].dt.year == year) & (df['create_time'].dt.month == month)]

#     # 把需要搜索的三列合并成一列
#     df['combined'] =  df['ocr'].astype(str) + ' ' + df['whisper'].astype(str)
#     df = df.reset_index(drop=True)
#     return df

def load_stopwords(file_path):
    stopwords = []
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            stopwords.append(line.strip())
    return stopwords


# def get_xiaoguo_dict_optimized(df_choose, tfidf_dict, columns):
#     # 合并所有列
#     df_choose['combined'] = df_choose[columns].apply(lambda row: ' '.join(row.fillna('')), axis=1)

#     # 创建Aho-Corasick自动机
#     A = ahocorasick.Automaton()
#     for word in tfidf_dict.keys():
#         if isinstance(word, str):
#             A.add_word(word, word)
#     A.make_automaton()

#     # 初始化结果字典
#     dict_xiaoguo = defaultdict(lambda: {'diggcount': 0, 'commentcount': 0, 'sharecount': 0, 'allcount': 0, '种草系数': 0, '视频数': 0})

#     # 计算每个词在每行中出现的次数，并更新结果字典
#     for _, row in tqdm(df_choose.iterrows()):
#         words_in_row = Counter(word for _, word in A.iter(row['combined']))
#         for word in words_in_row:
#             dict_xiaoguo[word]['diggcount'] += row['digg_count']
#             dict_xiaoguo[word]['commentcount'] += row['comment_count']
#             dict_xiaoguo[word]['sharecount'] += row['share_count']
#             dict_xiaoguo[word]['allcount'] += row['digg_count'] + row['comment_count'] + row['share_count']
#             dict_xiaoguo[word]['视频数'] += 1  # Increase the count for the word appearing in a document

#     # 计算种草系数
#     for word, values in dict_xiaoguo.items():
#         if values['diggcount'] != 0:
#             dict_xiaoguo[word]['种草系数'] = (values['sharecount'] + values['commentcount']) / values['diggcount']

#     # 将不存在于df_choose但存在于tfidf_dict的词加入到结果字典
#     for word in set(tfidf_dict.keys()) - set(dict_xiaoguo.keys()):
#         dict_xiaoguo[word] = {'diggcount': 0, 'commentcount': 0, 'sharecount': 0, 'allcount': 0, '种草系数': 0, '视频数': 0}

# return dict_xiaoguo

def compute_tfidf(document, idf_dict):
    # 创建Aho-Corasick自动机
    A = ahocorasick.Automaton()
    for word in idf_dict.keys():
        if isinstance(word, str):
            A.add_word(word, word)
    A.make_automaton()

    # 计算TF
    tf_dict = Counter(word for end_index, word in A.iter_long(document))

    # 计算TF-IDF
    tfidf_dict = {word: tf_dict.get(word, 0) * idf for word, idf in tqdm(idf_dict.items())}

    return tf_dict, tfidf_dict


def yinshe_dict(answer_emb_dict, cat_emb_dict):
    dict_sim = dict()
    answer_emb = np.array(list(answer_emb_dict.values()))
    print(answer_emb.shape)
    cat_emb = np.array(list(cat_emb_dict.values()))
    similarity_matrix = np.matmul(answer_emb, cat_emb.T)
    closest_word_indices = list(np.argmax(similarity_matrix, axis=1))
    max_similarities = list(np.max(similarity_matrix, axis=1))
    for i in range(len(closest_word_indices)):
        answer_text = list(answer_emb_dict.keys())[i]
        index = closest_word_indices[i]
        cat_text = list(cat_emb_dict.keys())[index]
        dict_sim[answer_text] = [cat_text, max_similarities[i]]

    return dict_sim


# def embd_dict_after(message, tags_embd):
#     tag_embd_dict = dict()
#     for tag_list, embd_list in zip(messages, tags_embd[0]):
#         for tag, emb_info in zip(tag_list, embd_list['data']):
#             tag_embd_dict[tag] = emb_info['embedding']
#     return tag_embd_dict


def choose_data(df, year, month):
    # 先把create_time转为日期时间格式，方便筛选
    df['create_time'] = pd.to_datetime(df['create_time'], format='%Y/%m/%d %H:%M')

    # 根据年月筛选数据
    df = df[(df['create_time'].dt.year == year) & (df['create_time'].dt.month == month)]
    df['allcount'] = df['digg_count'] + df['comment_count'] + df['share_count']

    df = df.reset_index(drop=True)
    return df


def calculate_growth(numerator, denominator):
    if denominator != 0:
        return (numerator / denominator) - 1
    elif numerator == 0:
        return 0
    else:
        return 'NA'


def get_xiaoguo_dict_optimized(df_choose, tfidf_dict, columns):
    # 合并所有列
    df_choose['combined'] = df_choose[columns].apply(lambda row: ' '.join(row.fillna('')), axis=1)

    # 创建Aho-Corasick自动机
    A = ahocorasick.Automaton()
    for word in tfidf_dict.keys():
        if isinstance(word, str):
            A.add_word(word, word)
    A.make_automaton()

    # 初始化结果字典
    dict_xiaoguo = defaultdict(
        lambda: {'diggcount': 0, 'commentcount': 0, 'sharecount': 0, 'allcount': 0, 'collectcount': 0, '种草系数': 0,
                 '深度种草系数': 0, '视频数': 0})

    # 计算每个词在每行中出现的次数，并更新结果字典
    for _, row in tqdm(df_choose.iterrows(), total=len(df_choose)):
        words_in_row = Counter(word for _, word in A.iter(row['combined']))
        for word in words_in_row:
            dict_xiaoguo[word]['diggcount'] += row['digg_count']
            dict_xiaoguo[word]['commentcount'] += row['comment_count']
            dict_xiaoguo[word]['sharecount'] += row['share_count']
            dict_xiaoguo[word]['collectcount'] += row['collect_count']
            dict_xiaoguo[word]['allcount'] += row['digg_count'] + row['comment_count'] + row['share_count'] + row[
                'collect_count']
            dict_xiaoguo[word]['视频数'] += 1  # Increase the count for the word appearing in a document

    # 计算种草系数
    for word, values in dict_xiaoguo.items():
        if values['diggcount'] != 0:
            dict_xiaoguo[word]['种草系数'] = (values['sharecount'] + values['commentcount']) / values['diggcount']
            dict_xiaoguo[word]['深度种草系数'] = (values['sharecount'] + values['commentcount']) / values['allcount']
    # 将不存在于df_choose但存在于tfidf_dict的词加入到结果字典
    for word in set(tfidf_dict.keys()) - set(dict_xiaoguo.keys()):
        dict_xiaoguo[word] = {'diggcount': 0, 'commentcount': 0, 'sharecount': 0, 'collect_count': 0, 'allcount': 0,
                              '种草系数': 0, '深度种草系数': 0, '视频数': 0, 'rank': None}

    # 根据'深度种草系数'对结果字典排序并更新排名字段
    sorted_dict = sorted(dict_xiaoguo.items(), key=lambda item: item[1]['深度种草系数'], reverse=True)
    for rank, (word, _) in enumerate(sorted_dict, 1):
        dict_xiaoguo[word]['rank'] = rank

    return dict_xiaoguo


class calcualte_list():
    def __init__(self, idf_dict, data, data_old, words_dict, task_names):

        self.idf_dict = idf_dict
        self.data = data
        self.data_old = data_old
        self.words_dict = words_dict
        self.task_names = task_names
        self.ww_awm_ph = dict()

    def get_xiaoguo_dict_optimized(self, df_choose, tfidf_dict, columns):
        # 合并所有列

        df_choose['combined'] = df_choose[columns].apply(lambda row: ' '.join(row.fillna('')), axis=1)
        #         print(df_choose['combined'])
        # 创建Aho-Corasick自动机
        A = ahocorasick.Automaton()
        for word in tfidf_dict.keys():
            if isinstance(word, str):
                A.add_word(word, (word, len(word)))
        A.make_automaton()

        # 初始化结果字典
        dict_xiaoguo = defaultdict(
            lambda: {'diggcount': 0, 'commentcount': 0, 'sharecount': 0, 'allcount': 0, 'collectcount': 0, '种草系数': 0,
                     '深度种草系数': 0, '视频数': 0})

        # 计算每个词在每行中出现的次数，并更新结果字典
        for _, row in tqdm(df_choose.iterrows(), total=len(df_choose)):
            words_in_row = set()
            for endi, (word, lw) in A.iter_long(row['combined']):
                words_in_row.add(word)
                left = 0 if endi-lw-8<0 else endi-lw-8
                right = endi+8
                self.ww_awm_ph[(word, row['视频id'])] = row['combined'][left:right]

            for word in words_in_row:
                dict_xiaoguo[word]['diggcount'] += row['digg_count']
                dict_xiaoguo[word]['commentcount'] += row['comment_count']
                dict_xiaoguo[word]['sharecount'] += row['share_count']
                dict_xiaoguo[word]['collectcount'] += row['collect_count']
                dict_xiaoguo[word]['allcount'] += row['digg_count'] + row['comment_count'] + row['share_count'] + row[
                    'collect_count']
                dict_xiaoguo[word]['视频数'] += 1  # Increase the count for the word appearing in a document

        # 计算种草系数
        for word, values in dict_xiaoguo.items():
            if values['diggcount'] != 0:
                dict_xiaoguo[word]['种草系数'] = (values['sharecount'] + values['commentcount']) / values['diggcount']
                dict_xiaoguo[word]['深度种草系数'] = (values['sharecount'] + values['commentcount']) / values['allcount']
        # 将不存在于df_choose但存在于tfidf_dict的词加入到结果字典
        for word in set(tfidf_dict.keys()) - set(dict_xiaoguo.keys()):
            # dict_xiaoguo[word] = {'diggcount': 0, 'commentcount': 0, 'sharecount': 0,'collect_count':0, 'allcount': 0, '种草系数': 0,'深度种草系数':0, '视频数': 0, 'rank': None}
            dict_xiaoguo[word] = {'diggcount': 0, 'commentcount': 0, 'sharecount': 0, 'collect_count': 0, 'allcount': 0,
                                  '种草系数': 0, '深度种草系数': 0, '视频数': 0}

        # 根据'深度种草系数'对结果字典排序并更新排名字段
        # sorted_dict = sorted(dict_xiaoguo.items(), key=lambda item: item[1]['深度种草系数'], reverse=True)
        # rank = 1
        # for word, _ in sorted_dict:
        #     if dict_xiaoguo[word]['深度种草系数'] != 0:
        #         dict_xiaoguo[word]['rank'] = rank
        #         rank += 1
        return dict_xiaoguo

    def calculate_growth(self, numerator, denominator):
        if denominator != 0:
            return (numerator / denominator) - 1
        elif numerator == 0:
            return 0
        else:
            return 'NA'

    def split_data_by_month(self, df):
        # 先把create_time转为日期时间格式，方便筛选
        df['create_time'] = pd.to_datetime(df['create_time'], format='%Y/%m/%d %H:%M')

        # 获取数据集的年份和月份范围
        unique_dates = df['create_time'].dt.to_period('M').unique()
        unique_dates = sorted(unique_dates.astype(str))

        dfs = []  # 存储按月份分割的数据集
        labels = []  # 存储年份和月份的标签

        # 遍历所有的年份和月份
        for date in unique_dates:
            year, month = map(int, date.split('-'))

            # 根据年月筛选数据
            df_temp = df[(df['create_time'].dt.year == year) & (df['create_time'].dt.month == month)]
            df_temp['allcount'] = df_temp['digg_count'] + df_temp['comment_count'] + df_temp['share_count']
            #             df_temp = df_temp.dropna(subset=['xunfei文本', 'ocr字幕', 'ocr_mass', 'whisper文本'], how='all')
            # 重设索引并保存数据集和标签
            df_temp = df_temp.reset_index(drop=True)
            dfs.append(df_temp)
            labels.append(f'{year}-{month}')

        return dfs, labels

    def compute_tfidf(self, document, idf_dict):
        # 创建Aho-Corasick自动机
        A = ahocorasick.Automaton()
        for word in idf_dict.keys():
            if isinstance(word, str):
                A.add_word(word, word)
        A.make_automaton()

        # 计算TF
        tf_dict = Counter(word for end_index, word in A.iter(document))

        # 计算TF-IDF
        tfidf_dict = {word: tf_dict.get(word, 0) * idf for word, idf in idf_dict.items()}

        return tf_dict, tfidf_dict

    def merge_columns_fillna(self, df, cols):
        result = df[cols[0]].fillna('')
        for col in cols[1:]:
            result += df[col].fillna('')
        return result

    def calculate_xiaoguo_month(self, dfs):
        dict_xiaoguo_month = list()
        tf_dicts = list()
        tfidf_dicts = list()
        for df in dfs:
            new_text = list(
                set([x for x in self.merge_columns_fillna(df, self.task_names) if
                     x != '']))
            texts = ''.join(new_text)
            tf_dict, tfidf_dict = self.compute_tfidf(texts, self.idf_dict)
            dict_xiaoguo = self.get_xiaoguo_dict_optimized(df, tfidf_dict,
                                                           self.task_names)
            dict_xiaoguo_month.append(dict_xiaoguo)
            tf_dicts.append(tf_dict)
            tfidf_dicts.append(tfidf_dict)
        return dict_xiaoguo_month, tf_dicts, tfidf_dicts

    # def process_data(self, i, df_month, idf_values):
    #     df_month0 = df_month[i - 1]
    #     df_month1 = df_month[i]
    #     words = list(self.filter_df(df_month1, df_month0)['词'])
    #     words += list(df_month1[df_month1['结构'].isin(['功能功效', '修饰_外观描述', '适用范围_适用人群', '修饰_产品属性'])]['词'])
    #     words = list(set(words))
    #     for word in words:
    #         if idf_values[word] <= 11:
    #             words.remove(word)
    #
    #     dfmonth0 = df_month0[df_month0['词'].isin(word_list)]
    #     dfmonth1 = df_month1[df_month1['词'].isin(word_list)]
    #
    #     # Rank words in dfmonth0
    #     dfmonth0_sorted = dfmonth0.sort_values(by='深度种草系数', ascending=False)
    #     dfmonth0_sorted['rank'] = dfmonth0_sorted['深度种草系数'].rank(method='min', ascending=False)
    #     dfmonth0_sorted.loc[dfmonth0_sorted['allcount'] == 0, 'rank'] = float('NaN')
    #
    #     # Rank words in dfmonth1
    #     dfmonth1_sorted = dfmonth1.sort_values(by='深度种草系数', ascending=False)
    #     dfmonth1_sorted['rank'] = dfmonth1_sorted['深度种草系数'].rank(method='min', ascending=False)
    #     dfmonth1_sorted.loc[dfmonth1_sorted['allcount'] == 0, 'rank'] = float('NaN')
    #
    #     return dfmonth0_sorted, dfmonth1_sorted, words

    def get_rank_difference(self, df1, df2, word):
        # Get rank of the word in each dataframe
        rank_df1 = df1.loc[df1['词'] == word, 'rank'].values[0]
        rank_df2 = df2.loc[df2['词'] == word, 'rank'].values[0]

        # Calculate rank difference
        rank_difference = rank_df1 - rank_df2

        # If the rank difference is 0, return 'new'
        if rank_difference == 0:
            return 'new'

        return rank_difference

    def generate_dfs_month(self, dict_xiaoguo_month, tf_dicts, tfidf_dicts):
        idf_values = self.idf_dict
        df_month = list()
        for i in range(len(tfidf_dicts)):
            tfidf_dict = tfidf_dicts[i]
            tf_dict = tf_dicts[i]
            dict_xiaoguo = dict_xiaoguo_month[i]
            rows = []

            for word, tfidf in tfidf_dict.items():
                tf = tf_dict[word]
                idf = idf_values[word]
                tag = self.words_dict[word]
                xiaoguo = dict_xiaoguo.get(word, {})  # Get corresponding values from dict_xiaoguo
                rows.append([tag, word, tfidf, tf, idf, xiaoguo.get('diggcount', 0), xiaoguo.get('commentcount', 0),
                             xiaoguo.get('sharecount', 0), xiaoguo.get('collectcount', 0), xiaoguo.get('allcount', 0),
                             xiaoguo.get('种草系数', 0), xiaoguo.get('视频数', 0), ])

            df = pd.DataFrame(rows, columns=['结构', '词', 'tfidf', 'tf', 'idf', 'diggcount', 'commentcount', 'sharecount',
                                             'collectcount', 'allcount', '种草系数', '视频数', ])
            df_month.append(df)
        return df_month

    def find_word_new(self, word_list, search_list, dfs):
        for df in dfs:
            search_list += list(self.merge_columns_fillna(df, self.task_names))
        A = ahocorasick.Automaton()
        for index, word in enumerate(tqdm(word_list, desc="Adding words to automaton")):
            A.add_word(str(word), (index, word))

        A.make_automaton()

        found_words = set()  # 储存所有在search_list出现过的word
        for item in tqdm(search_list, desc="Matching words in list"):
            for end_index, (insert_order, original_value) in A.iter(str(item)):
                found_words.add(original_value)

        not_in_list = [word for word in word_list if word not in found_words]

        return not_in_list

    def filter_df(self, df1, df2):
        df1 = df1[df1['视频数'] >= 5]
        # 找到在df1中'allcount'不为0的词
        df1_non_zero = df1[df1['allcount'] != 0]

        # 找到在df2中'allcount'为0的词
        df2_zero = df2[df2['allcount'] == 0]

        # 找到在df1中'allcount'不为0，而在df2中'allcount'为0的词
        words = set(df1_non_zero['词']).intersection(set(df2_zero['词']))

        # 返回这些词在df1中的行
        return df1[df1['词'].isin(words)]

    #     def calculate_growth_rate(self, df1, df2, new_words):
    #         # Create a copy of df1 to hold the results
    #         result_df = df1.copy()

    #         # Merge df1 and df2 on the '词' column, keep all words from df1
    #         merged_df = pd.merge(df1, df2, on='词', how='left', suffixes=('_df1', '_df2'))

    #         # Fill NaN values in 'allcount_df2' and '视频数_df2' with 0
    #         merged_df[['allcount_df2', '视频数_df2']] = merged_df[['allcount_df2', '视频数_df2']].fillna(0)

    #         # Define conditions
    #         def calculate_rate(row, field):
    #             if row['词'] in new_words:
    #                 return 'new'
    #             elif row[field+'_df2'] == 0:
    #                 return 'nan'
    #             else:
    #                 return row[field+'_df1'] / row[field+'_df2'] - 1

    #         # Calculate interaction growth rate
    #         merged_df['互动增速'] = merged_df.apply(lambda row: calculate_rate(row, 'allcount'), axis=1)

    #         # Calculate video count growth rate
    #         merged_df['视频数增速'] = merged_df.apply(lambda row: calculate_rate(row, '视频数'), axis=1)

    #         # Assign the calculated growth rates to the result DataFrame
    #         result_df['互动增速'] = merged_df['互动增速']
    #         result_df['视频数增速'] = merged_df['视频数增速']

    #         return result_df
    def calculate_growth_rate(self, df1, df2, new_words):
        # Create a copy of df1 to hold the results
        result_df = df1.copy()

        # Merge df1 and df2 on the '词' column, keep all words from df1
        merged_df = pd.merge(df1, df2, on='词', how='left', suffixes=('_df1', '_df2'))

        # Fill NaN values in 'allcount_df2' and '视频数_df2' with 0
        merged_df[['allcount_df2', '视频数_df2']] = merged_df[['allcount_df2', '视频数_df2']].fillna(0)

        # Create set for new words for faster check
        new_words_set = set(new_words)

        # Initialize '互动增速' and '视频数增速' with np.nan
        merged_df['互动增速'] = np.nan
        merged_df['视频数增速'] = np.nan

        # Calculate interaction growth rate and video count growth rate using vectorized operations
        for field in ['allcount', '视频数']:
            mask = (merged_df[field + '_df2'] != 0) & (~merged_df['词'].isin(new_words_set))

            merged_df.loc[mask, field + '增速'] = merged_df.loc[mask, field + '_df1'] / merged_df.loc[
                mask, field + '_df2'] - 1

            # Handle special cases: words in df2 have 'allcount' as 0 but not in df1
            mask_special = (merged_df[field + '_df2'] == 0) & (merged_df[field + '_df1'] != 0)
            merged_df.loc[mask_special & merged_df['词'].isin(new_words_set), field + '增速'] = 'new'
            merged_df.loc[mask_special & ~merged_df['词'].isin(new_words_set), field + '增速'] = np.inf

        # Assign 'new' to new words as a special indicator
        merged_df.loc[merged_df['词'].isin(new_words_set), ['allcount增速', '视频数增速']] = 'new'

        # Assign the calculated growth rates to the result DataFrame
        result_df['互动增速'] = merged_df['allcount增速']
        result_df['视频数增速'] = merged_df['视频数增速']

        return result_df

    def normalize_columns(self, df, column_names):
        # Create a new DataFrame to hold the normalized columns
        normalized_df = df.copy()

        for column_name in column_names:
            # Create a copy of the column to avoid modifying the original data
            column = df[column_name].copy()
            column = column.replace(0.0, np.nan)
            column = column.replace('new', np.nan)
            column = column.replace(np.inf, np.nan)

            # Identify numeric entries (ignore 'new', np.inf, and -np.inf)
            is_numeric = pd.to_numeric(column, errors='coerce').notna()

            # Normalize the numeric entries
            min_val = column[is_numeric].min()
            max_val = column[is_numeric].max()
            normalized_column = (column[is_numeric] - min_val) / (max_val - min_val)

            # Assign normalized values to the corresponding cells in the new DataFrame
            normalized_df.loc[is_numeric, column_name + '归一化'] = normalized_column

        return normalized_df

    def to_float(self, x):
        try:
            x = float(x)
        except ValueError:
            pass
        return x

    def generate_list(self, df_month):

        df_lists = list()
        [dfs, labels] = self.split_data_by_month(self.data)
        for i in tqdm(range(len(df_month[1:]))):
            df_month0 = df_month[i]
            df_month1 = df_month[i + 1]

            words_new = self.find_word_new(df_month1['词'].to_list(), self.data_old, dfs[:i + 1])
            print(f'words_new_nums={len(words_new)}')
            df_month_out = self.calculate_growth_rate(df_month1, df_month0, words_new)
            print('growh complete')
            df_month_out = self.normalize_columns(df_month_out, ['allcount', '互动增速', '视频数', '种草系数', '视频数增速'])
            df_month_out['热力榜'] = 0.45 * df_month_out['视频数归一化'] + 0.1 * df_month_out['种草系数归一化'] + 0.45 * df_month_out[
                'allcount归一化']
            df_month_out['新词榜'] = 0.3 * df_month_out['视频数归一化'] + 0.6 * df_month_out['种草系数归一化']
            df_month_out['增速榜'] = 0.2 * df_month_out['视频数归一化'] + 0.25 * df_month_out['种草系数归一化'] + 0.5 * df_month_out[
                '视频数增速归一化']
            df_lists.append(df_month_out)

        return df_lists

    def generate_datas(self):
        [dfs, labels] = self.split_data_by_month(self.data)
        dict_xiaoguo_month, tf_dicts, tfidf_dicts = self.calculate_xiaoguo_month(dfs)
        df_month = self.generate_dfs_month(dict_xiaoguo_month, tf_dicts, tfidf_dicts)
        dfs_growth = self.generate_list(df_month)
        return df_month, dfs_growth
