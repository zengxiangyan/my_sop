import json
import codecs
import numpy as np
import pandas as pd
from keras_bert import Tokenizer
import sys
from os.path import abspath, join, dirname, exists

sys.path.insert(0, join(abspath(dirname('__file__')), '../'))
import application as app


def get_bio_dict():
    """
    采用BIO标记形式，对于bert的CLS和SEP位置用TAG标记

    :return:
    """
    bio_dict = {'O': 0, 'B': 1, 'I': 2, 'TAG': 3}
    return bio_dict


def get_bio(mention):
    """

    :param mention: mention
    :return: mention的对应标记索引
    """
    bio_d = get_bio_dict()
    bio = []
    for i in range(len(mention)):
        if i == 0:
            bio.append(bio_d['B'])
        else:
            bio.append(bio_d['I'])
    return bio


def get_bio_list(text, mention_data):
    """

    :param text: 一段文本
    :param mention_data: 文本中mention列表
    :return: 标记列表
    """
    bio = [0] * len(text)
    for mention in mention_data:
        men = mention['mention']
        offset = int(mention['offset'])
        bio[offset:offset + len(men)] = get_bio(men)
    return bio


def seq_padding(seq, max_len, value=0):
    """

    :param seq: 序列列表
    :param max_len:最大长度
    :param value: 填充的值
    :return:
    """
    x = [value] * max_len
    x[:len(seq)] = seq[:max_len]
    return x


def get_token_dict(bert_dir):
    """
    获取bert词表
    :return:
    """
    # bert_path = 'bert_model/'
    dict_path = join(bert_dir, 'vocab.txt')
    token_dict = {}
    with codecs.open(dict_path, 'r', 'utf8') as reader:
        for line in reader:
            token = line.strip()
            token_dict[token] = len(token_dict)
    return token_dict


def get_ids_seg(text, tokenizer, max_len=52):
    """
    得到bert的输入

    :param text:
    :param tokenizer:
    :param max_len:
    :return:
    """
    indices, segments = [], []
    for ch in text[:max_len - 2]:
        indice, segment = tokenizer.encode(first=ch)
        if len(indice) != 3:
            indices += [100]
            segments += [0]
        else:
            indices += indice[1:-1]
            segments += segment[1:-1]
    segments = [0] + segments + [0]
    indices = [101] + indices + [102]
    return indices, segments


def get_input_bert(config, max_len=52, mode="train"):
    """
    序列标注的数据构建

    :param config: 配置
    :param max_len: 最大长度
    :param mode: train/test
    :return:
    """
    input_file = join(app.output_path(config.data.raw_dir),  f'{mode}.json')
    output_file = join(app.output_path(config.data.data_dir), f'input_{mode}_ner.pkl')
    bert_dir = config.data.bert_dir

    token_dict = get_token_dict(bert_dir)
    tokenizer = Tokenizer(token_dict)
    bio_dict = get_bio_dict()
    inputs = {'ids': [], 'seg': [], 'labels': []}
    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            temDict = json.loads(line)
            text = temDict['text']
            mention_data = temDict['mention_data']
            indices, segments = get_ids_seg(text, tokenizer)
            inputs['ids'].append(seq_padding(indices, max_len))
            inputs['seg'].append(seq_padding(segments, max_len))

            if mode == "train":
                label = get_bio_list(text, mention_data)
                label = [bio_dict['TAG']] + label + [bio_dict['TAG']]
                assert len(indices) == len(label)
                inputs['labels'].append(seq_padding(label, max_len))

    for key in inputs:
        inputs[key] = np.array(inputs[key])
        print(key, inputs[key].shape)
        print(inputs[key][0])
    print(text)
    print(mention_data)
    pd.to_pickle(inputs, output_file)
