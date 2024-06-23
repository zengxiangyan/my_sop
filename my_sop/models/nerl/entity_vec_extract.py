import codecs
from keras_bert import load_trained_model_from_checkpoint, Tokenizer
from keras.models import *
import pandas as pd
import os, sys
from os.path import abspath, join, dirname

sys.path.insert(0, join(abspath(dirname('__file__')), '../../'))
import application as app
from models.nerl.keras_layers import CLSOut
os.environ["CUDA_VISIBLE_DEVICES"] = "0"


def get_token_dict(bert_dir):
    # dict_path = bert_path + 'vocab.txt'
    dict_path = join(bert_dir, 'vocab.txt')
    token_dict = {}
    with codecs.open(dict_path, 'r', 'utf8') as reader:
        for line in reader:
            token = line.strip()
            token_dict[token] = len(token_dict)
    return token_dict


def get_model(bert_dir, seq_len):
    """
    bert模型，模型输出为CLS位置的向量

    :param bert_dir:
    :param seq_len:
    :return:
    """
    # config_path = bert_path + 'bert_config.json'
    config_path = join(bert_dir, 'bert_config.json')
    # checkpoint_path = bert_path + 'bert_model.ckpt'
    checkpoint_path = join(bert_dir, 'bert_model.ckpt')
    bert_model = load_trained_model_from_checkpoint(config_path, checkpoint_path, seq_len=seq_len)
    x = CLSOut()(bert_model.output)
    model = Model(bert_model.inputs, x)
    return model


def extract(config, max_len=512):
    """
    :param max_len: 文本最大长度
    :return: 字典形式，key: kb_id  value: kb_id对应描述文本形成的向量
    """
    bert_dir = config.data.bert_dir

    model = get_model(bert_dir, max_len)
    token_dict = get_token_dict(bert_dir)
    tokenizer = Tokenizer(token_dict)
    id_text = pd.read_pickle(join(app.output_path(config.data.data_dir), 'id_text.pkl'))
    id_embedding = {}
    for id in id_text:
        if int(id) % 10000 == 0:
            print(id)
        text = id_text[id]
        indices, segments = tokenizer.encode(first=text, max_len=512)
        predicts = model.predict([[indices], [segments]], verbose=2)
        id_embedding[id] = predicts[0]
    pd.to_pickle(id_embedding, join(app.output_path(config.data.data_dir), 'id_embedding.pkl'))


if __name__ == '__main__':
    # extract()
    pass
