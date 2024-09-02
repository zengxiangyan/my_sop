# coding=utf-8
import sys
import os
from os.path import abspath, join, dirname, exists

sys.path.insert(0, join(abspath(dirname('__file__')), '../../../'))

import tensorflow as tf

os.environ['TF_KERAS'] = '1'
os.environ['CUDA_VISIBLE_DEVICES'] = '0'

config = tf.compat.v1.ConfigProto()
config.gpu_options.allow_growth = True  # 用多少取多少
config.gpu_options.per_process_gpu_memory_fraction = 0.5  # 限制占用比例
sess = tf.compat.v1.Session(config=config)

import application as app
import pandas as pd
import numpy as np

import time
import json
import codecs
import random
import re
import jieba
import math
from extensions import utils

import tqdm
from bert4keras.backend import keras, set_gelu, K
from bert4keras.tokenizers import Tokenizer
from bert4keras.models import build_transformer_model
from bert4keras.optimizers import Adam, extend_with_piecewise_linear_lr
from bert4keras.snippets import sequence_padding, DataGenerator
from bert4keras.snippets import open
from keras.layers import Dropout, Dense, Lambda


db_140 = app.connect_db('dy2')

h_label_cid = {
    0: 0,
    1: 20018,
    2: 3,
    3: 6,
    4: 20109,
    5: 26415,
    6: 21616,
    7: 28191,
    8: 27051
}

set_gelu('tanh')  # 切换gelu版本
# 设置参数
num_classes = 9
maxlen = 128
batch_size = 64
config_path = '../electra_180g_small/electra_small_config.json'
checkpoint_path = '../electra_180g_small/electra_180g_small.ckpt'
dict_path = '../electra_180g_small/vocab.txt'
model_save_path = 'model/cls__bestmodel_9cid.weights'

# 建立分词器
tokenizer = Tokenizer(dict_path, do_lower_case=True)


class predict_generator(DataGenerator):
    """数据生成器
    """

    def __iter__(self, random=False):
        batch_token_ids, batch_segment_ids = [], []
        for is_end, text in self.sample(random):
            token_ids, segment_ids = tokenizer.encode(text, maxlen=maxlen)
            batch_token_ids.append(token_ids)
            batch_segment_ids.append(segment_ids)
            if len(batch_token_ids) == self.batch_size or is_end:
                batch_token_ids = sequence_padding(batch_token_ids)
                batch_segment_ids = sequence_padding(batch_segment_ids)

                yield [batch_token_ids, batch_segment_ids]
                batch_token_ids, batch_segment_ids = [], []


# 加载预训练模型
bert = build_transformer_model(
    config_path=config_path,
    checkpoint_path=checkpoint_path,
    model='electra',
    return_keras_model=False,
)

output = Lambda(lambda x: x[:, 0], name='CLS-token')(bert.model.output)
output = Dense(
    units=num_classes,
    activation='softmax',
    kernel_initializer=bert.initializer
)(output)

model = keras.models.Model(bert.model.input, output)
model.summary()

# 派生为带分段线性学习率的优化器。
# 其中name参数可选，但最好填入，以区分不同的派生优化器。
AdamLR = extend_with_piecewise_linear_lr(Adam, name='AdamLR')

model.compile(
    loss='sparse_categorical_crossentropy',
    # optimizer=Adam(1e-5),  # 用足够小的学习率
    optimizer=AdamLR(learning_rate=1e-4, lr_schedule={
        1000: 1,
        2000: 0.1
    }),
    metrics=['accuracy'],
)


def predict_zl_douyin_video(test=-1):
    
    table_name = 'douyin2.douyin_video_zl_26415'
    def dy_callback(data):
        ids, names = zip(*data)
        scores, cids = [], []

        predict_data = predict_generator(names,batch_size)
        for ii in predict_data:
            y_pred = model.predict(ii)
            cids.extend([h_label_cid[x] for x in y_pred.argmax(axis=1)])
            scores.extend([math.ceil(x*100) for x in y_pred.max(axis=1)])
        insert_lines = list(zip(cids, scores, ids))
        print('start', list(ids)[0])
        replace_sql = "update {} set cid2=%s, score2=%s where aweme_id=%s".format(table_name)
        db_140.execute_many(replace_sql, insert_lines)
        db_140.commit()

    db_140.connect()
    sql = '''
    select aweme_id, concat(`desc`, text_extra)
    from {}
    where `desc`<>'' and aweme_id>%d and aweme_id<7033707147519184164
    order by aweme_id
    limit %d;
    '''.format(table_name)
    utils.easy_traverse(db_140, sql, dy_callback, 7024481848763878669, 10000, test)


if __name__ == '__main__':
    st = time.time()

    predict_zl_douyin_video()

    et = time.time()
    print('used time:', (et - st))
