# coding=utf-8
import sys
import os
from os.path import abspath, join, dirname, exists

sys.path.insert(0, join(abspath(dirname('__file__')), '../../'))

import tensorflow as tf
os.environ['TF_KERAS'] = '1'
os.environ['CUDA_VISIBLE_DEVICES'] = '0'

config = tf.compat.v1.ConfigProto()
config.gpu_options.allow_growth = True   # 用多少取多少
# config.gpu_options.per_process_gpu_memory_fraction = 0.5  # 限制占用比例
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

import tqdm
from bert4keras.backend import keras, set_gelu, K
from bert4keras.tokenizers import Tokenizer
from bert4keras.models import build_transformer_model
from bert4keras.optimizers import Adam, extend_with_piecewise_linear_lr
from bert4keras.snippets import sequence_padding, DataGenerator
from bert4keras.snippets import open
from keras.layers import Dropout, Dense, Lambda

from sklearn.model_selection import train_test_split


set_gelu('tanh')  # 切换gelu版本
# 设置参数
num_classes = 9
maxlen = 128
batch_size = 128
config_path = '../electra_180g_small/electra_small_config.json'
checkpoint_path = '../electra_180g_small/electra_180g_small.ckpt'
dict_path = '../electra_180g_small/vocab.txt'
model_save_path = 'model/cls__best_model_9cid.weights'

# 建立分词器
tokenizer = Tokenizer(dict_path, do_lower_case=True)

class data_generator(DataGenerator):
    """数据生成器
    """
    def __iter__(self, random=False):
        batch_token_ids, batch_segment_ids, batch_labels = [], [], []
        for is_end, (text, label) in self.sample(random):
            token_ids, segment_ids = tokenizer.encode(text, maxlen=maxlen)
            batch_token_ids.append(token_ids)
            batch_segment_ids.append(segment_ids)
            batch_labels.append([label])
            if len(batch_token_ids) == self.batch_size or is_end:
                batch_token_ids = sequence_padding(batch_token_ids)
                batch_segment_ids = sequence_padding(batch_segment_ids)
                batch_labels = sequence_padding(batch_labels)
                yield [batch_token_ids, batch_segment_ids], batch_labels
                batch_token_ids, batch_segment_ids, batch_labels = [], [], []

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

def evaluate(data):
    total, right = 0., 0.
    for x_true, y_true in data:
        y_pred = model.predict(x_true).argmax(axis=1)
        y_true = y_true[:, 0]
        total += len(y_true)
        right += (y_true == y_pred).sum()
    return right / total


class Evaluator(keras.callbacks.Callback):
    """评估与保存
    """
    def __init__(self):
        self.best_val_acc = 0.

    def on_epoch_end(self, epoch, logs=None):
        val_acc = evaluate(valid_generator)
        if val_acc > self.best_val_acc:
            self.best_val_acc = val_acc
            model.save_weights(model_save_path)
        test_acc = evaluate(test_generator)
        print(
            u'val_acc: %.5f, best_val_acc: %.5f, test_acc: %.5f\n' %
            (val_acc, self.best_val_acc, test_acc)
        )

if __name__ == '__main__':
    db_ch = app.get_clickhouse('chsop')
    db_ch.connect()

    sql = '''
    with transform(category,['其他', '零食', '运动户外','美妆', '保健品', '电动牙刷', '剃须刀', '冲牙器', '漱口水'], [0,1,2,3,4,5,6,7,8], 0) as ccid
    select `desc`, text_extra, ccid
    from chprod.douyin_video_all_2 
    where category<>'';
    '''
    data_video = db_ch.query_all(sql)
    res = []
    for item in data_video:
        textt = item[0] + ';'.join(item[1])
        if textt:
            res.append((textt, item[2]))

    train_data, test_d = train_test_split(res, test_size=0.2, random_state=2022)
    valid_data, test_data = train_test_split(test_d, test_size=0.5, random_state=2022)

    # 转换数据集
    train_generator = data_generator(train_data, batch_size)
    valid_generator = data_generator(valid_data, batch_size)
    test_generator = data_generator(test_data, batch_size)

    evaluator = Evaluator()

    # 开始训练,可设置epochs
    model.fit(
        train_generator.forfit(),
        steps_per_epoch=len(train_generator),
        epochs=10,
        callbacks=[evaluator]
    )



    

