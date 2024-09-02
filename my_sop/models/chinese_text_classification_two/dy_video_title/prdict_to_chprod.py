# coding=utf-8
import sys
import os
from os.path import abspath, join, dirname, exists

sys.path.insert(0, join(abspath(dirname('__file__')), '../../../'))

import tensorflow as tf
os.environ['TF_KERAS'] = '1'
os.environ['CUDA_VISIBLE_DEVICES'] = '0'

config = tf.compat.v1.ConfigProto()
config.gpu_options.allow_growth = True   # 用多少取多少
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

from sklearn.model_selection import train_test_split

db_sop = app.get_clickhouse('chsop')

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
        batch_token_ids, batch_segment_ids= [], []
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


def check_mutations_end(dba, tbl, process={}):
    a, b = tbl.split('.')
    sql = '''
        SELECT parts_to_do, latest_fail_reason FROM system.mutations
        WHERE database='{}' AND table='{}' and is_done = 0 LIMIT 1
    '''.format(a, b)
    r = dba.query_all(sql)

    if len(r) == 0:
        return True

    if 'total' not in process:
        process['total'] = r[0][0]
    process['parts_to_do'] = (r[0][0] or 1)
    process['latest_fail_reason'] = r[0][1]
    process['process'] = 100 - process['parts_to_do'] / process['total'] * 100

    if process['latest_fail_reason'] != '':
        # 报错了 过60s再看 进度变了说明没报错
        time.sleep(60)
        sql = '''
            SELECT parts_to_do, latest_fail_reason FROM system.mutations
            WHERE database='{}' AND table='{}' and is_done = 0 LIMIT 1
        '''.format(a, b)
        r = dba.query_all(sql)
        if len(r) > 0 and r[0][0] == process['parts_to_do']:
            sql = 'KILL MUTATION WHERE database = \'{}\' AND table = \'{}\''.format(a, b)
            dba.execute(sql)
            raise Exception(process['latest_fail_reason'])

    return False

def predict_chprod_douyin_video_all(test=-1):
    
    table_name = "chprod.douyin_video_all_2"
    join_table = "chprod.douyin_video_all_join"

    def dy_callback(data):
        ids, names, tags = zip(*data)
        scores, cids = [], []
        names = [names[i] + ';'.join(tags[i]) for i in range(len(names))]
        predict_data = predict_generator(names, batch_size)
        for ii in predict_data:
            y_pred = model.predict(ii)
            cids.extend([h_label_cid[x] for x in y_pred.argmax(axis=1)])
            scores.extend([math.ceil(x*100) for x in y_pred.max(axis=1)])

        insert_line = list(zip(ids, cids, scores))
        insert_sql = "insert into {tbl} (aweme_id, cid2, score2) values".format(tbl=join_table)
        db_sop.batch_insert(insert_sql, '(%s, %s, %s)', insert_line)

    db_sop.connect()

    # 删除join表
    db_sop.execute('DROP TABLE IF EXISTS {tbl}'.format(tbl=join_table))

    # 新建join表
    create_sql = """
        CREATE TABLE {tbl}(
            `aweme_id` Int64,
            `cid2` UInt32,
            `score2` UInt32
        )ENGINE = Join(ANY, LEFT, aweme_id)""".format(tbl=join_table)
    db_sop.execute(create_sql)

    sql = 'select aweme_id,`desc`, text_extra from {} where aweme_id>%d order by aweme_id limit %d'
    utils.easy_traverse(db_sop, sql.format(table_name), dy_callback, 0, 10000, test)

    # 等待插入完成
    while not check_mutations_end(db_sop, join_table):
        time.sleep(5)

    # 从join表导入
    insert_sql = '''
        ALTER TABLE {tbl} UPDATE
            `cid2` = ifNull(joinGet({ans_tbl}, 'cid2', aweme_id), 0),
            `score2` = ifNull(joinGet({ans_tbl}, 'score2', aweme_id), 0)
        WHERE 1;
    '''.format(tbl=table_name, ans_tbl=join_table)
    db_sop.execute(insert_sql)

    while not check_mutations_end(db_sop, table_name):
        time.sleep(5)

    # 删除join表
    db_sop.execute('DROP TABLE IF EXISTS {tbl}'.format(tbl=join_table))


if __name__ == '__main__':
    model.load_weights(model_save_path)
    predict_chprod_douyin_video_all()
