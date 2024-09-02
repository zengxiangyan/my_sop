import numpy as np
import pandas as pd
import platform
from pathlib import Path

import sys
from os.path import join, abspath, dirname

sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
import application as app


def list_to_array(x):
    dff = pd.concat([pd.DataFrame({'{}'.format(index): labels}) for index, labels in enumerate(x)], axis=1)
    return dff.fillna(0).values.T.astype(int)


class NERParser:
    def __init__(self, task_name, category=['brand']):
        self.category = category
        if platform.system()[:5] == "Linux":
            self.albert_dir = Path('/home/zhang.zhenbang/albert_base_zh/')
        else:
            self.albert_dir = Path('D:/bert/albert_base_zh/')
        self.config_path = self.albert_dir / 'albert_config.json'
        self.checkpoint_path = self.albert_dir / 'model.ckpt-best'
        self.dict_path = self.albert_dir / 'vocab_chinese.txt'

        self.base_dir = Path(app.output_path('tiktok')) / task_name
        self.ner_dir = self.base_dir / 'ner'

        self.ner = self.init_ner()

    def init_ner(self):
        from bert4keras.backend import K
        from bert4keras.models import build_transformer_model
        from bert4keras.tokenizers import Tokenizer
        from bert4keras.optimizers import Adam
        from bert4keras.snippets import ViterbiDecoder, to_array
        from bert4keras.layers import ConditionalRandomField
        from keras.layers import Dense
        from keras.models import Model

        bert_layers = 12
        learning_rate = 2e-5  # bert_layers越小，学习率应该要越大
        crf_lr_multiplier = 1000  # 必要时扩大CRF层的学习率
        categories = self.category

        # 建立分词器
        tokenizer = Tokenizer(str(self.dict_path), do_lower_case=True)

        model = build_transformer_model(
            str(self.config_path),
            str(self.checkpoint_path),
            model='albert',
        )
        output_layer = 'Transformer-FeedForward-Norm'
        output = model.get_layer(output_layer).get_output_at(bert_layers - 1)
        output = Dense(len(categories) * 2 + 1)(output)
        CRF = ConditionalRandomField(lr_multiplier=crf_lr_multiplier)
        output = CRF(output)

        model = Model(model.input, output)
        # model.summary()

        model.compile(
            loss=CRF.sparse_loss,
            optimizer=Adam(learning_rate),
            metrics=[CRF.sparse_accuracy]
        )

        model.load_weights(str(self.ner_dir / 'model' / 'best_model.weights'))

        class NamedEntityRecognizer(ViterbiDecoder):
            """命名实体识别器
            """

            def batch_recognize(self, batch_texts):
                batch_tokens = [tokenizer.tokenize(text, maxlen=512) for text in batch_texts]
                batch_mapping = [tokenizer.rematch(text, tokens) for text, tokens in zip(batch_texts, batch_tokens)]
                batch_token_ids = [tokenizer.tokens_to_ids(tokens) for tokens in batch_tokens]
                batch_segment_ids = [[0] * len(token_ids) for token_ids in batch_token_ids]

                batch_token_ids = list_to_array(batch_token_ids)
                batch_segment_ids = list_to_array(batch_segment_ids)
                # batch_token_idsbatch_segment_ids = np.array(batch_segment_ids)

                # token_ids, segment_ids = to_array(batch_token_ids, batch_segment_ids)
                batch_nodes = model.predict([batch_token_ids, batch_segment_ids])
                batch_labels = [self.decode(nodes) for nodes in batch_nodes]
                batch_entities = []

                for labels in batch_labels:
                    entities, starting = [], False
                    for i, label in enumerate(labels):
                        if label > 0:
                            if label % 2 == 1:
                                starting = True
                                entities.append([[i], categories[(label - 1) // 2]])
                            elif starting:
                                entities[-1][0].append(i)
                            else:
                                starting = False
                        else:
                            starting = False
                    batch_entities.append(entities)

                return [[(mapping[w[0]][0], mapping[w[-1]][-1], l) for w, l in entities]
                        for mapping, entities in zip(batch_mapping, batch_entities)]

            def recognize(self, text):
                tokens = tokenizer.tokenize(text, maxlen=512)
                mapping = tokenizer.rematch(text, tokens)
                token_ids = tokenizer.tokens_to_ids(tokens)
                segment_ids = [0] * len(token_ids)
                token_ids, segment_ids = to_array([token_ids], [segment_ids])
                nodes = model.predict([token_ids, segment_ids])[0]
                labels = self.decode(nodes)
                entities, starting = [], False
                for i, label in enumerate(labels):
                    if label > 0:
                        if label % 2 == 1:
                            starting = True
                            entities.append([[i], categories[(label - 1) // 2]])
                        elif starting:
                            entities[-1][0].append(i)
                        else:
                            starting = False
                    else:
                        starting = False
                return [(mapping[w[0]][0], mapping[w[-1]][-1], l) for w, l in entities]

        ner = NamedEntityRecognizer(trans=K.eval(CRF.trans), starts=[0], ends=[0])
        return ner

    def batch_ner(self, data=None):
        """给brand做个ner"""
        return [{(text[s:d + 1], s): l for s, d, l in self.ner.recognize(text)} for text in data]

    def match_ner(self, text: str):
        """给brand做个ner"""
        return {(text[s:d + 1], s): l for s, d, l in self.ner.recognize(text)}


class TrainNERParser:
    def __init__(self, task_name, category=['brand']):
        self.category = category
        if platform.system()[:5] == "Linux":
            self.albert_dir = Path('/data2/zhang.zhenbang/albert_base_zh/')
        else:
            self.albert_dir = Path('D:/bert/albert_base_zh/')
        self.config_path = self.albert_dir / 'albert_config.json'
        self.checkpoint_path = self.albert_dir / 'model.ckpt-best'
        self.dict_path = self.albert_dir / 'vocab_chinese.txt'

        self.base_dir = Path(app.output_path('tiktok')) / task_name
        self.ner_dir = self.base_dir / 'ner'

    def train_ner(self):
        from bert4keras.backend import keras, K
        from bert4keras.models import build_transformer_model
        from bert4keras.tokenizers import Tokenizer
        from bert4keras.optimizers import Adam
        from bert4keras.snippets import sequence_padding, DataGenerator
        from bert4keras.snippets import open, ViterbiDecoder, to_array
        from bert4keras.layers import ConditionalRandomField
        from keras.layers import Dense
        from keras.models import Model
        from tqdm import tqdm

        maxlen = 256
        epochs = 10
        batch_size = 32
        bert_layers = 12
        learning_rate = 2e-5  # bert_layers越小，学习率应该要越大
        crf_lr_multiplier = 1000  # 必要时扩大CRF层的学习率
        categories = set()

        def load_data(filename):
            """加载数据
            单条格式：[text, (start, end, label), (start, end, label), ...]，
                      意味着text[start:end + 1]是类型为label的实体。
            """
            D = []
            with open(filename, encoding='utf-8') as f:
                f = f.read()
                for l in f.split('\n\n'):
                    if not l:
                        continue
                    d = ['']
                    for i, c in enumerate(l.split('\n')):
                        char, flag = c.split(' ')
                        d[0] += char
                        if flag[0] == 'B':
                            d.append([i, i, flag[2:]])
                            categories.add(flag[2:])
                        elif flag[0] == 'I':
                            d[-1][1] = i
                    D.append(d)
            return D

        # 标注数据
        train_data = load_data(self.ner_dir / 'data' / 'train.txt')
        valid_data = load_data(self.ner_dir / 'data' / 'dev.txt')
        test_data = load_data(self.ner_dir / 'data' / 'test.txt')
        categories = list(sorted(categories))

        # 建立分词器
        tokenizer = Tokenizer(str(self.dict_path), do_lower_case=True)

        class data_generator(DataGenerator):
            """数据生成器
            """

            def __iter__(self, random=False):
                batch_token_ids, batch_segment_ids, batch_labels = [], [], []
                for is_end, d in self.sample(random):
                    tokens = tokenizer.tokenize(d[0], maxlen=maxlen)
                    mapping = tokenizer.rematch(d[0], tokens)
                    start_mapping = {j[0]: i for i, j in enumerate(mapping) if j}
                    end_mapping = {j[-1]: i for i, j in enumerate(mapping) if j}
                    token_ids = tokenizer.tokens_to_ids(tokens)
                    segment_ids = [0] * len(token_ids)
                    labels = np.zeros(len(token_ids))
                    for start, end, label in d[1:]:
                        if start in start_mapping and end in end_mapping:
                            start = start_mapping[start]
                            end = end_mapping[end]
                            labels[start] = categories.index(label) * 2 + 1
                            labels[start + 1:end + 1] = categories.index(label) * 2 + 2
                    batch_token_ids.append(token_ids)
                    batch_segment_ids.append(segment_ids)
                    batch_labels.append(labels)
                    if len(batch_token_ids) == self.batch_size or is_end:
                        batch_token_ids = sequence_padding(batch_token_ids)
                        batch_segment_ids = sequence_padding(batch_segment_ids)
                        batch_labels = sequence_padding(batch_labels)
                        yield [batch_token_ids, batch_segment_ids], batch_labels
                        batch_token_ids, batch_segment_ids, batch_labels = [], [], []

        model = build_transformer_model(
            str(self.config_path),
            str(self.checkpoint_path),
            model='albert',
        )
        output_layer = 'Transformer-FeedForward-Norm'
        output = model.get_layer(output_layer).get_output_at(bert_layers - 1)
        output = Dense(len(categories) * 2 + 1)(output)
        CRF = ConditionalRandomField(lr_multiplier=crf_lr_multiplier)
        output = CRF(output)

        model = Model(model.input, output)
        model.summary()

        model.compile(
            loss=CRF.sparse_loss,
            optimizer=Adam(learning_rate),
            metrics=[CRF.sparse_accuracy]
        )

        class NamedEntityRecognizer(ViterbiDecoder):
            """命名实体识别器
            """

            def recognize(self, text):
                tokens = tokenizer.tokenize(text, maxlen=512)
                mapping = tokenizer.rematch(text, tokens)
                token_ids = tokenizer.tokens_to_ids(tokens)
                segment_ids = [0] * len(token_ids)
                token_ids, segment_ids = to_array([token_ids], [segment_ids])
                nodes = model.predict([token_ids, segment_ids])[0]
                labels = self.decode(nodes)
                entities, starting = [], False
                for i, label in enumerate(labels):
                    if label > 0:
                        if label % 2 == 1:
                            starting = True
                            entities.append([[i], categories[(label - 1) // 2]])
                        elif starting:
                            entities[-1][0].append(i)
                        else:
                            starting = False
                    else:
                        starting = False
                return [(mapping[w[0]][0], mapping[w[-1]][-1], l) for w, l in entities]

        NER = NamedEntityRecognizer(trans=K.eval(CRF.trans), starts=[0], ends=[0])

        def evaluate(data):
            """评测函数
            """
            X, Y, Z = 1e-10, 1e-10, 1e-10
            for d in tqdm(data, ncols=100):
                R = set(NER.recognize(d[0]))
                T = set([tuple(i) for i in d[1:]])
                X += len(R & T)
                Y += len(R)
                Z += len(T)
            f1, precision, recall = 2 * X / (Y + Z), X / Y, X / Z
            return f1, precision, recall

        _ner_dir = self.ner_dir

        class Evaluator(keras.callbacks.Callback):
            """评估与保存
            """

            def __init__(self):
                self.best_val_f1 = 0

            def on_epoch_end(self, epoch, logs=None):
                trans = K.eval(CRF.trans)
                NER.trans = trans
                print(NER.trans)
                f1, precision, recall = evaluate(valid_data)
                # 保存最优
                if f1 >= self.best_val_f1:
                    self.best_val_f1 = f1
                    model.save_weights(str(_ner_dir / 'model' / 'best_model.weights'))
                print(
                    'valid:  f1: %.5f, precision: %.5f, recall: %.5f, best f1: %.5f\n' %
                    (f1, precision, recall, self.best_val_f1)
                )
                f1, precision, recall = evaluate(test_data)
                print(
                    'test:  f1: %.5f, precision: %.5f, recall: %.5f\n' %
                    (f1, precision, recall)
                )

        evaluator = Evaluator()
        train_generator = data_generator(train_data, batch_size)

        model.fit(
            train_generator.forfit(),
            steps_per_epoch=len(train_generator),
            epochs=epochs,
            callbacks=[evaluator]
        )
