import re
import ast
import jieba
from sklearn.model_selection import KFold
from keras.callbacks import *
from keras.layers import Embedding, Dense, LSTM, concatenate, Dropout
from keras import Input
from keras.models import Model
import numpy as np
import pandas as pd

embedding_dim = 100
max_len = 20


def merge_prop(line):
    prop_all_dict = dict()
    try:
        if isinstance(line['属性名'], list):
            for pn, pv in zip(line['属性名'], line['属性值']):
                if prop_all_dict.get(pn) is None:
                    prop_all_dict[pn] = set()
                prop_all_dict[pn].add(pv)
        else:
            for pn, pv in zip(ast.literal_eval(line['属性名']), ast.literal_eval(line['属性值'])):
                if prop_all_dict.get(pn) is None:
                    prop_all_dict[pn] = set()
                prop_all_dict[pn].add(pv)
    except ValueError:
        return ''
    return '，'.join(f'{pn}: {" ".join(pvs)}' for pn, pvs in prop_all_dict.items())


class Get_Sentences(object):
    lower = True

    def __init__(self, pos_content):
        self.content = pos_content

    def __iter__(self):
        re_ = re.compile(u"([\u4E00-\u9FD5a-zA-Z0-9#&\._%]+)")
        re_mine = re.compile("[\.0-9]+[\u4e00-\u9fa5]", re.U)

        for data_item in self.content:
            word = []
            for item in data_item:
                if not isinstance(item, str):
                    continue
                if self.lower:
                    item = item.lower()
                blocks = re_.split(item)
                for blk in blocks:
                    if re_.match(blk):
                        if len(blk) <= 5:
                            word.append(blk)
                        else:
                            if re.findall(re_mine, blk):
                                for gg in set(re.findall(re_mine, blk)):
                                    jieba.add_word(gg)
                            for w in jieba.cut(blk):
                                word.append(w)
            yield word


def train_word2vec(df, filename):
    from gensim.models import word2vec as w2v
    all_tokens = Get_Sentences(df.values.tolist())
    model = w2v.Word2Vec(all_tokens, size=embedding_dim, window=5, min_count=1, workers=6)
    model.wv.save_word2vec_format(filename, binary=False)


def get_word2vec(word2vec_file):
    import gensim
    _word2vec = gensim.models.KeyedVectors.load_word2vec_format(word2vec_file, binary=False)
    assert _word2vec.vector_size == embedding_dim, 'Wrong dimension for embedding...'
    return _word2vec


def get_embedding(word2vec_file):
    word2vec = get_word2vec(word2vec_file)
    word2index = {"UNK": 0}
    index2word = word2vec.index2word
    embeddings_matrix = np.zeros((len(index2word) + 1, embedding_dim))

    for i, word in enumerate(index2word):
        word2index[word] = i + 1
        embeddings_matrix[i + 1] = word2vec[word]

    for word in index2word:
        jieba.add_word(word)

    return word2index, embeddings_matrix


def data_processing(df, word2vec_file, output_pickle, mode='train'):
    from keras import preprocessing

    word2index, _ = get_embedding(word2vec_file)
    train_segments = []
    for tokens in Get_Sentences(df[['name']].values.tolist()):
        segment = []
        for word in tokens:
            segment.append(word2index.get(word, 0))
        train_segments.append(segment)
    x_train_name = np.array(train_segments)
    x_train_name = preprocessing.sequence.pad_sequences(x_train_name, maxlen=max_len)

    train_segments = []
    for tokens in Get_Sentences(df[['props']].values.tolist()):
        segment = []
        for word in tokens:
            segment.append(word2index.get(word, 0))
        train_segments.append(segment)
    x_train_prop = np.array(train_segments)
    x_train_prop = preprocessing.sequence.pad_sequences(x_train_prop, maxlen=max_len)

    if mode == 'train' or mode == 'dev':
        from keras.utils.np_utils import to_categorical
        y_train = np.array(df['label'].tolist())
        y_train = to_categorical(y_train, num_classes=7)

        pd.to_pickle((x_train_name, x_train_prop, y_train), output_pickle)
    else:
        pd.to_pickle((x_train_name, x_train_prop), output_pickle)


class ClassifyModel:
    def __init__(self, num_class, word2vec_file):
        self.num_class = num_class
        self.word2index, self.embeddings_matrix = get_embedding(word2vec_file)

    @staticmethod
    def step_decay(epoch):
        """
        用来控制学习率
        :param epoch:
        :return:
        """
        if epoch < 2:
            lr = 5e-5
            # lr = 2e-5
        else:
            lr = 2e-6
            # lr = 1e-6
        return lr

    def link_model(self):
        name_input = Input(shape=(None,), dtype='int32', name='name')  # 文本输入是一个长度可变的整数序列。可以选择对输入进行命名
        embedded_name = Embedding(len(self.embeddings_matrix), embedding_dim, weights=[self.embeddings_matrix],
                                  trainable=False)(
            name_input)  # 将输入嵌入长度为 64 的向量
        encoded_name = LSTM(128)(embedded_name)

        prop_input = Input(shape=(None,), dtype='int32', name='prop')  # 文本输入是一个长度可变的整数序列。可以选择对输入进行命名
        embedded_prop = Embedding(len(self.embeddings_matrix), embedding_dim, weights=[self.embeddings_matrix], trainable=False)(
            prop_input)  # 将输入嵌入长度为 64 的向量
        encoded_prop = LSTM(128)(embedded_prop)
        # encoded_prop = Dropout(0.5)(encoded_prop)

        concatenated = concatenate([encoded_name, encoded_prop], axis=-1)
        x = Dense(256, activation='relu')(concatenated)
        # x = Dense(128, activation='relu')(encoded_name)
        x = Dropout(0.2)(x)
        prediction = Dense(self.num_class, activation='softmax')(x)
        model = Model([name_input, prop_input], prediction)
        # model = Model(name_input, prediction)
        model.compile(optimizer='rmsprop', loss='categorical_crossentropy', metrics=['accuracy'])
        return model

    def train(self, input_file, model_file, batch_size=128):
        # index, x_train_name, x_train_prop, y_train = pd.read_pickle(input_file)
        x_train_name, x_train_prop, y_train = pd.read_pickle(input_file)

        kfold = KFold(n_splits=3, shuffle=False)
        for i, (tra_index, val_index) in enumerate(kfold.split(x_train_name)):
            K.clear_session()
            input_train_name = x_train_name[tra_index]
            input_train_prop = x_train_prop[tra_index]
            input_val_name = x_train_name[val_index]
            input_val_prop = x_train_prop[val_index]

            output_train_label = y_train[tra_index]
            output_val_label = y_train[val_index]

            model = self.link_model()
            model.summary()
            # from keras.utils import plot_model
            # plot_model(model, to_file='Lstm_model.png', show_shapes=True)
            # exit()

            checkpoint = ModelCheckpoint(str(model_file) + f'_{i}', monitor='val_loss', verbose=2,
                                         save_weights_only=True,
                                         save_best_only=True, mode='min')
            earlystopping = EarlyStopping(monitor='val_loss', min_delta=0.0001, patience=2, verbose=2, mode='auto')
            lrate = LearningRateScheduler(self.step_decay, verbose=2)
            callbacks = [checkpoint, lrate, earlystopping]

            model.fit([input_train_name, input_train_prop], output_train_label,
                      epochs=20,
                      batch_size=batch_size,
                      validation_data=([input_val_name, input_val_prop], output_val_label),
                      verbose=1,
                      callbacks=callbacks)

    def test(self, input_file, model_file):
        x_train_name, x_train_prop, y_train = pd.read_pickle(input_file)

        labels = None
        for i in range(3):
            model = self.link_model()
            model.load_weights(str(model_file) + f'_{i}')
            if labels is None:
                labels = model.predict([x_train_name, x_train_prop])
            else:
                labels += model.predict([x_train_name, x_train_prop])

        labels = np.argmax(labels, axis=1)
        answers = np.argmax(y_train, axis=1)
        prediction = np.equal(labels, answers)
        accuracy = np.mean(prediction)
        return accuracy

    def predict(self, input_file, model_file, noi=0):
        x_train_name, x_train_prop = pd.read_pickle(input_file)

        labels = None
        if noi:
            model = self.link_model()
            model.load_weights(str(model_file) + f'_{noi}')
            labels = model.predict([x_train_name, x_train_prop])
            labels = np.argmax(labels, axis=1)
        else:
            for i in range(3):
                model = self.link_model()
                model.load_weights(str(model_file) + f'_{i}')
                if labels is None:
                    labels = model.predict([x_train_name, x_train_prop])
                else:
                    labels += model.predict([x_train_name, x_train_prop])

            labels = np.argmax(labels, axis=1)
        return labels

    def data_processing(self, df, output_pickle, mode='train'):
        from keras import preprocessing

        train_segments = []
        for tokens in Get_Sentences(df[['name']].values.tolist()):
            segment = []
            for word in tokens:
                segment.append(self.word2index.get(word, 0))
            train_segments.append(segment)
        x_train_name = np.array(train_segments)
        x_train_name = preprocessing.sequence.pad_sequences(x_train_name, maxlen=max_len)

        train_segments = []
        for tokens in Get_Sentences(df[['props']].values.tolist()):
            segment = []
            for word in tokens:
                segment.append(self.word2index.get(word, 0))
            train_segments.append(segment)
        x_train_prop = np.array(train_segments)
        x_train_prop = preprocessing.sequence.pad_sequences(x_train_prop, maxlen=max_len)

        if mode == 'train' or mode == 'dev':
            from keras.utils.np_utils import to_categorical
            y_train = np.array(df['label'].tolist())
            y_train = to_categorical(y_train, num_classes=self.num_class)

            pd.to_pickle((x_train_name, x_train_prop, y_train), output_pickle)
        else:
            pd.to_pickle((x_train_name, x_train_prop), output_pickle)