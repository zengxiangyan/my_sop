import pandas as pd
# from evaluate import ner_eval
# from keras_layers import CRF
from sklearn.model_selection import KFold
from keras.layers import *
from keras.models import *
from keras.optimizers import *
from keras.callbacks import *
from keras_bert import load_trained_model_from_checkpoint
import logging
import argparse
import os
from os.path import abspath, join, dirname

sys.path.insert(0, join(abspath(dirname('__file__')), '../'))
import application as app
from models.nerl import ner_eval, CRF

os.environ["CUDA_VISIBLE_DEVICES"] = "1"

logging.basicConfig(level=logging.DEBUG, filename='NER_bert_crf.log', filemode='a',
                    format='%(asctime)s - %(levelname)s: %(message)s')


# parser = argparse.ArgumentParser()
# parser.add_argument('--dropout', type=float, default=0.2)
# parser.add_argument('--batch_size', type=int, default=64)
# parser.add_argument('--num_epochs', type=int, default=7)
# parser.add_argument('--mode', type=int, default=1)
#
# arg = parser.parse_args()


class Evaluate(Callback):
    """
    keras类继承于Callback，每一轮评价一次，并按照f1保存模型
    """

    def __init__(self, validation_data, filepath, factor=0.5, patience=1, min_lr=1e-4, stop_patience=3):
        val_data, val_label = validation_data
        self.F1 = []
        self.val = val_data
        self.label = val_label
        self.best = 0.
        self.factor = factor
        self.min_lr = min_lr
        self.patience = patience
        self.wait = 0
        self.stop_patience = stop_patience
        self.filepath = filepath

    def on_epoch_end(self, epoch, logs=None):
        print('Evaluate:')
        precision, recall, f1, = self.evaluate()

        if f1 > self.best:
            self.best = f1
            self.model.save_weights(self.filepath)
        print(' precision: %.6f, recall: %.6f,f1: %.6f, best f1: %.6f\n' % (
            float(precision), float(recall), float(f1), float(self.best)))

        logging.debug(str(precision) + ' ' + str(recall) + ' ' + str(f1))

    def evaluate(self):
        pred = self.model.predict(self.val)
        return ner_eval(self.label, pred)

    def stop_train(self, F1, best_f1, stop_patience):
        stop = True
        for f in F1[-stop_patience:]:
            if f >= best_f1:
                stop = False
        if stop == True:
            print('EarlyStopping!!!')
            self.model.stop_training = True


def bert_model(config):
    """
    BERT+CRF模型

    :return:
    """
    # bert_path = 'bert_model/'
    # config_path = bert_path + 'bert_config.json'
    # checkpoint_path = bert_path + 'bert_model.ckpt'

    bert_dir = config.data.bert_dir
    config_path = join(bert_dir, 'bert_config.json')
    checkpoint_path = join(bert_dir, 'bert_model.ckpt')

    bert_model = load_trained_model_from_checkpoint(config_path, checkpoint_path, trainable=True, seq_len=52)
    x = bert_model.output
    x = TimeDistributed(Dropout(config.train.dropout))(x)
    crf = CRF(units=4, learn_mode='join', test_mode='viterbi', sparse_target=True)
    x = crf(x)
    model = Model(bert_model.inputs, x)
    model.compile(optimizer=adam(1e-5), loss=crf.loss_function, metrics=[crf.accuracy])
    return model


def get_input(input_file):
    data = pd.read_pickle(input_file)
    inputs = [data['ids'], data['seg'], np.expand_dims(data['labels'], axis=-1)]
    return inputs


def step_decay(epoch):
    """
    调整学习率

    :param epoch:
    :return:
    """
    if epoch < 3:
        return 1e-5
    else:
        return 1e-6


def train_ner(config):
    """
    NER模型训练，9折交叉验证，分贝用loss和f1保存模型，共18个

    :return:
    """
    train_file = join(app.output_path(config.data.data_dir), 'input_train_ner.pkl')
    model_dir = app.output_path(config.train.model_dir)

    train = get_input(train_file)
    kfold = KFold(n_splits=config.train.k_fold, shuffle=False)
    for i, (tra_index, val_index) in enumerate(kfold.split(train[0])):
        K.clear_session()
        input_train = [tra[tra_index] for tra in train]
        input_val = [tra[val_index] for tra in train]
        filepath_loss = join(model_dir, "NER_bert_crf_loss.h5_" + str(i))
        filepath_f1 = join(model_dir, "NER_bert_crf_f1.h5_" + str(i))
        evaluate = Evaluate((input_val[:-1], input_val[-1]), filepath_f1)
        checkpoint = ModelCheckpoint(filepath_loss, monitor='val_loss', verbose=2, save_best_only=True, mode='min')

        earlystopping = EarlyStopping(monitor='val_loss', min_delta=0.000001, patience=2, verbose=2, mode='auto')

        lrate = LearningRateScheduler(step_decay, verbose=2)
        callbacks = [checkpoint, evaluate, lrate, earlystopping]
        model = bert_model(config)
        print(model.summary())
        logging.debug(str(i) * 30)
        model = model.fit(input_train[:-1], input_train[-1],
                          batch_size=config.train.batch_size,
                          epochs=config.train.num_epochs,
                          validation_data=(input_val[:-1], input_val[-1]), verbose=1, callbacks=callbacks)
        logging.debug(config.train.__str__())
        logging.debug(model.history)
        logging.debug(np.min(model.history['val_loss']))


def predict_ner(config, input_file='input_develop_ner.pkl', out_file='result_develop_ner.npy'):
    """
    NER模型预测，用loss和f1保存的18个模型进行预测，保存结果，后续会对结果投票选择实体

    :param config:
    :param input_file:
    :param out_file:
    :return:
    """
    model_dir = app.output_path(config.train.model_dir)
    input_file = join(app.output_path(config.data.data_dir), input_file)
    out_file = join(app.output_path(config.data.result_dir), out_file)

    print('test predict')
    all_test = []
    for i in range(config.train.k_fold):
        model = bert_model(config)
        filepath_loss = join(model_dir, "NER_bert_crf_loss.h5_" + str(i))
        test = get_input(input_file)
        model.load_weights(filepath_loss)
        y_pred_test = model.predict(test[:-1], batch_size=8, verbose=2)
        all_test.append(y_pred_test)

        model = bert_model(config)
        filepath_f1 = join(model_dir, "NER_bert_crf_f1.h5_" + str(i))
        test = get_input(input_file)
        model.load_weights(filepath_f1)
        y_pred_test = model.predict(test[:-1], batch_size=8, verbose=2)
        all_test.append(y_pred_test)

    np.save(out_file, all_test)
