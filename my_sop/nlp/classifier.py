# coding: UTF-8
import time
import torch
import numpy as np
from importlib import import_module
import argparse

import sys
from os.path import abspath, join, dirname, exists

sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
import application as app
from models.analyze.NLP_models.train_eval import train, init_network, predict

parser = argparse.ArgumentParser(description='Chinese Text Classification')
parser.add_argument('--model', type=str, required=True,
                    choices=['TextCNN', 'TextRNN', 'FastText', 'TextRCNN', 'TextRNN_Att', 'DPCNN', 'Transformer'],
                    help='choose a model: TextCNN, TextRNN, FastText, TextRCNN, TextRNN_Att, DPCNN, Transformer')
parser.add_argument('--dataset', required=True, type=str, help='dataset')
parser.add_argument('--embedding', default='pre_trained', type=str, help='random or pre_trained')
parser.add_argument('--word', default=False, type=bool, help='True for word, False for char')

parser.add_argument('--train', action="store_true", help='Train')
parser.add_argument('--predict', action="store_true", help='Predict')
args = parser.parse_args()


if __name__ == '__main__':
    dataset = args.dataset  # 数据集

    # 搜狗新闻:embedding_SougouNews.npz, 腾讯:embedding_Tencent.npz, 随机初始化:random
    embedding = 'word2vec.npz'
    if args.embedding == 'random':
        embedding = 'random'
    model_name = args.model  # 'TextRCNN'  # TextCNN, TextRNN, FastText, TextRCNN, TextRNN_Att, DPCNN, Transformer
    if model_name == 'FastText':
        from models.analyze.NLP_models.utils_fasttext import build_dataset, build_iterator, get_time_dif

        embedding = 'random'
    else:
        from models.analyze.NLP_models.utils import build_dataset, build_iterator, get_time_dif

    x = import_module('models.analyze.NLP_models.' + model_name)
    config = x.Config(app.output_path(dataset), embedding)
    np.random.seed(1)
    torch.manual_seed(1)
    torch.cuda.manual_seed_all(1)
    torch.backends.cudnn.deterministic = True  # 保证每次结果一样

    if args.train:
        start_time = time.time()
        print("Loading data...")
        vocab, train_data, dev_data, test_data = build_dataset(config, args.word)
        train_iter = build_iterator(train_data, config)
        dev_iter = build_iterator(dev_data, config)
        test_iter = build_iterator(test_data, config)

        # train
        config.n_vocab = len(vocab)
        model = x.Model(config).to(config.device)
        if model_name != 'Transformer':
            init_network(model)
        print(model.parameters)
        train(config, model, train_iter, dev_iter, test_iter)

        time_dif = get_time_dif(start_time)
        print("Time usage:", time_dif)

    if args.predict:
        start_time = time.time()
        print("Loading data...")
        vocab, predict_data = build_dataset(config, args.word, mode='predict')
        predict_iter = build_iterator(predict_data, config)

        # predict
        config.n_vocab = len(vocab)
        model = x.Model(config).to(config.device)
        if model_name != 'Transformer':
            init_network(model)
        print(model.parameters)
        labels = predict(config, model, predict_iter)
        np.save(app.output_path(dataset) + '/predict_{}.npy'.format(model_name), labels)
