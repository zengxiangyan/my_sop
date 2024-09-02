# coding: UTF-8
import time
import torch
import numpy as np
from importlib import import_module
import argparse
import sys
from os.path import abspath, join, dirname

sys.path.insert(0, join(abspath(dirname(__file__)), '../'))
from models.chinese_text_classification import *

parser = argparse.ArgumentParser(description='Chinese Text Classification')
parser.add_argument('--dataset', required=True, type=str, help='dataset')
parser.add_argument('--model', type=str, required=True, help='choose a model: Bert, ERNIE')
parser.add_argument('--predict_path', type=str)

parser.add_argument('--train', action="store_true", help='Train')
parser.add_argument('--predict', action="store_true", help='Predict')
args = parser.parse_args()

if __name__ == '__main__':
    dataset = args.dataset  # 数据集

    model_name = args.model  # bert
    x = import_module('models.chinese_text_classification.models.' + model_name)
    config = x.Config(dataset)
    np.random.seed(1)
    torch.manual_seed(1)
    torch.cuda.manual_seed_all(1)
    torch.backends.cudnn.deterministic = True  # 保证每次结果一样

    if args.train:
        start_time = time.time()
        print("Loading data...")
        train_data, dev_data, test_data = build_dataset(config)
        train_iter = build_iterator(train_data, config)
        dev_iter = build_iterator(dev_data, config)
        test_iter = build_iterator(test_data, config)
        time_dif = get_time_dif(start_time)
        print("Time usage:", time_dif)

        # train
        model = x.Model(config).to(config.device)
        train(config, model, train_iter, dev_iter, test_iter)

    if args.predict:
        start_time = time.time()
        print("Loading data...")
        predict_data = load_dataset(config.tokenizer, args.predict_path, config.pad_size)

        predict_iter = build_iterator(predict_data, config)

        # predict
        model = x.Model(config).to(config.device)
        labels = predict(config, model, predict_iter)
        np.save(dataset + '/predict_{}.npy'.format(model_name), labels)

"""
# train
python new_classifier.py --dataset THUCNews --model ERNIE --train 
python new_classifier.py --dataset 91230 --model ERNIE --train 
# predict
python new_classifier.py --dataset THUCNews --model ERNIE --predict --predict_path ./THUCNews/data/test.txt
python new_classifier.py --dataset THUCNews --model 91230 --predict --predict_path ./91230/data/predict.txt
"""