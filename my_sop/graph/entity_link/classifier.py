import numpy as np
from importlib import import_module
import sys
from os.path import abspath, join, dirname

sys.path.insert(0, join(abspath(dirname(__file__)), '../../'))
import application as app
from models.chinese_text_classification import *


class ClassifierParser:
    def __init__(self, dataset='lv0cid_classifier', model_name='ERNIE'):
        self.dataset = app.output_path(dataset)
        self.model_name = model_name
        self.x = import_module('models.chinese_text_classification.models.' + self.model_name)
        self.config = self.x.Config(self.dataset)

    def predict(self, data):
        predict_data = embedding(self.config.tokenizer, data)
        predict_iter = build_iterator(predict_data, self.config)
        model = self.x.Model(self.config).to(self.config.device)
        scores, labels = predict(self.config, model, predict_iter)
        labels = np.array(self.config.class_list)[labels]

        assert len(data) == len(labels)
        return labels


if __name__ == '__main__':
    lv0cid_classifier = ClassifierParser()
    print(lv0cid_classifier.predict(
        ["杂粮主义糙米500g袋装五谷杂粮健身米饭粗粮八宝粥原材料糙米胚芽",
         "UGG2021秋冬新款女士服饰瑞伊毛绒徽标连帽衫长袖卫衣 1121385",
         "小米真无线蓝牙耳机Air2 SE通话降噪小米官方旗舰店适用苹果华为"]))
else:
    lv0cid_classifier = ClassifierParser()
