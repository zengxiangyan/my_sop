import argparse
import platform

import sys
from os.path import abspath, join, dirname

sys.path.insert(0, join(abspath(dirname('__file__')), '../'))
import application as app
from models.nerl import *

json_file = './config/nerl.json' if platform.system() == 'Windows' else './nerl_linux.json'
configs = Configure(config_json_file=json_file)
logger.Logger(config=configs)

parser = argparse.ArgumentParser(description='NERL with model')
parser.add_argument('--load', action="store_true", help='Load Data from Test Sample')
parser.add_argument('--process', action="store_true", help='Pre-process Data')
parser.add_argument('--train_ner', action="store_true", help='Train NER Model')
parser.add_argument('--train_match', action="store_true", help='Train MATCH Model')
parser.add_argument('--train_nel', action="store_true", help='Train NEL Model')
parser.add_argument('--predict_ner', action="store_true", help='Predict NER Model')
parser.add_argument('--predict_match', action="store_true", help='Predict MATCH Model')
parser.add_argument('--predict_nel', action="store_true", help='Predict NEL Model')
parser.add_argument('--merge_ner_result', action="store_true", help='Merge NER & MATCH Result')
parser.add_argument('--merge_nel_result', action="store_true", help='Merge NEL Result')
parser.add_argument('--test', action="store_true", help='Test Code')
args = parser.parse_args()

if __name__ == '__main__':
    for f in [configs.data.raw_dir, configs.data.data_dir, configs.train.model_dir]:
        app.mkdirs(app.output_path(f))

    if args.load:
        logger.info("Load Data from Test Sample")
        get_train_data(configs.data.raw_dir)
        get_kb_data(configs.data.raw_dir)

    if args.process:
        logger.info("Pre-process Data")
        kb_processing(configs)
        # 生成描述文本
        generate_description(configs)

    if args.train_ner:
        logger.info("Train NER Model")
        # 获取input_train_ner.pkl, 转换输入到bert可识别的输入数据保存
        get_input_bert(configs)
        # 序列标注训练BIO
        train_ner(configs)

    if args.train_match:
        # 获取split_entity.pkl
        get_split_entity(configs)
        # 获取id_embedding.pkl
        extract(configs)
        # 获取entity_embedding.pkl
        entity_embedding(configs)

        # 获取input_train_match.pkl
        get_input_match(configs)
        # 字典树匹配训练bert_vector
        train_match(configs)

    if args.train_nel:
        # 获取input_train_nel.pkl
        get_input(configs)
        # nel训练
        train_nel(configs)

    if args.test:
        ...
        # generate_description(configs)

    if args.predict_ner:
        # 获取input_develop_ner.pkl, 转换输入到bert可识别的输入数据保存
        get_input_bert(configs, mode='develop')
        # result_develop_ner.pkl
        predict_ner(configs)

    if args.predict_match:
        # 获取input_train_match.pkl
        get_input_match(configs, mode='develop')
        # result_develop_match.pkl
        predict_match(configs)

    if args.predict_nel:
        get_input(configs, mode='develop')

        for i in range(5):
            predict_f1(configs, model_index=i)
            predict_loss(configs, model_index=i)

    if args.merge_ner_result:
        get_NER_result(configs)

    if args.merge_nel_result:

        get_NEL_result([join(app.output_path(configs.data.result_dir), f'{i}_loss_result_develop_nel.json') for i in range(5)],
                       join(app.output_path(configs.data.result_dir), f'result_nel.json'))


# name = "MEUNIER/曼妮雅卸妆油深层清洁眼唇脸部温和不刺激敏感肌适用正品"
# label = [{"gid": -1, "mention": "MEUNIER", "offset": 0},
#          {"gid": -1, "mention": "曼妮雅", "offset": 8},
#          {"gid": 2351136, "mention": "卸妆油", "offset": 11},
#          {"gid": 547635248, "mention": "深层清洁", "offset": 14},
#          {"gid": 284872776, "mention": "眼唇", "offset": 18},
#          {"gid": 284876872, "mention": "脸部", "offset": 20},
#          {"gid": 293261384, "mention": "不刺激", "offset": 24},
#          {"gid": 275468512, "mention": "敏感肌", "offset": 27},
#          {"gid": 290320440, "mention": "正品", "offset": 32}]
#
# name = "柏蕊诗绿晶卸妆霜膏温和深层清洁卸妆油卸妆水娇兰佳人卸妆膏"
# label = [{"gid": 14966904, "mention": "柏蕊诗", "offset": 0},
#          {"gid": 2351136, "mention": "卸妆霜膏", "offset": 5},
#          {"gid": 547635248, "mention": "深层清洁", "offset": 11},
#          {"gid": 2351136, "mention": "卸妆油", "offset": 15},
#          {"gid": 2351136, "mention": "卸妆水", "offset": 18},
#          {"gid": 15224872, "mention": "娇兰佳人", "offset": 21},
#          {"gid": 2351136, "mention": "卸妆膏", "offset": 25}]
