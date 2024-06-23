# coding=utf-8
import csv
import platform
import re
import random
import sys, os, shutil
import pandas as pd
from tqdm import tqdm
from annoy import AnnoyIndex
from math import log2
from bert_serving.client import BertClient
from os.path import abspath, join, dirname, exists

sys.path.insert(0, join(abspath(dirname('__file__')), '../../'))
import application as app
from extensions import utils
from nlp import pnlp

db = app.connect_db('graph')
base_dir = app.output_path('tf/test_sku4')


def relative_path(dir):
    return '.' + dir.split('output')[1]


class sku_classifier:
    def __init__(self, eid):
        self.eid = eid
        self.env_dir = join(base_dir, str(eid))
        app.mkdirs(join(self.env_dir, 'item'))
        app.mkdirs(join(self.env_dir, 'data'))
        app.mkdirs(join(self.env_dir, 'model'))
        app.mkdirs(join(self.env_dir, 'fine-tune/'))
        app.mkdirs(join(self.env_dir, 'classified/'))
        app.mkdirs(join(self.env_dir, 'merge'))
        app.mkdirs(join(self.env_dir, 'result/'))

    def init_by_pickle(self):
        pass

    def item_classify_sku_cache(self):
        pid_cid_bid = dict()  # {pid: {'cid':[cid], 'alias_all_bid': [alias_all_bid]}}
        file_path = join(self.env_dir, 'item', 'entity_item.csv')
        print('caching items into %s' % relative_path(file_path))
        sql = 'select a.pkid, a.eid, a.name, a.cid, a.alias_all_bid, pid_new, sp1 ' \
              'from graph.product_lib_item a left join product_lib.entity_90526_item b ' \
              'on (a.id = b.id) where a.eid=90526 and a.split=0 and pid_new!=0;'.format(table=self.eid)
        data = db.query_all(sql)
        with open(file_path, 'w', encoding='gb18030', newline='') as f:
            csv_w = csv.writer(f)
            csv_w.writerow(('id', 'eid', 'name', 'cid', 'alias_all_bid', 'pid', 'category'))
            for item in data:
                id, eid, name, cid, alias_all_bid, pid, category = item
                csv_w.writerow(item)
                if pid_cid_bid.get(pid) is None:
                    pid_cid_bid[pid] = {'cid': set(), 'alias_all_bid': set()}
                pid_cid_bid[pid]['cid'].add(cid)
                pid_cid_bid[pid]['alias_all_bid'].add(alias_all_bid)

        file_path = join(self.env_dir, 'item', 'skus_view.csv')
        print('caching skus into %s' % relative_path(file_path))
        sql = 'SELECT pkid, {eid} eid, name, alias_all_bid FROM graph.product_lib_product where pkid in {pids};'.format(
            eid=self.eid, pids=tuple(pid_cid_bid.keys()))
        data = db.query_all(sql)
        skus = dict()
        with open(file_path, 'w', encoding='gb18030', newline='') as f:
            csv_w = csv.writer(f)
            csv_w.writerow(('pid', 'eid', 'name', 'cids', 'alias_all_bids'))
            for sku in data:
                pid, eid, name, alias_all_bid = sku
                cids = pid_cid_bid[pid]['cid']
                alias_all_bids = pid_cid_bid[pid]['alias_all_bid']
                alias_all_bids.add(alias_all_bid)
                csv_w.writerow((pid, eid, name,
                                '/'.join([str(i) for i in cids]),
                                '/'.join([str(i) for i in alias_all_bids])))
                skus[pid] = {'name': name, 'cids': cids, 'alias_all_bid': alias_all_bids}

    def item_to_train_dev_test(self):
        data = []
        with open(join(self.env_dir, 'item/entity_item.csv'), newline='', encoding='gb18030') as input_f:
            next(input_f)
            csv_r = csv.reader(input_f)
            for item in csv_r:
                data.append(item)

        train, dev_test = [], []
        rng = random.Random(12345)
        indexes = list(range(len(data)))
        rng.shuffle(indexes)
        counter = dict()
        for index in indexes:
            if data[index][-1] == '':
                continue
            num = counter[data[index][-2]] = counter.get(data[index][-2], 0) + 1
            if num <= 10:
                train.append(data[index])
            else:
                dev_test.append(data[index])
        if int(len(data) * 0.6) - len(train) > 0:
            train_addition = rng.sample(dev_test, int((len(train) + len(dev_test)) * 0.6) - len(train))
            train.extend(train_addition)
            dev_test = [item for item in dev_test if item not in train_addition]
        dev = rng.sample(dev_test, int(0.5 * len(dev_test)))
        test = [item for item in dev_test if item not in dev]

        with open(join(self.env_dir, 'data/train.tsv'), 'w', encoding='utf-8', newline='') as f:
            csv_w = csv.writer(f, delimiter='\t')
            csv_w.writerow(('category', 'id', 'name'))
            for item in train:
                id, eid, name, cid, alias_all_bid, pid, category = item
                csv_w.writerow((category, id, re.sub('[ ]+', ' ', re.sub('[\n\t]', ' ', name))))

        with open(join(self.env_dir, 'data/dev.tsv'), 'w', encoding='utf-8', newline='') as f:
            csv_w = csv.writer(f, delimiter='\t')
            csv_w.writerow(('category', 'id', 'name'))
            for item in dev:
                id, eid, name, cid, alias_all_bid, pid, category = item
                csv_w.writerow((category, id, re.sub('[ ]+', ' ', re.sub('[\n\t]', ' ', name))))

        with open(join(self.env_dir, 'data/test.tsv'), 'w', encoding='utf-8', newline='') as f:
            csv_w = csv.writer(f, delimiter='\t')
            csv_w.writerow(('category', 'id', 'name'))
            for item in test:
                id, eid, name, cid, alias_all_bid, pid, category = item
                csv_w.writerow((category, id, re.sub('[ ]+', ' ', re.sub('[\n\t]', ' ', name))))

        with open(join(self.env_dir, 'item/skus_view.csv'), newline='', encoding='gb18030') as input_f:
            next(input_f)
            csv_r = csv.reader(input_f)
            with open(join(self.env_dir, 'data/sku.tsv'), 'w', encoding='utf-8', newline='') as f:
                csv_w = csv.writer(f, delimiter='\t')
                csv_w.writerow(('category', 'pid', 'name'))
                for sku in csv_r:
                    pid, eid, name, cids, alias_all_bids = sku
                    csv_w.writerow(('其它', pid, re.sub('[ ]+', ' ', re.sub('[\n\t]', ' ', name))))
        print('create train and test file for', self.eid)
        print('train length :', len(train))
        print('dev  length :', len(dev))
        print('test  length :', len(test))

    def keyword_cache(self):
        input_filename = join(self.env_dir, 'item/entity_item.csv')
        bid0_set, cid0_set = set(), set()
        with open(input_filename, newline='', encoding='gb18030') as input_f:
            next(input_f)
            csv_r = csv.reader(input_f)
            for item in csv_r:
                bid0_set.add(int(item[4]))
                cid0_set.add(int(item[3]))

        # brand keyword
        sql = 'select name from graph.brand where alias_bid0 in ({string}) and confirmType!=2 and deleteFlag!=1;' \
            .format(string=('%s, ' * len(bid0_set))[:-2])
        brand_name = db.query_all(sql, tuple(bid0_set))
        brand_keyword = []
        for name in brand_name:
            brand_keyword.extend(pnlp.cut_brand_name_base(name[0]))
        brand_keyword = list(set(brand_keyword))
        brand_keyword = [prop for prop in brand_keyword if len(prop) > 1]
        print(brand_keyword)

        # category keyword
        sql = 'select cid, name from graph.category where cid0 in ({string});'.format(
            string=('%s, ' * len(cid0_set))[:-2])
        category_cid_name = db.query_all(sql, tuple(cid0_set))
        category_keyword, cid_list = [], []
        for (cid, name) in category_cid_name:
            cid_list.append(cid)
            category_keyword.extend(pnlp.cut_brand_name_base(name))
        # print(category_keyword)

        # prop keyword
        sql = 'select cpn.cid, cpnid, cpn.pnid, cpn.name, t.pvid, pv_name from categorypropname cpn left join ' \
              '(select cpv.cpnid cpnid, cpv.pvid pvid, pv.name pv_name from categorypropvalue cpv left join ' \
              'propvalue pv on (cpv.pvid = pv.pvid)) t on (t.cpnid = cpn.id) where cpn.cid in ({string});' \
            .format(string=('%s, ' * len(cid_list))[:-2])
        props_info = db.query_all(sql, tuple(cid_list))
        prop_keyword, category_keyword_addition = set(), []
        for prop_info in props_info:
            cid, cpnid, pnid, pnname, pvid, pvname = prop_info
            if len(pvname) > 5:
                continue
            elif pnname[-2:] in ['分类', '种类', '品类']:
                category_keyword_addition.extend(pnlp.cut_brand_name_base(pvname))
            elif utils.is_chinese(pvname):
                prop_keyword.add(pvname)
        prop_keyword = [prop for prop in prop_keyword if len(prop) > 1]
        category_keyword_addition = list(set(category_keyword_addition))
        print(prop_keyword)
        # print(category_keyword_addition)
        category_keyword = category_keyword + category_keyword_addition
        if '其它' in category_keyword:
            category_keyword.remove('其它')
        category_keyword = list(set(category_keyword))
        category_keyword = [prop for prop in category_keyword if len(prop) > 1]
        print(category_keyword)
        pd.to_pickle((brand_keyword, category_keyword, prop_keyword), join(self.env_dir, 'item/keywords.pkl'))

    def create_sample_txt(self, create_negative=True):
        input_dir = join(self.env_dir, 'item')
        output_dir = join(self.env_dir, 'data')
        sku_dict = dict()
        with open(join(input_dir, 'skus_view.csv'), encoding='gb18030', newline='') as f:
            next(f)
            csv_r = csv.reader(f)
            for item in csv_r:
                sku_dict[item[0]] = item[2]

        input_filename = join(self.env_dir, 'item/entity_item.csv')
        output_filename = join(output_dir, 'sample_train.txt')
        with open(input_filename, newline='', encoding='gb18030') as input_f:
            with open(output_filename, 'w', encoding='utf-8') as output_f:
                next(input_f)
                csv_r = csv.reader(input_f)
                for item in csv_r:
                    output_f.write('{name}\t{sku}\t{label}\n'.format(
                        name=re.sub('[ ]+', ' ', re.sub('[\n\t]', ' ', item[2])), sku=sku_dict[item[5]], label=1))
                    if create_negative:
                        for _ in range(10):
                            for _ in range(10):
                                sku_random = random.choice(sku_dict.values())
                                if sku_random != sku_dict[item[5]] and sku_random is not None:
                                    output_f.write(
                                        '{name}\t{sku}\t{label}\n'.format(
                                            name=re.sub('[ ]+', ' ', re.sub('[\n\t]', ' ', item[2])),
                                            sku=sku_dict[item[5]], label=0))
                                    break
            print('wrote %s' % relative_path(output_filename))

    def create_pretraining_data(self):
        shutil.copyfile('../../models/tf/bert_config.json', join(self.env_dir, 'model', 'bert_config.json'))
        shutil.copyfile('../../models/tf/vocab.txt', join(self.env_dir, 'model', 'vocab.txt'))
        print('copy 2 files bert_config.json, vocab.txt to model_dir')
        print('create pretrainig data for wwm...')
        input_dir = join(self.env_dir, 'data/sample_train.txt')
        output_dir = join(self.env_dir, 'model/tf_examples.tfrecord')
        vocab_file = join(self.env_dir, 'model/vocab.txt')
        keyword_dir = join(self.env_dir, 'item/keywords.pkl')
        if platform.system() == 'Linux':
            os.system("p3 ../../extensions/bert/create_pretraining_data_QA.py --input_file={input_dir} "
                      "--output_file={output_dir} --vocab_file={vocab_file} --keyword_file={keyword_dir}"
                      .format(input_dir=input_dir, output_dir=output_dir, vocab_file=vocab_file,
                              keyword_dir=keyword_dir))
        elif platform.system() == 'Windows':
            os.system("python ../../extensions/bert/create_pretraining_data_QA.py --input_file={input_dir} "
                      "--output_file={output_dir} --vocab_file={vocab_file} --keyword_file={keyword_dir}"
                      .format(input_dir=input_dir, output_dir=output_dir, vocab_file=vocab_file,
                              keyword_dir=keyword_dir))
        print('create_pretraining_data done')

    def pretraining(self):
        print('pretrainig model...')
        input_file = join(self.env_dir, 'model/tf_examples.tfrecord')
        output_dir = join(self.env_dir, 'model')
        bert_config_file = join(self.env_dir, 'model/bert_config.json')
        if platform.system() == 'Linux':
            os.system("p3 ../../extensions/bert/run_pretraining.py --input_file={input_file} "
                      "--bert_config_file={bert_config_file} --output_dir={output_dir} --do_train=True --do_eval=True"
                      .format(input_file=input_file, bert_config_file=bert_config_file, output_dir=output_dir))
        elif platform.system() == 'Windows':
            os.system("python ../../extensions/bert/run_pretraining.py --input_file={input_file} "
                      "--bert_config_file={bert_config_file} --output_dir={output_dir} --do_train=True --do_eval=True"
                      .format(input_file=input_file, bert_config_file=bert_config_file, output_dir=output_dir))

        for num in range(6000, 10000, 1000):
            os.remove(join(self.env_dir, 'model/model.ckpt-{num}.meta'.format(num=num)))
            os.remove(join(self.env_dir, 'model/model.ckpt-{num}.index'.format(num=num)))
            os.remove(join(self.env_dir, 'model/model.ckpt-{num}.data-00000-of-00001'.format(num=num)))
        print('pretraining done')

    def classifier(self):
        print('classifying...')

        input_file = join(self.env_dir, 'data/')
        vocab_file = join(self.env_dir, 'model/vocab.txt')
        bert_config_file = join(self.env_dir, 'model/bert_config.json')
        init_checkpoint = join(self.env_dir, 'model/model.ckpt-10000')
        learning_rate = 2e-5
        output_dir = join(self.env_dir, 'fine-tune/')

        if platform.system() == 'Linux':
            os.system("p3 ../../extensions/bert/run_classifier_wine.py --task_name=Wine --do_train=true --do_eval=true "
                      "--data_dir={data_dir} --vocab_file={vocab_file} --bert_config_file={bert_config_file} "
                      "--init_checkpoint={init_checkpoint} --learning_rate={learning_rate} --output_dir={output_dir}".
                      format(data_dir=input_file, vocab_file=vocab_file, bert_config_file=bert_config_file,
                             init_checkpoint=init_checkpoint, learning_rate=learning_rate, output_dir=output_dir))
        elif platform.system() == 'Windows':
            os.system(
                "python ../../extensions/bert/run_classifier_wine.py --task_name=Wine --do_train=true --do_eval=true "
                "--data_dir={data_dir} --vocab_file={vocab_file} --bert_config_file={bert_config_file} "
                "--init_checkpoint={init_checkpoint} --learning_rate={learning_rate} --output_dir={output_dir}".
                    format(data_dir=input_file, vocab_file=vocab_file, bert_config_file=bert_config_file,
                           init_checkpoint=init_checkpoint, learning_rate=learning_rate, output_dir=output_dir))

    def check_classifier(self, check_file_name='test.tsv', correction=True):
        print('checking %s...' % check_file_name)
        input_file = join(self.env_dir, 'data/')
        vocab_file = join(self.env_dir, 'model/vocab.txt')
        bert_config_file = join(self.env_dir, 'model/bert_config.json')
        init_checkpoint = join(self.env_dir, 'fine-tune/')
        output_dir = join(self.env_dir, 'classified/')
        if platform.system() == 'Linux':
            os.system("p3 ../../extensions/bert/run_classifier_wine.py --task_name=Wine --do_predict=true "
                      "--data_dir={data_dir} --vocab_file={vocab_file} --bert_config_file={bert_config_file} "
                      "--init_checkpoint={init_checkpoint} --output_dir={output_dir} "
                      "--check_file_name={check_file_name} --correction={correction}".
                      format(data_dir=input_file, vocab_file=vocab_file, bert_config_file=bert_config_file,
                             init_checkpoint=init_checkpoint, output_dir=output_dir, check_file_name=check_file_name,
                             correction=correction))
        elif platform.system() == 'Windows':
            os.system("python ../../extensions/bert/run_classifier_wine.py --task_name=Wine --do_predict=true "
                      "--data_dir={data_dir} --vocab_file={vocab_file} --bert_config_file={bert_config_file} "
                      "--init_checkpoint={init_checkpoint} --output_dir={output_dir} "
                      "--check_file_name={check_file_name} --correction={correction}".
                      format(data_dir=input_file, vocab_file=vocab_file, bert_config_file=bert_config_file,
                             init_checkpoint=init_checkpoint, output_dir=output_dir, check_file_name=check_file_name,
                             correction=correction))

    def create_vector(self, set_type='item',
                      merge_filename='test.tsv', filename='entity_item.csv', output_filename='test_merge.pkl'):
        merge_dict = dict()
        with open(join(self.env_dir, 'classified/{filename}'.format(filename=merge_filename)),
                  'r', encoding='utf-8') as f:
            next(f)
            csv_r = csv.reader(f, delimiter='\t')
            for line in csv_r:
                merge_dict[line[0]] = line[2]

        data, name_list = [], []
        with open(join(self.env_dir, 'item/', filename), 'r', encoding='gb18030') as f:
            next(f)
            csv_r = csv.reader(f)
            for line in csv_r:
                if line[0] in merge_dict:
                    if set_type == 'item':
                        add_data = [line[0], line[2], line[4], line[5], merge_dict[line[0]]]
                    else:
                        add_data = [line[0], line[2], line[4], merge_dict[line[0]]]
                    data.append(add_data)
                    name_list.append(line[2])

        os.system('setsid bert-serving-start -model_dir={env_dir}/model/ '
                  '-ckpt_name=model.ckpt-10000 -num_worker=4 >t.log 2>&1 &'.format(env_dir=self.env_dir))
        bc = BertClient()
        vector_list = bc.encode(name_list)
        os.system("ps aux|grep bert-serving|awk '{print $2}'|xargs kill -9")
        for file in os.listdir(os.curdir):
            if file[:3] == 'tmp':
                shutil.rmtree(file)
                print('remove file %s' % file)
        os.remove('t.log')
        print('remove file t.log')

        for i, line in enumerate(data):
            line.append(vector_list[i])

        pd.to_pickle(data, join(self.env_dir, 'merge/', output_filename))

    def sku_task(self, sku_filename='sku_merge.pkl', test_filename='test_merge.pkl', train_filename='train_merge.pkl'):
        correct, correct3, correct5, correct10, total = 0, 0, 0, 0, 0
        skus_vector = pd.read_pickle(join(self.env_dir, 'merge/', sku_filename))
        test_vector = pd.read_pickle(join(self.env_dir, 'merge/', test_filename))
        train_vector = pd.read_pickle(join(self.env_dir, 'merge/', train_filename))

        pid_train = dict()
        adjust_vector = dict()
        for item in train_vector:
            if pid_train.get(int(item[3])) is None:
                pid_train[int(item[3])] = list()
            pid_train[int(item[3])].append(int(item[0]))
            adjust_vector[int(item[0])] = item[5]

        skus_vector_dict = {int(sku[0]): {'name': sku[1], 'alias_all_bid': sku[2].split('/'),
                                          'category': sku[3],
                                          'vector':
                                              sku[4] if pid_train.get(int(sku[0])) is None else
                                              0.9 * sum([adjust_vector[id] / len(pid_train[int(sku[0])])
                                                         for id in pid_train[int(sku[0])]]) + 0.1 * sku[4]
                                          }
                            for sku in skus_vector}
        item_vector_dict = {int(item[0]): {'name': item[1], 'alias_all_bid': item[2], 'pid': int(item[3]),
                                           'category': item[4], 'vector': item[5]}
                            for item in test_vector}
        ss = sku_similarity(skus_vector_dict)
        ss.init_annoy_model(join(self.env_dir, 'result/'))
        ss.load_annoy_model(join(self.env_dir, 'result/'))

        f = open(join(self.env_dir, 'result/result.txt'), 'w', encoding='utf-8', newline='')
        for id, item in item_vector_dict.items():
            result = ss.most_similarity(item['vector'])
            result = {pid: score for pid, score in result.items()
                      # if item['alias_all_bid'] in skus_vector_dict[pid]['alias_all_bid'] and
                      # item['category'] == skus_vector_dict[pid]['category']}
                      }
            result = {pid: score for i, (pid, score) in enumerate(result.items()) if i < 10}
            output_line = '{id:<10}\t{name:{chr}<30}\t{category:{chr}<10}\t{pid:<10}\n'.format(
                id=id, name=item['name'], pid=item['pid'], category=item['category'], chr=chr(12288))
            f.write(output_line)
            print(output_line, end='')

            for pid, similarity in result.items():
                sku = skus_vector_dict[pid]
                output_line = '\t{id:<7}\t{name:{chr}<30}\t{category:{chr}<10}\t{similarity}\n'.format(
                    id=pid, name=sku['name'], category=sku['category'], similarity=round(similarity, 3), chr=chr(12288))
                print(output_line, end='')
                f.write(output_line)
            f.write('\n')
            print()

            result_id = list(result.keys())
            if len(result_id) > 0 and int(item['pid']) == result_id[0]:
                correct += 1
            if len(result_id) >= 3 and int(item['pid']) in result_id[:3]:
                correct3 += 1
            if len(result_id) >= 5 and int(item['pid']) in result_id[:5]:
                correct5 += 1
            if len(result_id) == 10 and int(item['pid']) in result_id:
                correct10 += 1
            total += 1
            # input()
        print(correct / total)
        print(correct3 / total)
        print(correct5 / total)
        print(correct10 / total)
        f.write('{name:<10}:{acc}\n'.format(name='Accuracy', acc=str(round(correct / total, 5))))
        f.write('{name:<10}:{acc}\n'.format(name='Top 3', acc=str(round(correct3 / total, 5))))
        f.write('{name:<10}:{acc}\n'.format(name='Top 5', acc=str(round(correct5 / total, 5))))
        f.write('{name:<10}:{acc}\n\n'.format(name='Top 10', acc=str(round(correct10 / total, 5))))
        f.close()

    def __del__(self):
        pass


class sku_similarity(object):
    def __init__(self, skus):
        self.skus = skus
        self.pid_index = {pid: i for i, pid in enumerate(self.skus.keys())}
        self.index_pid = {i: pid for i, pid in enumerate(self.skus.keys())}
        self.annoy_model = AnnoyIndex(768, 'angular')

    def init_annoy_model(self, path):
        for pid, sku_info in self.skus.items():
            self.annoy_model.add_item(self.pid_index[pid], sku_info['vector'])
        self.annoy_model.build(int(log2(len(self.skus))) - 1)
        self.annoy_model.save(join(path, 'test.ann'))

    def load_annoy_model(self, path):
        self.annoy_model.load(join(path, 'test.ann'))

    def most_similarity(self, vector, k=-1):
        index, scores = self.annoy_model.get_nns_by_vector(vector, k, include_distances=True)
        return {self.index_pid[i]: score for i, score in zip(index, scores)}
