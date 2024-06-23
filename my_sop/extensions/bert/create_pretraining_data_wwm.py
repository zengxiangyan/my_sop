#!/usr/bin/python3
# -*- coding: utf-8 -*-
import collections
import jieba
import os, sys
from os.path import abspath, join, dirname
import random
import re

import tensorflow as tf
import tokenization_wwm as tokenization
sys.path.insert(0, join(abspath(dirname('__file__')), '../../'))
import application as app

flags = tf.flags
FLAGS = flags.FLAGS
flags.DEFINE_string("keyword_dir", None, "Input directory of keyword files")
flags.DEFINE_string("input_dir", None, "Input directory of raw text files")
flags.DEFINE_string("output_dir", None, "Output directory for created tfrecord files")
flags.DEFINE_string("vocab_file", None, "The vocabulary file that the BERT model was trained on.")
flags.DEFINE_list("params", ['title', 'content'], "the params in dataset, like label sentences etc")
flags.DEFINE_bool("do_lower_case", True,
                  "Whether to lower case the input text. Should be True for uncased models and False for cased models.")
flags.DEFINE_bool("do_whole_word_mask", True, "Whether to do whole word mask")
flags.DEFINE_bool("ramdom_next", True,
                  "Whether to use next sentence loss, it's for fitting Roberta, if False, the training data will like "
                  "Bert or AlBert, if True, it will like Roberta training data")
flags.DEFINE_integer("max_seq_length", 128, "Maximum sequence length.")
flags.DEFINE_integer("max_predictions_per_seq", 20, "Maximum number of masked LM predictions per sequence.")
flags.DEFINE_integer("random_seed", 12345, "Random seed for data generation.")
flags.DEFINE_integer("dupe_factor", 5, "Number of times to duplicate the input data (with different masks).")
flags.DEFINE_float("masked_lm_prob", 0.15, "Masked LM probability.")
flags.DEFINE_integer("max_workers", 2, "max workers")
flags.DEFINE_float("short_seq_prob", 0.1,
                   "Probability of creating sequences which are shorter than the maximum length.")
flags.DEFINE_integer("doc_stride", 128,
                     "When splitting up a long document into chunks, how much stride to take between chunks.")
punc_pattern = re.compile(r"^[\s+\.\!\/_,$%^*\(\+\-\"\'\)\[\]]+|[+——()?【】“”！，。？、~@#￥%……&*（）]+$")


class TrainingInstance(object):
    """A single training instance (sentence pair)."""

    def __init__(self, tokens, segment_ids, masked_lm_positions, masked_lm_labels,
                 is_random_next):
        self.tokens = tokens
        self.segment_ids = segment_ids
        self.is_random_next = is_random_next
        self.masked_lm_positions = masked_lm_positions
        self.masked_lm_labels = masked_lm_labels

    def __str__(self):
        s = ""
        s += "tokens: %s\n" % (" ".join(
            [tokenization.printable_text(x) for x in self.tokens]))
        s += "segment_ids: %s\n" % (" ".join([str(x) for x in self.segment_ids]))
        s += "is_random_next: %s\n" % self.is_random_next
        s += "masked_lm_positions: %s\n" % (" ".join(
            [str(x) for x in self.masked_lm_positions]))
        s += "masked_lm_labels: %s\n" % (" ".join(
            [tokenization.printable_text(x) for x in self.masked_lm_labels]))
        s += "\n"
        return s

    def __repr__(self):
        return self.__str__()


def write_instance_to_example_files(instances, tokenizer, max_seq_length,
                                    max_predictions_per_seq, output_files):
    """Create TF example files from `TrainingInstance`s."""
    writers = []
    for output_file in output_files:
        writers.append(tf.python_io.TFRecordWriter(output_file))

    writer_index = 0

    total_written = 0
    for (inst_index, instance) in enumerate(instances):
        input_ids = tokenizer.convert_tokens_to_ids(instance.tokens)
        input_mask = [1] * len(input_ids)
        segment_ids = list(instance.segment_ids)
        assert len(input_ids) <= max_seq_length

        while len(input_ids) < max_seq_length:
            input_ids.append(0)
            input_mask.append(0)
            segment_ids.append(0)

        assert len(input_ids) == max_seq_length
        assert len(input_mask) == max_seq_length
        assert len(segment_ids) == max_seq_length

        masked_lm_positions = list(instance.masked_lm_positions)
        masked_lm_ids = tokenizer.convert_tokens_to_ids(instance.masked_lm_labels)
        masked_lm_weights = [1.0] * len(masked_lm_ids)

        while len(masked_lm_positions) < max_predictions_per_seq:
            masked_lm_positions.append(0)
            masked_lm_ids.append(0)
            masked_lm_weights.append(0.0)

        next_sentence_label = 1 if instance.is_random_next else 0

        features = collections.OrderedDict()
        features["input_ids"] = create_int_feature(input_ids)
        features["input_mask"] = create_int_feature(input_mask)
        features["segment_ids"] = create_int_feature(segment_ids)
        features["masked_lm_positions"] = create_int_feature(masked_lm_positions)
        features["masked_lm_ids"] = create_int_feature(masked_lm_ids)
        features["masked_lm_weights"] = create_float_feature(masked_lm_weights)
        features["next_sentence_labels"] = create_int_feature([next_sentence_label])

        tf_example = tf.train.Example(features=tf.train.Features(feature=features))

        writers[writer_index].write(tf_example.SerializeToString())
        writer_index = (writer_index + 1) % len(writers)

        total_written += 1

    for writer in writers:
        writer.close()

    tf.logging.info("Wrote %d total instances", total_written)


def create_int_feature(values):
    feature = tf.train.Feature(int64_list=tf.train.Int64List(value=list(values)))
    return feature


def create_float_feature(values):
    feature = tf.train.Feature(float_list=tf.train.FloatList(value=list(values)))
    return feature


MaskedLmInstance = collections.namedtuple("MaskedLmInstance",
                                          ["index", "label"])


def create_masked_lm_predictions(tokens, masked_lm_prob,
                                 max_predictions_per_seq, vocab_words, rng):
    """Creates the predictions for the masked LM objective."""
    cand_indexes = []
    for (i, token) in enumerate(tokens):
        if (token == "[CLS]" or token == "[SEP]" or re.match(punc_pattern, token)) and token != '[NUM]':
            continue

        if (FLAGS.do_whole_word_mask and len(cand_indexes) >= 1 and
                token.startswith("##")):
            cand_indexes[-1].append(i)
        else:
            cand_indexes.append([i])
    rng.shuffle(cand_indexes)
    output_tokens = [t[2:] if len(re.findall('##[\u4E00-\u9FA5]', t)) > 0 else t for t in tokens]

    num_to_predict = min(max_predictions_per_seq,
                         max(1, int(round(len(tokens) * masked_lm_prob))))

    masked_lms = []
    covered_indexes = set()
    for index_set in cand_indexes:
        if len(masked_lms) >= num_to_predict:
            break
        # If adding a whole-word mask would exceed the maximum number of
        # predictions, then just skip this candidate.
        if len(masked_lms) + len(index_set) > num_to_predict:
            continue
        is_any_index_covered = False
        for index in index_set:
            if index in covered_indexes:
                is_any_index_covered = True
                break
        if is_any_index_covered:
            continue
        for index in index_set:
            covered_indexes.add(index)

            # 80% of the time, replace with [MASK]
            if rng.random() < 0.8:
                masked_token = "[MASK]"
            else:
                # 10% of the time, keep original
                if rng.random() < 0.5:
                    masked_token = tokens[index][2:] if len(re.findall('##[\u4E00-\u9FA5]', tokens[index])) > 0 else \
                        tokens[index]
                # 10% of the time, replace with random word
                else:
                    masked_token = vocab_words[rng.randint(0, len(vocab_words) - 1)]

            output_tokens[index] = masked_token

            masked_lms.append(MaskedLmInstance(index=index, label=tokens[index]))
    assert len(masked_lms) <= num_to_predict
    masked_lms = sorted(masked_lms, key=lambda x: x.index)

    masked_lm_positions = []
    masked_lm_labels = []
    for p in masked_lms:
        masked_lm_positions.append(p.index)
        if len(re.findall('##[\u4E00-\u9FA5]', p.label)) > 0:
            masked_lm_labels.append(p.label[2:])
        else:
            masked_lm_labels.append(p.label)

    return output_tokens, masked_lm_positions, masked_lm_labels


def get_new_segment(sentence):  # 新增的方法 ####
    """
    输入一句话，返回一句经过处理的话: 为了支持中文全称mask，将被分开的词，将上特殊标记("#")，使得后续处理模块，能够知道哪些字是属于同一个词的。
    :param segment: 一句话
    :return: 一句处理过的话
    """

    def is_chinese(word):
        for ch in word:
            if '\u4e00' <= ch <= '\u9fff':
                return True
        return False

    # text = re.sub('##', '', ''.join(sentence))
    name = [a for a in sentence if is_chinese(a)]
    seq_cws = jieba.lcut(re.sub('##', '', ''.join(name)))

    i, j, w = 0, 0, ''
    new_segment = []
    while i < len(sentence) and j < len(seq_cws):
        if w and sentence[i][0] != '[':
            new_segment.append('##' + re.sub('##', '', sentence[i]))
        else:
            new_segment.append(sentence[i])
        if is_chinese(sentence[i]):
            w = w + re.sub('##', '', sentence[i])
        if w == seq_cws[j]:
            j += 1
            w = ''
        i = i + 1
    # print(new_segment)
    return new_segment


def create_instances_from_sentences(
        all_sentences, sentence_index, max_seq_length, short_seq_prob,
        masked_lm_prob, max_predictions_per_seq, vocab_words, rng):
    sentence = all_sentences[sentence_index]
    random_sentence = random.choice(all_sentences)
    max_sequence_length_allowed = max_seq_length - 3
    text_list = get_new_segment(sentence[:int(max_sequence_length_allowed * 0.6)])
    random_text_list = get_new_segment(random_sentence[:int(max_sequence_length_allowed * 0.4)])

    tokens = []
    segment_ids = []
    tokens.append("[CLS]")
    segment_ids.append(0)
    for token in text_list:
        tokens.append(token)
        segment_ids.append(0)
    tokens.append("[SEP]")
    segment_ids.append(0)

    for token in random_text_list:
        tokens.append(token)
        segment_ids.append(1)
    tokens.append('[SEP]')
    segment_ids.append(1)

    # 2、调用原有的方法
    instances = []
    is_random_next = True
    (tokens, masked_lm_positions,
     masked_lm_labels) = create_masked_lm_predictions(
        tokens, masked_lm_prob, max_predictions_per_seq, vocab_words, rng)
    instance = TrainingInstance(
        tokens=tokens,
        segment_ids=segment_ids,
        is_random_next=is_random_next,
        masked_lm_positions=masked_lm_positions,
        masked_lm_labels=masked_lm_labels)
    instances.append(instance)
    return instances


def create_training_instances(file, tokenizer, max_seq_length,
                              dupe_factor, short_seq_prob, masked_lm_prob,
                              max_predictions_per_seq, rng, params):
    all_sentences = []

    tf.logging.info('start convert %s to tokens' % file)
    f = open(file, 'r', encoding='utf-8')
    lines = [line for line in f]
    from tqdm import tqdm
    for line in tqdm(lines):
        content = line
        if not len(content) < 20:
            content = tokenization.convert_to_unicode(content.replace("<eop>", ""))
            tokens = tokenizer.tokenize(content)
            tokens = [token for token in tokens if token != '[UNK]']
            if len(tokens) == 0:
                continue

            all_sentences.append(tokens)

    tf.logging.info('end convert %s to tokens' % file)
    # with open('cut.txt', 'w', encoding='utf-8') as f:
    #     for s in all_sentences:
    #         f.write(str(s))
    # print('output word cut to cut.txt')
    vocab_words = list(tokenizer.vocab.keys())

    instances = []
    tf.logging.info('start convert %s tokens to instance' % file)
    for sentence_index in tqdm(range(len(all_sentences))):
        for _ in range(dupe_factor):
            instances.extend(
                create_instances_from_sentences(
                    all_sentences, sentence_index, max_seq_length, short_seq_prob,
                    masked_lm_prob, max_predictions_per_seq, vocab_words, rng))
    tf.logging.info('end convert %s tokens to instance' % file)
    rng.shuffle(instances)
    return instances


def main(_):
    tf.logging.set_verbosity(tf.logging.INFO)

    input_files = []
    for file in os.listdir(FLAGS.input_dir):
        input_files.append(os.path.join(FLAGS.input_dir, file))

    for file in input_files:
        tf.logging.info('input file %s' % file)

    tokenizer = tokenization.FullTokenizer(
        vocab_file=FLAGS.vocab_file, do_lower_case=FLAGS.do_lower_case)

    for file in input_files:
        tf.logging.info('start file %s' % file)
        rng = random.Random(FLAGS.random_seed)
        instances = create_training_instances(
            file, tokenizer, FLAGS.max_seq_length, FLAGS.dupe_factor,
            FLAGS.short_seq_prob, FLAGS.masked_lm_prob, FLAGS.max_predictions_per_seq,
            rng, params=FLAGS.params)
        print(instances[:10])
        tf.logging.info('start convert %s instance to tfrecords' % file)
        # paths = file.split('/')
        # output_files = [os.path.join(FLAGS.output_dir, '%s.tfrecord' % (paths[-1][:-4]))]
        output_files = FLAGS.output_dir.split(",")
        write_instance_to_example_files(instances, tokenizer, FLAGS.max_seq_length,
                                        FLAGS.max_predictions_per_seq, output_files)
        tf.logging.info('end convert %s instance to tfrecords' % file)

        del instances
    tf.logging.info('all were done')


if __name__ == "__main__":
    flags.mark_flag_as_required("input_dir")
    flags.mark_flag_as_required("output_dir")
    flags.mark_flag_as_required("vocab_file")
    flags.mark_flag_as_required("keyword_dir")
    import pandas as pd
    keywords, brands_keywords_set, categorys_keywords_set, props_keywords_set = \
        pd.read_pickle(FLAGS.keyword_dir)
    for b in brands_keywords_set:
        jieba.add_word(b, freq=len(b) * 10000)
    for c in categorys_keywords_set:
        jieba.add_word(c, freq=len(c) * 10000)
    for p in props_keywords_set:
        jieba.add_word(p, freq=len(p) * 10000)

    tf.app.run()
