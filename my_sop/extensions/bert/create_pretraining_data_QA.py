from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import jieba, re
import collections
import random
import tokenization_wwm as tokenization
import tensorflow as tf

flags = tf.flags
FLAGS = flags.FLAGS

flags.DEFINE_string("keyword_file", None, "keywords.")
flags.DEFINE_string("input_file", None, "Input raw text file (or comma-separated list of files).")
flags.DEFINE_string("output_file", None, "Output TF example file (or comma-separated list of files).")
flags.DEFINE_string("vocab_file", None, "The vocabulary file that the BERT model was trained on.")
flags.DEFINE_bool("do_lower_case", True, "Whether to lower case the input text. Should be True for uncased "
                                         "models and False for cased models.")
flags.DEFINE_bool("do_whole_word_mask", True, "Whether to use whole word masking rather than per-WordPiece masking.")
flags.DEFINE_integer("max_seq_length", 128, "Maximum sequence length.")
flags.DEFINE_integer("max_predictions_per_seq", 38, "Maximum number of masked LM predictions per sequence.")
flags.DEFINE_integer("random_seed", 12345, "Random seed for data generation.")
flags.DEFINE_integer("dupe_factor", 15, "Number of times to duplicate the input data (with different masks).")
flags.DEFINE_float("masked_lm_prob", 0.3, "Masked LM probability.")
flags.DEFINE_float("short_seq_prob", 0.1,
                   "Probability of creating sequences which are shorter than the maximum length.")
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
    name = re.sub('[ ]+', ' ', ''.join(w if is_chinese(w) else ' ' for w in sentence).strip())
    seq_cws = jieba.lcut(name)
    new_segment = []
    j = 0
    w = ''
    for i in range(len(sentence)):
        if is_chinese(sentence[i]):
            if w:
                new_segment.append('##' + sentence[i])
            else:
                new_segment.append(sentence[i])
            w = w + sentence[i]
        elif sentence[i] != '[SPACE]':
            w = ''
            new_segment.append(sentence[i])
        if j < len(seq_cws) and w == seq_cws[j]:
            w = ''
            j += 1
            if j < len(seq_cws) and seq_cws[j] == ' ':
                j += 1
    return new_segment


def create_instances_from_document_sentence_pairs(
        all_documents, document_index, answer_list, tokenizer, max_seq_length, short_seq_prob,
        masked_lm_prob, max_predictions_per_seq, vocab_words, rng):
    document = all_documents[document_index]
    max_sequence_length_allowed = max_seq_length - 3

    instances = []
    for index, (question, answer) in enumerate(document):
        # 添加random还是答案
        if rng.random() < 0.8 and index > 0:
            is_random_next = True
            random_answer = None
            for _ in range(10):
                random_answer = rng.choice(answer_list)
                if answer != random_answer and random_answer is not None:
                    break
            output_answer = random_answer
        else:
            is_random_next = False
            output_answer = answer

        question_tokens = get_new_segment(tokenizer.tokenize(question[:int(max_sequence_length_allowed * 0.6)]))
        answer_tokens = get_new_segment(tokenizer.tokenize(output_answer[:int(max_sequence_length_allowed * 0.4)]))

        tokens = []
        segment_ids = []
        tokens.append("[CLS]")
        segment_ids.append(0)
        for token in question_tokens:
            tokens.append(token)
            segment_ids.append(0)
        tokens.append("[SEP]")
        segment_ids.append(0)

        for token in answer_tokens:
            tokens.append(token)
            segment_ids.append(1)
        tokens.append('[SEP]')
        segment_ids.append(1)
        
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


def create_training_instances(input_files, tokenizer, max_seq_length,
                              dupe_factor, short_seq_prob, masked_lm_prob,
                              max_predictions_per_seq, rng):
    all_documents = [[]]
    answers_set = set()
    for input_file in input_files:
        with tf.gfile.GFile(input_file, "r") as reader:
            while True:
                line = tokenization.convert_to_unicode(reader.readline())
                if not line:
                    all_documents.append([])
                    break
                line = line.strip()

                question, answer = line.split('\t')
                all_documents[-1].append((question, answer))
                answers_set.add(answer)

    all_documents = [x for x in all_documents if x]
    answers_list = list(answers_set)
    vocab_words = list(tokenizer.vocab.keys())
    instances = []
    # 每组生成不同mask
    for document_index in range(len(all_documents)):
        for _ in range(dupe_factor):
            instances.extend(
                create_instances_from_document_sentence_pairs(
                    all_documents, document_index, answers_list, tokenizer, max_seq_length, short_seq_prob,
                    masked_lm_prob, max_predictions_per_seq, vocab_words, rng))
            print(instances[-2:])

    rng.shuffle(instances)
    return instances


def main(_):
    tf.logging.set_verbosity(tf.logging.INFO)

    tokenizer = tokenization.FullTokenizer(vocab_file=FLAGS.vocab_file, do_lower_case=FLAGS.do_lower_case)

    input_files = []
    for input_pattern in FLAGS.input_file.split(","):
        input_files.extend(tf.gfile.Glob(input_pattern))

    tf.logging.info("*** Reading from input files ***")
    for input_file in input_files:
        tf.logging.info("  %s", input_file)

    rng = random.Random(FLAGS.random_seed)
    instances = create_training_instances(
        input_files, tokenizer, FLAGS.max_seq_length, FLAGS.dupe_factor,
        FLAGS.short_seq_prob, FLAGS.masked_lm_prob, FLAGS.max_predictions_per_seq,
        rng)

    output_files = FLAGS.output_file.split(",")
    tf.logging.info("*** Writing to output files ***")
    for output_file in output_files:
        tf.logging.info("  %s", output_file)

    write_instance_to_example_files(instances, tokenizer, FLAGS.max_seq_length,
                                    FLAGS.max_predictions_per_seq, output_files)


if __name__ == "__main__":
    flags.mark_flag_as_required("input_file")
    flags.mark_flag_as_required("output_file")
    flags.mark_flag_as_required("vocab_file")
    flags.mark_flag_as_required("keyword_file")
    import pandas as pd
    brand_keyword, category_keyword, prop_keyword = pd.read_pickle(FLAGS.keyword_file)
    for b in brand_keyword:
        jieba.add_word(b, freq=len(b) * 10000)
    for c in category_keyword:
        jieba.add_word(c, freq=len(c) * 10000)
    for p in prop_keyword:
        jieba.add_word(p, freq=len(p) * 10000)
    tf.app.run()
