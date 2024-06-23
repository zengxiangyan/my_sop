import tensorflow as tf
from tensorflow.python.framework import graph_util
import argparse


def freeze_graph(ckpt, output_graph):
    output_node_names = 'bert/encoder/layer_7/output/dense/kernel'
    # saver = tf.train.import_meta_graph(ckpt+'.meta', clear_devices=True)
    saver = tf.compat.v1.train.import_meta_graph(ckpt + '.meta', clear_devices=True)
    graph = tf.get_default_graph()
    input_graph_def = graph.as_graph_def()

    with tf.Session() as sess:
        saver.restore(sess, ckpt)
        output_graph_def = graph_util.convert_variables_to_constants(
            sess=sess,
            input_graph_def=input_graph_def,
            output_node_names=output_node_names.split(',')
        )
        with tf.gfile.GFile(output_graph, 'wb') as fw:
            fw.write(output_graph_def.SerializeToString())
        print('{} ops in the final graph.'.format(len(output_graph_def.node)))


# ckpt = r'D:\DataCleaner\src\output\model\ckpt_model\model.ckpt-1355'
# pb = r'D:\DataCleaner\src\output\model\pb_model\ner_model.pb'

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='frozening weights.')

    # 超参
    parser.add_argument('--ckpt', type=str, required=True, help='input checkpoint name')
    parser.add_argument('--pb', type=str, required=True, help='output pb filename')

    args = parser.parse_args()
    freeze_graph(args.ckpt, args.pb)