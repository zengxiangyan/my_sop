from bert_base.client import BertClient
import sys
from os.path import abspath, join, dirname

sys.path.insert(0, join(abspath(dirname('__file__')), '../../'))
from models.nerl import *


class NERParser:
    bc = None

    def __init__(self):
        ...

    def connect_bert_server(self, ip='10.21.200.128', port=5555, port_out=5556, timeout=2000):
        self.bc = BertClient(ip=ip, port=port, port_out=port_out, timeout=timeout,
                             show_server_config=False, check_version=False, check_length=False, mode='NER')

    def get_ner_result(self, text):
        processed_text = encode_string(text)
        string_list = [list(processed_text)]
        # try:
        rst = self.bc.encode(string_list, is_tokenized=True)
        print(rst)
        # except:
        #     return [{}]
        answers = [match_answer(string, result) for string, result in zip([list(text)], rst)]
        return answers


if __name__ == '__main__':
    ner = NERParser()
    ner.connect_bert_server()
    print(ner.get_ner_result("蔡文静同款推荐小圆饼干野村植物油粗粮脆饼进口130g"))
