# 词/近义词等基类
from typing import Dict
import json


class Word:
    """词与alias类，可扩充data语义段"""
    to_lower = True
    mode = 1

    def __init__(self, subject, type='UNK', alias=None, subject_id='NIL', OF=None, cpnid=None):
        """
        初始化词类

        :param subject: 主体名
        :param type: 主体的类型 支持['UNK', 'BRD', 'CAT', 'PRV']四种
        :param alias: 主体别名
        :param subject_id: 主体id（gid或NIL）
        :param OF: 操作标志位
        """
        if OF is None:
            OF = []
        self._id = subject_id
        self._subject = subject.lower() if self.to_lower else subject
        self._operation = OF

        if type[:3] in ['UNK', 'BRD', 'CAT', 'PRV']:
            self._type = type
            # 如果有节点, 需要为prop节点提供categorypropname节点为了方便修改
            if isinstance(self._id, int) and self._type == 'PRV':
                if cpnid is None:
                    raise ValueError('Need cpnid for Prop node')
                else:
                    self.cpnid = [cpnid]
        else:
            raise ValueError(f'Wrong type input: {type}')

        self._alias = {self._subject}
        if isinstance(alias, list) or isinstance(alias, set):
            self._alias = self._alias | {word.lower() if self.to_lower else word for word in alias if word}
        elif isinstance(alias, str):
            if alias:
                self._alias.add(alias.lower() if self.to_lower else alias)
        elif alias is None:
            pass
        else:
            raise TypeError('alias has a wrong type')

    @property
    def subject_id(self):
        return self._id

    @property
    def subject(self) -> str:
        return self._subject

    @subject.setter
    def subject(self, _subject):
        self._subject = _subject

    @property
    def alias(self) -> set:
        return self._alias

    @alias.setter
    def alias(self, _alias):
        self._alias = _alias

    @property
    def type(self) -> str:
        return self._type

    @property
    def operation(self):
        return self._operation

    @operation.setter
    def operation(self, OF):
        self._operation = OF

    @property
    def alias_to_subject(self) -> Dict[str, str]:
        return {w: self._subject for w in self._alias if w != self._subject}

    @property
    def alias_to_type(self) -> Dict[str, str]:
        return {w: self._type for w in self._alias}

    def add_alias(self, word_list):
        """
        添加别名

        :param word_list:
        :return:
        """
        if isinstance(word_list, list) or isinstance(word_list, set):
            self._alias = self._alias | {word.lower() if self.to_lower else word for word in word_list if word}
        elif isinstance(word_list, str):
            if word_list:
                self._alias.add(word_list.lower() if self.to_lower else word_list)
        else:
            raise TypeError('add alias has a wrong type')

    def to_json(self) -> str:
        """
        单行输出

        :return:
        """
        return json.dumps({'subject_id': self._id,
                           'alias': list(self._alias),
                           'subject': self._subject,
                           'type': self._type,
                           'OF': self._operation},
                          ensure_ascii=False)

    def __str__(self):
        return f'   id   : {self._id}\n' \
               f'subject : {self._subject}\n' \
               f' alias  : {" ".join(self._alias)}\n' \
               f'  type  : {self._type}\n' \
               f'=========================================\n'

    def __repr__(self):
        return self._subject


def build_words_from_json_file(filename):
    """
    从json构建word到index的映射, List[Word <class>]

    :param filename:
    :return:
    """
    f = open(filename, 'r', encoding='utf-8')
    data = f.readlines()
    f.close()

    words = [Word(**json.loads(line.strip())) for line in data]
    word2index = dict()
    for index, word in enumerate(words):
        for w in word.alias:
            word2index[w] = word2index.get(w, []) + [index]

    return word2index, words


def output_words_to_json_file(filename, word_list):
    f = open(filename, 'w', encoding='utf-8')
    for word in word_list:
        f.write(word.to_json() + '\n')
    f.close()