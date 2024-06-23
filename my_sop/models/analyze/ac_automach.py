# -*- coding: utf-8 -*-
import ahocorasick

class ACAutomaton:
    """
    AC自动机
    """
    def __init__(self, lower=True):
        self.lower = lower
        self.A = ahocorasick.Automaton()

    def insert(self, word, label='test'):
        """
        向AC自动机添加实体
        """
        if self.lower:
            word = word.lower()
        self.A.add_word(word, (word,len(word),label))

    def make(self):
        self.A.make_automaton()

    def search_entity(self, text, if_long=False):
        """
        实体搜索
        :param text: 一段文本
        :return: 文本中实体列表
        """
        if self.lower:
            text = text.lower()
        res = []
        try:
            if not if_long:
                entitys = list(self.A.iter(text))
            else:
                entitys = list(self.A.iter_long(text))
            for rr in entitys:
                res.append((rr[1][0], rr[0]-rr[1][1]+1, rr[0]+1, rr[1][-1]))
        except Exception as e:
            print(e)
        return res

    @staticmethod
    def entitiy_process(entitys):
        # 去除包含实体但是允许部分重叠的实体
        if not entitys:
            return []
        res = []  # 模拟一个栈
        for ii in entitys:
            if not res:
                res.append(ii)
                continue
            if ii[1] <= res[-1][1]:
                while res and res[-1][1] >= ii[1]:
                    res.pop(-1)
            elif ii[2] == res[-1][2] and ii[1] > res[-1][1]:
                continue
            res.append(ii)

        return res

if __name__ == '__main__':
    entities = ['碳酸钙', '碳酸', '钙', '黑碳', '美白','牙白','白牙','美白牙齿','美白配方','清晰','下颌线','清晰下颌线','兰','兰蔻']
    name_list = ['人气美白牙膏',
                 '成分是钙和碳酸钙超级美白哦美白牙齿',
                 '成分是黑碳酸钙',
                 '清晰下颌线','兰蔻小瓶和兰面膜']

    ac = ACAutomaton()
    # 添加实体
    for ww in entities:
        ac.insert(ww.lower(), label={1: 'test'})
    ac.make()

    # print(ac.entitiy_process([("l'oréal paris", 0, 13, {1: {44785}}), ("l'oréal paris professionnel", 0, 27, {-1: {44785}, 1: {51974}}), ('蔻萝兰', 28, 31, {1: {493473}}), ('兰', 30, 31, {2: {35445}}), ('elemis', 78, 84, {1: {245281}}), ('兰', 88, 89, {2: {35445}}), ('雅诗兰黛', 86, 90, {1: {52297}})]))

    for name in name_list:
        results = ac.search_entity(name.lower())
        resultss = ac.search_entity(name.lower(), if_long=True)
        results_p = ac.entitiy_process(results)
        # print(ac.entitiy_process([(995, 10, 12, 0), (1363791, 10, 12, 0)]))
        print(name, results, resultss, results_p, sep='\n')


