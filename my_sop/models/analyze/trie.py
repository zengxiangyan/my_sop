class Trie:
    """
    字典树
    """

    def __init__(self, lower=True):
        self.root = {}
        self.end = -1
        self.lower = lower

    def insert(self, word, label=1):
        """
        向字典树添加实体

        :param label:
        :param word:
        :return:
        """
        if self.lower:
            word = word.lower()
        curNode = self.root
        for c in word:
            if not c in curNode:
                curNode[c] = {}
            curNode = curNode[c]
        curNode[self.end] = label

    def search(self, word):
        if self.lower:
            word = word.lower()
        curNode = self.root
        for c in word:
            if c not in curNode:
                return False
            curNode = curNode[c]
        if self.end not in curNode:
            return False
        return curNode[self.end]

    def startsWith(self, prefix):
        curNode = self.root
        for c in prefix:
            if c not in curNode:
                return False
            curNode = curNode[c]
        return True

    def search_entity(self, text):
        """
        正向最大实体搜索

        :param text: 一段文本
        :return: 文本中实体列表
        """
        if self.lower:
            text = text.lower()
        entitys = []
        i = 0
        while i < len(text):
            e = i + 1
            param = self.startsWith(text[i:e])
            if param:
                en = text[i:e]
                while e <= len(text):
                    p = self.startsWith(text[i:e])
                    if p:
                        en = text[i:e]
                        e += 1
                    else:
                        break
                label = self.search(en)
                if label:
                    entitys.append((en, i, i + len(en), label))
                    i = e - 1
                    # i+=1
                else:
                    i += 1
            else:
                i += 1
        return entitys

    def search_entity_(self, text, search_all=False):
        """
        实体搜索

        :param text: 一段文本
        :return: 文本中实体列表
        """
        if self.lower:
            text = text.lower()
        entitys = []
        length = len(text)
        i = 0
        while i < length:
            curNode = self.root
            j = i
            match = -1  # 记录最长匹配实体的末端位置
            while j < length:
                if text[j] not in curNode:  # 如果在字典树中找不到，则跳出
                    break
                curNode = curNode[text[j]]
                if self.end in curNode:  # 如果找到一个实体，则记录位置
                    match = j
                j += 1
            if match != -1:  # 如果找到了实体，则添加到结果中
                entitys.append((text[i:match + 1], i, match + 1, self.search(text[i:match + 1])))
            i += 1
        # 去除包含的情况，但是允许部分重合的情况
        if entitys and not search_all:
            res = []
            current_interval = entitys[0]
            for i in range(1, len(entitys)):
                # 如果当前时间段的结束时间小于等于前一个时间段的结束时间，则跳过
                if entitys[i][2] <= entitys[i - 1][2]:
                    continue
                # 如果当前时间段的开始时间与前一个时间段相同，但结束时间更晚，更新当前时间段
                elif entitys[i][1] == entitys[i - 1][1]:
                    if entitys[i][2] > entitys[i - 1][2]:
                        current_interval = entitys[i]
                # 否则，将当前时间段添加到结果中，并更新当前时间段为下一个
                else:
                    res.append(current_interval)
                    current_interval = entitys[i]
            # 添加最后一个时间段
            if not res or res[-1] != current_interval:
                res.append(current_interval)
            return res
        return entitys

    def all_entity(self):
        _entities = []

        def dfs(layer: dict, word: str):
            for w in layer:
                if w == -1:
                    _entities.append(word)
                else:
                    dfs(layer[w], word + w)
        dfs(self.root, '')
        return _entities


if __name__ == '__main__':
    entities = ['nasa', '猫和老鼠', '可乐', '哆啦a梦', 'nasa联名']
    name_list = ['雾霾蓝t恤男士nasa联名短袖2020年新款男潮牌潮流ins宽松纯棉半袖',
                 '【猫和老鼠联名】妖精的口袋亮丝短袖t恤女2020夏季韩版宽松上衣',
                 'nasa短袖男潮牌 潮流可乐联名夏季半袖纯棉百搭宽松涂鸦宇航员t恤']

    # 建树
    ip_trie = Trie()

    # 添加实体
    for ip in entities:
        ip_trie.insert(ip.lower())

    # 全部实体返回
    print(ip_trie.all_entity())

    # 实体正向最大匹配
    print(f'实体：{entities}\n')
    for name in name_list:
        results = ip_trie.search_entity(name.lower())
        print(name, results, sep='\n')
        results = ip_trie.search_entity_(name.lower())
        print(name, results, sep="\n")
