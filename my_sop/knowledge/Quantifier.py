import re

class Quantifier:
    keys_list = ['g','段','罐','盒','克','kg','个','ml','袋']
    quantifier_ref = {
        # 'kg': {'n': 'g', 'v': 1000},
        '克': {'n': 'g', 'v': 1}
    }
    quantifier_reverse_ref = dict()
    used_keys_list = None
    pattern = None
    props = dict()

    #自动将转换量词反转
    def __init__(self):
        r = self.quantifier_reverse_ref
        for k in self.quantifier_ref:
            v = self.quantifier_ref[k]
            kk = v['n']
            if kk in r:
                h_sub = r[kk]
            else:
                h_sub = []
                r[kk] = h_sub
            h_sub.append(k)
        print(r)

    #获取量词匹配模式
    def get_pattern(self, l):
        self.used_keys_list = l
        l_after = []
        l_quantifier = []
        for i in range(len(l)):
            v = l[i]
            for k in v:
                l_after.append(k)
                if i == 2:
                    l_quantifier.append(k)
                if not k in self.quantifier_reverse_ref:
                    continue
                for kk in self.quantifier_reverse_ref[k]:
                    l_after.append(kk)
        # str = r'([\d-]+)(%s)([\|[\+|\*]*(?![\d-]+(?:-|%s))[\d-]*)' %('|'.join(l_after), '|'.join(l_after))
        str = r'\*?(\d[\d\.-]*)(%s)((\*(?!\d+(?:-|%s))\d*)|[\+|\*]?)' %('|'.join(l_after), '|'.join(l_after))
        print(str)
        self.pattern = re.compile(str, re.I)
        return self.pattern

    #匹配项处理
    def process_match(self, matched):
        print('matched:', matched.group(1), matched.group(2), matched.group(3))
        k = matched.group(1)
        v = matched.group(2)
        add = matched.group(3)
        quantifier_ref = self.quantifier_ref
        if v in quantifier_ref:
            vv = quantifier_ref[v]
            name = vv['n']
            value = vv['v']
        else:
            name = v
            value = 1

        props = self.props
        for i in range(len(self.used_keys_list)):
            if i in props:
                h_sub = props[i]
            else:
                h_sub = dict()
                props[i] = h_sub

            if not name in self.used_keys_list[i]:
                continue

            if name in h_sub:
                l = h_sub[name]
            else:
                l = []
                h_sub[name] = l
            if value == 1:
                l.append(k)
            else:
                l.append(int(k) * value)
        if add and add.find('*') >= 0:
            mm = re.search(r'(\d+)', add, re.I)
            if mm:
                value = mm.group(1)
                h_sub = props[2]
                name = ''
                if name in h_sub:
                    l = h_sub[name]
                else:
                    l = []
                    h_sub[name] = l
                l.append(value)

    # print('process_name_prop:', h)
        return ''

    def match_and_remove(self, s, l = None):
        self.props = {0:{}, 1:{}, 2:{}}
        if s is None or s == '':
            return [s, self.props]
        if l:
            self.get_pattern(l)
        pattern = self.pattern
        s = re.sub(pattern, self.process_match, s)
        s = s.strip()
        return [s, self.props]

