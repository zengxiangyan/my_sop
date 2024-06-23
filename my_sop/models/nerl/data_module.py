import re
import jieba

import application as app
from extensions import utils
from gremlin_python.process.traversal import P
from gremlin_python.process.traversal import T
from gremlin_python.process.graph_traversal import __
from models.nerl.KGEntity_module import BrandEntity

brackets_signal = "「」（〔［｛《【〖〈([{<）〕］｝》】〗〉)]{>"


class Mention:
    def __init__(self):
        self.token = None  # token
        self.offset = -1  # offset of the sub string in the sentence
        self.candidates = set()  # Set[gid]
        # self.candidates = dict()  # Dict[gid, confidence]
        # self.results = []  # [(gid, confidence)], mentioned entities, order by confidence desc

    def __repr__(self):
        return str(self.token) + ' ' + str(self.offset)


class AbstractData:
    def __init__(self, trie):
        self.en_Trie = trie

    def cut(self, text):
        en_pattern = '(?:^|[^0-9a-z]){}(?=[^0-9a-z]|$)'

        def single_process(word, offset):
            m = Mention()
            m.token = word
            m.offset = offset
            mentions.append(m)

        mentions = []

        tokenize_result = jieba.tokenize(text.lower())
        # segment = jieba.lcut(text.lower(), cut_all=True)
        en_segment = []
        for word, st, ed in tokenize_result:
            if utils.is_only_chinese(word):
                # 中文
                single_process(word, st)
            else:
                # 英文
                if len(en_segment) > 0 and len(en_segment[-1][0]) + en_segment[-1][1] == st and en_segment[-1][0] not \
                        in brackets_signal:
                    # 与之前的英文字段相连
                    en_segment[-1][0] += word
                elif word != ' ':
                    # 避免词段首为空格
                    if len(en_segment) > 0:
                        # 避免词段末为空格
                        en_segment[-1][0] = en_segment[-1][0].rstrip()
                    en_segment.append([word, st])

        def process_escapes(context):
            new_context = ''
            char = r'*.?+$^[](){}|\/'
            for ch in context:
                if ch in char:
                    new_context += "\\" + ch
                else:
                    new_context += ch
            return new_context

        # 处理英文分词
        for seg, offset in en_segment:
            res = self.en_Trie.search_entity(seg)
            rel_p = 0
            while res:
                en = res.pop(0)

                # 复查，是否链接出来的和其他字母相连，即非完整字段
                if not re.search(en_pattern.format(process_escapes(en[0])), seg):
                    continue
                if rel_p != en[1]:
                    single_process(seg[rel_p:en[1]].rstrip(), offset + rel_p)
                single_process(en[0], offset + en[1])
                rel_p = en[2]
            if rel_p < len(seg):
                single_process(seg[rel_p:], offset + rel_p)
        return sorted(mentions, key=lambda m: m.offset)

    def condition_cut(self, text, ner_result):
        i = 0
        all_mentions = []
        # ner_result_list = [(ner, offset, label) for (ner, offset), label in sorted(ner_result.items(),
        #                                                                            key=lambda i:i[0][1])]
        for (ner, offset), label in sorted(ner_result.items(), key=lambda i: i[0][1]):
            if offset != 0:
                part_mention = self.cut(text[i:offset])
                for mention in part_mention:
                    mention.offset += i
                all_mentions += part_mention

            m = Mention()
            m.token = ner
            m.offset = offset
            all_mentions.append(m)
            i = offset + len(ner)
        if i < len(text):
            part_mention = self.cut(text[i:])
            for mention in part_mention:
                mention.offset += i
            all_mentions += part_mention
        return all_mentions


class TextData(AbstractData):
    def __init__(self, text, trie):
        super().__init__(trie)
        self.text = text
        self.mentions = self.cut(text)

    @property
    def all_tokens(self):
        return [m.token for m in self.mentions]

    def print_mention(self, entities, other_result, gid2probability):
        print('[Text]', self.text)
        for mention in self.mentions:
            print('\t\t', mention.token, ' -- ', mention.offset)
            for gid in sorted(mention.candidates, key=lambda g: -gid2probability.get(g, 0)):
                print('\t\t\t', entities[gid], ' -> ', round(gid2probability.get(gid, 0), 3))

            if other_result.get((mention.token, mention.offset)) is not None:
                print('\t\t\t', BrandEntity(-1, -1, mention.token), ' -> ', 'NER')
            # for gid, c in sorted(candidate2confidence.items(), key=lambda i: -i[1]):
            #     print('\t\t\t', entities[gid], ' -> ', c)

    @staticmethod
    def print_relation(entities, relationship):
        print('[Relation]')
        for g1, g2 in relationship:
            print('\t\t', entities[g1], ' --- ', entities[g2])

    def return_mention(self, entities, other_result, gid2probability, top=False):
        ret = []
        for mention in self.mentions:
            mention_ret = {'word': mention.token, 'start': mention.offset, 'end': mention.offset + len(mention.token),
                           'entity': []}
            for gid in sorted(mention.candidates, key=lambda g: -gid2probability.get(g, 0)):
                mention_ret['entity'].append({'name': entities[gid].name,
                                              'gid': gid,
                                              'id': entities[gid].tid,
                                              'type': entities[gid].label,
                                              'confidence': round(gid2probability.get(gid, 0), 3)})
                if top:
                    mention_ret['entity'] = mention_ret['entity'][0]
                    break

            if len(mention.candidates) == 0 and other_result.get((mention.token, mention.offset)) is not None:
                mention_ret['entity'].append({'name': mention.token,
                                              'gid': -1,
                                              'id': -1,
                                              'type': other_result[(mention.token, mention.offset)],
                                              'confidence': -1})
            # if isinstance(mention_ret['entity'], list):
            #     mention_ret['entity'] = dict()
            ret.append(mention_ret)
        return ret

    # # @used_time
    # def cal_confidences_by_relation(self, entities, relations):
    #     all_gids = []
    #     for mention in self.mentions:
    #         if mention.candidates:
    #             all_gids.append(tuple(mention.candidates))
    #
    #     gid2confidence, gid2confidence_r = dict(), dict()
    #     already_cal_relation = set()
    #
    #     for i in range(len(all_gids)):
    #         for gid in all_gids[i]:
    #             gid2confidence[gid] = gid2confidence.get(gid, 0) + 1
    #
    #     def get_alpha(g1, g2):
    #         _alpha = 0
    #         for g in [g1, g2]:
    #             if entities[g].label in ['brand', 'category']:
    #                 _alpha += 0.6
    #         return _alpha if _alpha < 1.0 else 1.0
    #
    #     def check_gid_relation(g1, g2):
    #         if (g1, g2) in relations and (g1, g2) not in already_cal_relation:
    #             already_cal_relation.add((g1, g2))
    #             alpha = get_alpha(g1, g2)
    #             gid2confidence_r[g1] = gid2confidence_r.get(g1, 0) + alpha * gid2confidence[g2]
    #             gid2confidence_r[g2] = gid2confidence_r.get(g2, 0) + alpha * gid2confidence[g1]
    #
    #     for i in range(len(all_gids) - 1):
    #         for j in range(i + 1, len(all_gids)):
    #             for gid1 in all_gids[i]:
    #                 for gid2 in all_gids[j]:
    #                     check_gid_relation(gid1, gid2)
    #                     check_gid_relation(gid2, gid1)
    #
    #     gid2confidence = {gid: (gid2confidence[gid] + gid2confidence_r.get(gid, 0)
    #                             if entities[gid].label in ['brand', 'category'] else gid2confidence_r.get(gid, 0))
    #                       for gid in gid2confidence}
    #     return gid2confidence, already_cal_relation
    #
    # def cal_confidences_by_graph(self):
    #     all_gids = []
    #     for mention in self.mentions:
    #         if mention.candidates:
    #             all_gids.append(tuple(mention.candidates))
    #
    #     relation_score, appearance_score = dict(), dict()
    #
    #     for i in range(len(all_gids) - 1):
    #         for j in range(i + 1, len(all_gids)):
    #             # i组和j组 计算概率 同组不重复计算
    #             for _i in all_gids[i]:
    #                 for _j in all_gids[j]:
    #                     if _i == _j:
    #                         relation_score[_i] = relation_score.get(_i, 1) + 1
    #                     else:
    #                         rank = self.g.V(_i).repeat(
    #                             __.both('categoryBrand', 'categoryPropNameR', 'categoryPropValue',
    #                                     'categoryParent').simplePath().timeLimit(2000)).until(
    #                             __.hasId(_j).or_().loops().is_(P.gte(6))).path().limit(1).toList()
    #                         if rank:
    #                             relation_score[tuple(sorted([_i, _j]))] = 1 / len(rank[0])
    #
    #     gid2confidence = dict()
    #     for (_i, _j), r in relation_score.items():
    #         gid2confidence[_i] = gid2confidence.get(_i, 0) + \
    #                              r * appearance_score.get(_i, 1) * appearance_score.get(_j, 1)
    #         gid2confidence[_j] = gid2confidence.get(_j, 0) + \
    #                              r * appearance_score.get(_i, 1) * appearance_score.get(_j, 1)
    #
    #     return gid2confidence

    def get_all_candidate(self):
        all_candidate = []
        for mention in self.mentions:
            if mention.candidates:
                all_candidate.append(tuple(mention.candidates))
        return all_candidate
