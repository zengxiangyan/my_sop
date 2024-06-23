import numpy as np
from typing import List


class Statistics:
    top_items = dict()
    statistics = np.array([0, 0, 0]).astype(np.int64)


class WordConcept:
    def __init__(self, name: str, keywords, label, collocates: List[tuple] = []):
        self.subject = name
        self.keywords = set()
        self.label = set()
        self.merge(keywords, label)
        for collocate in collocates:
            for concept in collocate:
                if not isinstance(concept, WordConcept) and not isinstance(concept, str):
                    raise TypeError('wrong type for collocate...')
        self.collocates = collocates

        self.month = None
        self.appear = None
        self.layer_statistics = dict()

    def merge(self, keywords, label):
        if isinstance(label, str):
            self.label.add(label)
        elif isinstance(label, set):
            self.label |= label
        else:
            raise TypeError('Wrong type of label')

        if isinstance(keywords, str):
            self.keywords.add(keywords)
        elif isinstance(keywords, set):
            self.keywords |= keywords
        else:
            raise TypeError('Wrong type of keywords')

    def check_ip_brand(self):
        # 防止跨界联名影响
        if {'IP', '品牌词'} <= self.label:
            self.label.remove('IP')

    # def change_label(self, label):
    #     self.label = set()
    #     if isinstance(label, str):
    #         self.label.add(label)
    #     elif isinstance(label, set):
    #         self.label |= label
    #     else:
    #         raise TypeError('Wrong type of label')

    def __str__(self):
        output_collocates = ['×'.join([concept.subject if isinstance(concept, WordConcept) else concept
                                       for concept in collocate]) for collocate in self.collocates]
        return f"subject    : {self.subject}\n" \
               f"label      : {self.label}\n" \
               f"keywords   : {self.keywords}\n" \
               f"collocates : {output_collocates}\n"

    def __eq__(self, other):
        return self.subject == other.subject

    def __lt__(self, other):
        return self.subject < other.subject

    def __hash__(self):
        return hash(self.subject)
