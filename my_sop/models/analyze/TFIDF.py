from math import log
from collections import Counter


def is_chinese(word):
    for ch in word:
        if '\u4e00' <= ch <= '\u9fff':
            return True
    return False


class TFIDF:
    def __init__(self, corpus):
        self.corpus = corpus
        self.corpus_count = {parag_id: Counter() for parag_id in corpus}
        self.statistic()

    def statistic(self):
        for parag_id in self.corpus:
            for sentence in self.corpus[parag_id]:
                self.corpus_count[parag_id] += Counter([token for token in sentence
                                                        if len(token) > 1 and is_chinese(token)])

    def tfidf(self, token, parag_id):
        tf = self.corpus_count[parag_id][token] / sum(self.corpus_count[parag_id].values())
        idf = log(len(self.corpus_count) / (sum([1 if c.get(token) else 0 for c in self.corpus_count.values()]) + 1))
        return tf * idf

    def topK_tfidf(self, parag_id, topK=10, threshold=0):
        token_tfidf = dict()
        for token in self.corpus_count[parag_id]:
            token_tfidf[token] = self.tfidf(token, parag_id)

        sorted_word_tfidf = [(w, s) for w, s in sorted(token_tfidf.items(), key=lambda t: -t[1])[:int(topK)]
                             if s > threshold]
        if len(sorted_word_tfidf) > 0:
            word, score = zip(*sorted_word_tfidf)
            return word, score
        else:
            return (), ()


if __name__ == '__main__':
    # Suppose we have the following corpus
    corpus = {
        "parag1": [["我", "喜欢", "读书"], ["我", "喜欢", "吃", "苹果"]],
        "parag2": [["他", "不", "喜欢", "吃", "苹果"], ["他", "喜欢", "篮球"]],
        "parag3": [["我们", "都", "喜欢", "篮球"], ["我们", "也", "喜欢", "吃", "苹果"]],
        "parag4": [["我们", "都", "喜欢", "吃"], ["我们", "也", "喜欢", "读书"]]
    }

    # Create a TFIDF object
    tfidf_calculator = TFIDF(corpus)

    tfidf_score = tfidf_calculator.tfidf("篮球", "parag2")
    print(f'TF-IDF of "篮球" in "parag2": {tfidf_score}')
    tfidf_score = tfidf_calculator.tfidf("篮球", "parag3")
    print(f'TF-IDF of "篮球" in "parag3": {tfidf_score}')

    # If you want to find the top 2 tokens with the highest tf-idf scores in paragraph "parag2"
    top_tokens, top_scores = tfidf_calculator.topK_tfidf("parag2", topK=2)
    print(f'Top 2 tokens in "parag2": {top_tokens}')
    print(f'Top 2 scores in "parag2": {top_scores}')
