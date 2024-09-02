import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
import torch
from transformers import BertTokenizer, BertModel, BertConfig
from os.path import join


class BertEmbedding:
    def __init__(self, model_path, VOCAB="vocab.txt"):
        self.model_path = model_path
        self.VOCAB = VOCAB
        self.tokenizer = BertTokenizer.from_pretrained(join(self.model_path, VOCAB))
        self.model = BertModel.from_pretrained(model_path, output_hidden_states=True)  # 如果想要获取到各个隐层值需要如此设置
        self.model.eval()

    def encode(self, string):
        # Convert token to vocabulary indices
        tokenized_string = self.tokenizer.tokenize(string)
        tokens_ids = self.tokenizer.convert_tokens_to_ids(tokenized_string)

        # Convert inputs to PyTorch tensors
        tokens_tensor = torch.tensor([tokens_ids])
        outputs = self.model(tokens_tensor)  # encoded_layers, pooled_output
        if self.model.config.output_hidden_states:
            hidden_states = outputs[2]
            # last_layer = outputs[-1]
            second_to_last_layer = hidden_states[-2]
            # 由于只要一个句子，所以尺寸为[1, 8, 768]
            token_vecs = second_to_last_layer[0]
            # print(token_vecs.shape)
            # Calculate the average of all input token vectors.
            sentence_embedding = torch.mean(token_vecs, dim=0)
            # print(sentence_embedding.shape)
            # print(sentence_embedding[0:10])
            return sentence_embedding
