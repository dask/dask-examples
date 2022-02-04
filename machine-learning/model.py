# more details can be found here: https://github.com/bentrevett/pytorch-sentiment-analysis/blob/master/4%20-%20Convolutional%20Sentiment%20Analysis.ipynb
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.nn.utils.rnn import pad_sequence
import torchtext
import numpy as np


class CNN(nn.Module):
    def __init__(self, n_filters=100, filter_sizes=(2,3,4), output_dim=2, dropout=0.2, pretrained_embeddings=None, TEXT=None):
        
        super().__init__()
        self.TEXT = TEXT
        # will be used to initialize model embeddings layer
        self.embedding = nn.Embedding.from_pretrained(pretrained_embeddings)
        self.embedding.weight.requires_grad = False # save some computation
        embedding_dim = self.embedding.embedding_dim
        self.conv_0 = nn.Conv1d(in_channels = 1, 
                                out_channels = n_filters, 
                                kernel_size = (filter_sizes[0], embedding_dim))
        self.conv_1 = nn.Conv1d(in_channels = 1, 
                                out_channels = n_filters, 
                                kernel_size = (filter_sizes[1], embedding_dim))
        self.conv_2 = nn.Conv1d(in_channels = 1, 
                                out_channels = n_filters, 
                                kernel_size = (filter_sizes[2], embedding_dim))
        self.fc = nn.Linear(len(filter_sizes) * n_filters, 2)
        self.dropout = nn.Dropout(dropout)
        
    def forward(self, text):
#         # bit of a hack to preprocess data inside the network
#         if isinstance(text, np.ndarray):
#             text = self.TEXT.process(text)
        
        #text = [batch size, sent len]
        embedded = self.embedding(text)
        #embedded = [batch size, sent len, emb dim]
        embedded = embedded.unsqueeze(1)
        #embedded = [batch size, 1, sent len, emb dim]
        conved_0 = F.relu(self.conv_0(embedded).squeeze(3))
        conved_1 = F.relu(self.conv_1(embedded).squeeze(3))
        conved_2 = F.relu(self.conv_2(embedded).squeeze(3))
        #conved_n = [batch size, n_filters, sent len - filter_sizes[n] + 1]
        pooled_0 = F.max_pool1d(conved_0, conved_0.shape[2]).squeeze(2)
        pooled_1 = F.max_pool1d(conved_1, conved_1.shape[2]).squeeze(2)
        pooled_2 = F.max_pool1d(conved_2, conved_2.shape[2]).squeeze(2)
        #pooled_n = [batch size, n_filters]
        cat = self.dropout(torch.cat((pooled_0, pooled_1, pooled_2), dim = 1))
        #cat = [batch size, n_filters * len(filter_sizes)]
        logits = self.fc(cat)
        #logits = [batch_size, output_dim]
        return F.softmax(logits, dim=-1)