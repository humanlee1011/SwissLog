import re

from tqdm import tqdm
import pandas as pd
from layers.layer import Layer
import pickle
import re
class TokenizeGroupLayer(Layer):

    def __init__(self, log_messages, rex=[], dictionary_file=None):
        self.log_messages = log_messages
        self.rex = rex
        if dictionary_file:
            f = open(dictionary_file, 'rb')
            self.dictionary = pickle.load(f)
            f.close()
        else:
            self.dictionary = None

    def preprocess(self, line):
        for currentRex in self.rex:
            line = re.sub(currentRex, ' <*> ', line)
        return line

    def splitbychars(self, s, chars):
        l = 0
        tokens = []
        for r in range(len(s)):
            if s[r] in chars:
                tokens.append(s[l:r])
                tokens.append(s[r])
                l=r+1
        tokens.append(s[l:])
        tokens = list(filter(None, [token.strip() for token in tokens]))
        for i in range(len(tokens)):
            if all(char.isdigit() for char in tokens[i]):
                tokens[i] = '<*>'
        return tokens

    def tokenize_space(self):
        '''
            Split string using space
        '''
        lst = []
        for key, value in tqdm(self.log_messages.items(), desc='tokenazation'):
            # words = value.split(' ')
            doc = self.preprocess(value['Content'])
            # doc = re.sub('[,;:"=]', ' ', doc)
            words = self.splitbychars(doc, ',;:"= ')
            self.log_messages[key]['Content'] = words
        return self.log_messages




    def run(self) -> list:

        ''' 几种方法:
            1. 用空格去tokenize
            2. 使用冒号和分号
        '''
        results = self.tokenize_space()
        print('Tokenization layer finished.')
        return results
