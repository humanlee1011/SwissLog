import re

from tqdm import tqdm
import pandas as pd
from layers.layer import Layer
import pickle
import re
class TokenizeOnlineGroupLayer(Layer):

    def __init__(self, rex=[], debug=False):
        # self.log_messages = log_messages
        self.rex = rex
        self.debug = debug
        # if dictionary_file:
        #     f = open(dictionary_file, 'rb')
        #     self.dictionary = pickle.load(f)
        #     f.close()
        # else:
        #     self.dictionary = None

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

    def tokenize_space(self, log_entry):
        '''
            Split string using space
        '''
        lst = []

        # for key, value in tqdm(self.log_messages.items(), desc='tokenization'):
            # words = value.split(' ')
        doc = self.preprocess(log_entry['Content'])
        words = self.splitbychars(doc, ',;:"= ')
        log_entry['Content'] = words
        return log_entry


    def run(self, log_entry):
        ''' 
        几种方法:
            1. 用空格去tokenize
            2. 使用冒号和分号
        '''
        if self.debug:
            print("Starting tokenization...\n")
        results = self.tokenize_space(log_entry)
        if self.debug:
            print('Tokenization layer finished.')
        return results
