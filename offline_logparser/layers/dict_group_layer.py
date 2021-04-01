from tqdm import tqdm
from layers.layer import Layer
import pickle
from tqdm import tqdm
import wordninja

def hasDigit(inputString):
    return any(char.isdigit() for char in inputString)

def tolerant(source_dwords, target_dwords):
    rmt = target_dwords.copy()
    if len(source_dwords)<4: return False
    if len(target_dwords)<4: return False
    rms = set()
    for word in source_dwords:
        if word in target_dwords:
            rmt.remove(word)
        else:
            rms.add(word)
    return len(rmt)<=1 and len(rms) <=1


class DictGroupLayer(Layer):
    def __init__(self, log_messages, dictionary_file=None):
        self.log_messages = log_messages
        self.dictionary = None
        if dictionary_file:
            with open(dictionary_file, 'rb') as f:
                self.dictionary = pickle.load(f)

    def dictionaried(self):
        result = list()
        for key, value in tqdm(self.log_messages.items(), desc='dictionaried'):
            # dictionary_words = set()
            dictionary_list = list()
            for word in value['Content']:
                if hasDigit(word):
                    continue
                word = word.strip('.:*')
                if word in self.dictionary:
                    # dictionary_words.add(word)
                    dictionary_list.append(word)
                elif all(char.isalpha() for char in word):
                    splitted_words = wordninja.split(word)
                    for sword in splitted_words:
                        if len(sword) <= 2: continue
                        # dictionary_words.add(sword)
                        dictionary_list.append(sword)
            result_dict = dict(message=value['Content'], dwords=dictionary_list, LineId=value['LineId'])
            result.append(result_dict)
        return result

    def run(self) -> dict:
        dicted_list = self.dictionaried()
        dwords_group = dict()
        for element in tqdm(dicted_list, desc='group by dictionary words'):
            # frozen_dwords = frozenset(element['dwords'])
            frozen_dwords = tuple(sorted(element['dwords']))
            if frozen_dwords not in dwords_group:
                dwords_group[frozen_dwords] = []
            dwords_group[frozen_dwords].append(element)
        tot = 0
        result_group = dict()
        diffrent_length = 0 
        # for key in dwords_group.keys():
        #     if len(key) == 0:
        #         for entry in dwords_group[key]:
        #             result_group[tot] = [entry]
        #             tot += 1
        #         continue
        #     result_group[tot] = dwords_group[key]
        #     len_set = set()
            # for element in result_group[tot]:
            #     len_set.add(len(element['message']))
            # diffrent_length += len(len_set)
            # tot += 1
        # print('if split by length, total: {}'.format(diffrent_length))
        print('After Dictionary Group, total: {} bin(s)'.format(len(result_group.keys())))
        # return result_group
        return dwords_group
