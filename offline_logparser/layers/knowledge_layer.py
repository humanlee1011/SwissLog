import re

from tqdm import tqdm

from layers.layer import Layer


class KnowledgeGroupLayer(Layer):

    def __init__(self, log_messages):
        self.log_messages = log_messages

    def run(self) -> list:
        for key, log in tqdm(self.log_messages.items(), desc='priori knowledge preprocess'):
            # preprocess = list()
            # for value in log['Content']:
                # 2019-03-26 00:49:39
            value = log['Content']
            value = re.sub(
                r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', ' <*> ',
                value)

            # Mar 26 00:50:09
            value = re.sub(
                r'\w+ \d{2} \d{2}:\d{2}:\d{2}', ' <*> ',
                value)

            # 00:50:09.216340
            value = re.sub(
                r'\d{2}:\d{2}:\d{2}.\d{6}', ' <*> ',
                value)

            # 222.200.180.181:41406
            value = re.sub(
                r'\d+\.\d+\.\d+\.\d+:\d+', ' <*> ',
                value)

            # value = re.sub(
            #     r'\d+\.\d+\.\d+\.\d+', ' <*> ',
            #     value)

            # 英文的周数
            value = re.sub(
                r'(Mon|Tue|Wed|Thu|Fri|Sat|Sun)', ' <*> ',
                value)
            # 英文的月份
            value = re.sub(
                r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)', ' <*> ',
                value)
            self.log_messages[key]['Content'] = value
            # preprocess.append(value)
        # log['Content'] = preprocess
        print('Knowledge group layer finished.')
        return self.log_messages
