from layers.layer import Layer
import sys
import re
import os
import numpy as np
import pandas as pd
import hashlib
from datetime import datetime
import string
import shutil
import csv


class FileOnlineOutputLayer(Layer):
    def __init__(self, log_messages,  results: dict, filename: str, templates: list, message_headers: list):
        self.log_messages = log_messages
        self.filename = filename
        self.results = results
        self.templates = templates
        self.message_headers = message_headers

    def output_csv(self, filename, messages, headers):
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            for key, row in messages.items():
                writer.writerow(row)


    def outputResult(self):
        # import pdb; pdb.set_trace()
        log_events = dict()     
        eids = dict()
        for idx, val in self.results.items():
            temp = ' '.join(self.templates[val])
            if temp not in eids:
                eids[temp] = hashlib.md5(temp.encode('utf-8')).hexdigest()[0:8]
            self.log_messages[idx]['EventTemplate'] = temp

            self.log_messages[idx]['EventTemplate'] = temp
            self.log_messages[idx]['EventId'] = eids[temp]
        tot = 0
        for temp, eid in eids.items():
            log_events[tot] = dict(EventId=eid, EventTemplate=temp)
            tot += 1

            

        self.message_headers += ['EventId', 'EventTemplate']
        event_headers = ['EventId', 'EventTemplate']
        # df_event = pd.DataFrame(df_event, columns=['EventId', 'EventTemplate', 'Occurrences'])
        # import pdb; pdb.set_trace()

        # if self.keep_para:
        #     self.df_log["ParameterList"] = self.df_log.apply(self.get_parameter_list, axis=1) 
        self.output_csv(self.filename+'_structured.csv', self.log_messages, self.message_headers)
        self.output_csv(self.filename+'_templates.csv', log_events, event_headers)

    def run(self):
        dirname = os.path.dirname(self.filename)
        # if os.path.exists(dirname):
        #     shutil.rmtree(dirname)
        # os.makedirs(dirname)
        self.outputResult()
        # with open(self.filename, 'w') as file:
        #     file.writelines('发现了%s种模式' % len(self.sim_hash_dict.keys()))
        #     file.writelines('\n')
        #     file.writelines('\n')

        #     for key in self.sim_hash_dict.keys():
        #         file.writelines(
        #             "[本组数据量]:" + str(len(self.sim_hash_dict[key])) +
        #             '\n[模板]:' + self.sim_hash_dict[key][0][self.key])
        #         # "\n[抽样]:" + self.sim_hash_dict[key][len(self.sim_hash_dict[key]) - 1]['message'])
        #         file.writelines('\n')
        #         file.writelines('\n')
