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


class FileOutputLayer(Layer):
    def __init__(self, log_messages,  sim_hash_dict: dict, filename: str, templates: list, message_headers: list):
        self.log_messages = log_messages
        self.filename = filename
        self.sim_hash_dict = sim_hash_dict
        self.templates = templates
        self.message_headers = message_headers

    def output_csv(self, filename, messages, headers):
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            for key, row in messages.items():
                writer.writerow(row)


    def outputResult(self):
        log_events = dict()
        event_tot = 0
        for key in self.templates.keys():
            # template_str = ' '.join(logclust.logTemplate)
            eid = hashlib.md5(key.encode('utf-8')).hexdigest()[0:8]
            for logid in self.templates[key]:
                self.log_messages[logid]['EventTemplate'] = key
                self.log_messages[logid]['EventId'] = eid
            log_events[event_tot] = dict(EventId=eid, EventTemplate=key, Occurrences=len(self.templates[key]))
            event_tot += 1
        self.message_headers += ['EventId', 'EventTemplate']
        event_headers = ['EventId', 'EventTemplate', 'Occurrences']
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
