# -*- coding: utf-8 -*-
from layers.file_output_layer import FileOutputLayer
from layers.knowledge_layer import KnowledgeGroupLayer
# from layers.leven_layer import LevenLayer
from layers.mask_layer import MaskLayer
# from layers.minhash_reduce_layer import MinHashReduceLayer
# from layers.simhash_group_layer import SimHashGroupLayer
from layers.tokenize_group_layer import TokenizeGroupLayer
from layers.dict_group_layer import DictGroupLayer
#!/usr/bin/env python

import sys
# sys.path.append('../')
from evaluator import evaluator
import os
import re
import string
import hashlib
from datetime import datetime
from tqdm import tqdm

input_dir = 'logs/' # The input directory of log file
output_dir = 'LogParserResult/' # The output directory of parsing results


def load_logs(log_file, regex, headers):
    """ Function to transform log file to dataframe
    """
    log_messages = dict()
    linecount = 0
    with open(log_file, 'r') as fin:
        for line in tqdm(fin.readlines(), desc='load data'):
            try:
                linecount += 1
                # if linecount >3000000:
                #     break
                match = regex.search(line.strip())
                message = dict()
                for header in headers:
                    message[header] = match.group(header)
                message['LineId'] = linecount
                log_messages[linecount] = message
            except Exception as e:
                pass
    return log_messages

def generate_logformat_regex(logformat):
    """ Function to generate regular expression to split log messages
    """
    headers = []
    splitters = re.split(r'(<[^<>]+>)', logformat)
    regex = ''
    for k in range(len(splitters)):
        if k % 2 == 0:
            splitter = re.sub(' +', '\\\s+', splitters[k])
            regex += splitter
        else:
            header = splitters[k].strip('<').strip('>')
            regex += '(?P<%s>.*?)' % header
            headers.append(header)
    regex = re.compile('^' + regex + '$')
    return headers, regex

def get_parameter_list(row):
    template_regex = re.sub(r"\s<.{1,5}>\s", "<*>", row["EventTemplate"])
    if "<*>" not in template_regex: return []
    template_regex = re.sub(r'([^A-Za-z0-9])', r'\\\1', template_regex)
    template_regex = re.sub(r'\\ +', r'[^A-Za-z0-9]+', template_regex)
    template_regex = "^" + template_regex.replace("\<\*\>", "(.*?)") + "$"
    parameter_list = re.findall(template_regex, row["Content"])
    parameter_list = parameter_list[0] if parameter_list else ()
    parameter_list = list(parameter_list) if isinstance(parameter_list, tuple) else [parameter_list]
    parameter_list = [para.strip(string.punctuation).strip(' ') for para in parameter_list]
    return parameter_list

def gen_checkpoint(log_messages, results, templates):
    import pickle
    pickle.dump(log_messages, open('log_messages.pkl', 'wb'))
    pickle.dump(results, open('results.pkl', 'wb'))
    pickle.dump(templates, open('templates.pkl', 'wb'))

def load_checkpoint():
    import pickle
    log_message = pickle.load(open('log_messages.pkl', 'rb'))
    results = pickle.load(open('results.pkl', 'rb'))
    templates = pickle.load(open('templates.pkl', 'rb'))
    return log_messages, results, templates

benchmark_settings = {
    # 'Kubelet': {
    #     'log_file': 'Kubelet/kubelet.log',
    #     'log_format': '<Month> <Day> <Time> <Cluster> <KubeId>: <Level> <AccurateTime> <KubeIdd> <CodeLine> <Content>',
    #     'regex': [r'(\d+\.){3}\d+(:\d+)?'],
    #     },
    # 'KubeController': {
    #     'log_file': 'KubeController/kubecontroller.log',
    #     'log_format': '<Month> <Day> <Time> <Cluster> <KubeId>: <Level> <AccurateTime> <KubeIdd> <CodeLine> <Content>',
    #     'regex': [r'(\d+\.){3}\d+(:\d+)?'],
    #     },
    #'BGL_1000': {
    #    'log_file': 'BGL_sampled/BGL_1000.log',
    #    'log_format': '<Label> <Timestamp> <Date> <Node> <Time> <NodeRepeat> <Type> <Component> <Level> <Content>',
    #    'regex': [r'core\.\d+', r'[0-9a-fA-f]{8}'],
    #    },
    'offline': {
         'log_file': 'HDFS_full/HDFS_sampled.log',
         'log_format': '<Date> <Time> <Level> <Component>: <Content>',
          'regex': [r'blk_-?\d+', r'(\d+\.){3}\d+(:\d+)?'],
        }
    #  'cpu': {
    #      'log_file': 'cpu.log',
    #      'log_format': '<Date> <Time> <Level> <Component>: <Content>',
    #       'regex': [r'blk_-?\d+', r'(\d+\.){3}\d+(:\d+)?'],
    #     },
    # 'drophb': {
    #      'log_file': 'drophb.log',
    #      'log_format': '<Date> <Time> <Level> <Component>: <Content>',
    #       'regex': [r'blk_-?\d+', r'(\d+\.){3}\d+(:\d+)?'],
    #     },
    # 'blkrcvdelay': {
    #      'log_file': 'blkrcvdelay.log',
    #      'log_format': '<Date> <Time> <Level> <Component>: <Content>',
    #       'regex': [r'blk_-?\d+', r'(\d+\.){3}\d+(:\d+)?'],
    #     },
    # 'blksnddelay': {
    #      'log_file': 'blksnddelay.log',
    #      'log_format': '<Date> <Time> <Level> <Component>: <Content>',
    #       'regex': [r'blk_-?\d+', r'(\d+\.){3}\d+(:\d+)?'],
    #     },
    # 'pktsnddelay': {
    #      'log_file': 'pktsnddelay.log',
    #      'log_format': '<Date> <Time> <Level> <Component>: <Content>',
    #       'regex': [r'blk_-?\d+', r'(\d+\.){3}\d+(:\d+)?'],
    #     },
    # 'diskrd': {
    #      'log_file': 'diskrd.log',
    #      'log_format': '<Date> <Time> <Level> <Component>: <Content>',
    #       'regex': [r'blk_-?\d+', r'(\d+\.){3}\d+(:\d+)?'],
    #     },
    # 'diskwr': {
    #      'log_file': 'diskwr.log',
    #      'log_format': '<Date> <Time> <Level> <Component>: <Content>',
    #       'regex': [r'blk_-?\d+', r'(\d+\.){3}\d+(:\d+)?'],
    #     },
    # 'netdelay': {
    #      'log_file': 'netdelay.log',
    #      'log_format': '<Date> <Time> <Level> <Component>: <Content>',
    #       'regex': [r'blk_-?\d+', r'(\d+\.){3}\d+(:\d+)?'],
    #     },
    # 'netdrop': {
    #      'log_file': 'netdrop.log',
    #      'log_format': '<Date> <Time> <Level> <Component>: <Content>',
    #       'regex': [r'blk_-?\d+', r'(\d+\.){3}\d+(:\d+)?'],
    #     },
    # 'jvmcfl': {
    #      'log_file': 'jvmcfl.log',
    #      'log_format': '<Date> <Time> <Level> <Component>: <Content>',
    #       'regex': [r'blk_-?\d+', r'(\d+\.){3}\d+(:\d+)?'],
    #     },
    # 'blkcrpt': {
    #      'log_file': 'blkcrpt.log',
    #      'log_format': '<Date> <Time> <Level> <Component>: <Content>',
    #       'regex': [r'blk_-?\d+', r'(\d+\.){3}\d+(:\d+)?'],
    #     },
}

# load_checkpoint()

bechmark_result = []
for dataset, setting in benchmark_settings.items():
    # if dataset is not 'Windows':
    #   continue
    print('\n=== Evaluation on %s ==='%dataset)
    indir = os.path.join(input_dir, os.path.dirname(setting['log_file']))
    outdir = os.path.join(output_dir, os.path.dirname(setting['log_file']))
    log_file = os.path.basename(setting['log_file'])

    # print(log_file)
    filepath = os.path.join(indir, log_file)
    print('Parsing file: ' + filepath)
    starttime = datetime.now()
    headers, regex = generate_logformat_regex(setting['log_format'])
    log_messages = load_logs(filepath, regex, headers)
    # tokenize log Content
    log_messages = KnowledgeGroupLayer(log_messages).run()
    log_messages = TokenizeGroupLayer(log_messages, rex=setting['regex']).run()
    dict_group_result = DictGroupLayer(log_messages, 'EngCorpus.pkl').run()
    results, templates = MaskLayer(dict_group_result).run()
    output_file = os.path.join(outdir, log_file)
    # gen_checkpoint(log_messages, results, templates)
    FileOutputLayer(log_messages, results, output_file, templates, ['LineId'] + headers).run()
    print('Parsing done. [Time taken: {!s}]'.format(datetime.now() - starttime))


