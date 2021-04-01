# -*- coding: utf-8 -*-
from layers.fileonline_output_layer import FileOnlineOutputLayer
from layers.knowledgeonline_layer import KnowledgeOnlineGroupLayer
from layers.maskonline_layer import MaskOnlineLayer
from layers.tokenizeonline_group_layer import TokenizeOnlineGroupLayer
from layers.dictonline_group_layer import DictOnlineGroupLayer

import sys
from evaluator import evaluator
import os
import re
import string
import hashlib
from datetime import datetime
from tqdm import tqdm
import argparse
import pickle

# input_dir = 'logs/HDFS_full' # The input directory of log file
input_dir = 'logs' # The input directory of log file
output_dir = 'LogParserResult' # The output directory of parsing results

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

# HDFS
ds_setting ={
    'HDFS': {
        # 'log_file': 'online_input.log',
        'log_file': 'HDFS_full/HDFS_full_10000.log',
        'log_format': '<Date> <Time> <Level> <Component>: <Content>',
        'regex': [r'blk_-\d+', r'blk_\d+', r'(\d+\.){3}\d+(:\d+)?'],
    },

# Android
    'Android': {
        'log_file': 'Android_full/Andriod_10000.log',
        'log_format': '<Date> <Time>  <Pid>  <Tid> <Level> <Component>: <Content>',
        'regex': [r'(/[\w-]+)+', r'([\w-]+\.){2,}[\w-]+', r'\b(\-?\+?\d+)\b|\b0[Xx][a-fA-F\d]+\b|\b[a-fA-F\d]{4,}\b'],
        'st': 0.2,
        'depth': 6
        },
# BGL
    'BGL': {
        'log_file': 'BGL_full/BGL_full.log',
        'log_format': '<Label> <Timestamp> <Date> <Node> <Time> <NodeRepeat> <Type> <Component> <Level> <Content>',
        'regex': [r'core\.\d+'],
        'st': 0.5,
        'depth': 4
        }
}

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--online', action='store_true')
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--dataset', default='HDFS', type=str)
    # parser.add_argument('--logfile', action_store=True)

    args = parser.parse_args()
    isOnline = args.online
    debug = args.debug
    dataset = args.dataset
    setting = ds_setting[dataset]


    print("------------------Online Parsing------------------------\n")
    # read file settings
    indir = os.path.join(input_dir, os.path.dirname(setting['log_file']))
    outdir = os.path.join(output_dir, os.path.dirname(setting['log_file']))
    log_file = os.path.basename(setting['log_file'])
    filepath = os.path.join(indir, log_file)
    print('Parsing file: ' + filepath)

    # load templates
    templates = pickle.load(open('templates.pkl', 'rb'))
    templates = dict()
    # load log format
    headers, regex = generate_logformat_regex(setting['log_format'])
    log_messages = load_logs(filepath, regex, headers)
    # log messages is a dictionary where the key is linecount, the item is {'LineId': , header: ''}
    results = dict()
    knowledge_layer = KnowledgeOnlineGroupLayer(debug)
    tokenize_layer = TokenizeOnlineGroupLayer(rex=setting['regex'], debug=debug)
    dict_layer = DictOnlineGroupLayer('EngCorpus.pkl', debug)
    mask_layer = MaskOnlineLayer(dict_layer, templates, results, debug)
    starttime = datetime.now()
    for lineid, log_entry in log_messages.items():
        if lineid in [1000, 10000, 100000, 1000000, 2000000, 3000000, 4000000, 5000000, 6000000, 7000000, 8000000, 9000000, 10000000]:
            # import pdb; pdb.set_trace()
            print('Parsing done. [Time taken: {!s}]'.format(datetime.now() - starttime))
        # print(log_entry['Content'])
        # go through four steps:
        # import pdb; pdb.set_trace()
        # preprocess
        log_entry = knowledge_layer.run(log_entry)

        # tokenize log content
        log_entry = tokenize_layer.run(log_entry)

        # look up dictionary, return a dict: {message: log_entry['Content'], dwords: wordset, LineId: }
        wordset = dict_layer.run(log_entry)

        # LCS with existing templates, merging in prefix Tree
        mask_layer.run(wordset, log_entry)

        # print('After online parsing, templates updated: {} \n\n\n'.format(templates))
    output_file = os.path.join(outdir, log_file)
    FileOnlineOutputLayer(log_messages, results, output_file, mask_layer.cluster2Template, ['LineId'] + headers).run()
    F1_measure, accuracy = evaluator.evaluate(
                           groundtruth=os.path.join(indir, log_file + '_structured.csv'),
                           parsedresult=os.path.join(outdir, log_file + '_structured.csv')
                           )


