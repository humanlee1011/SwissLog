from tqdm import tqdm
import csv
import pickle
import re
import random
import wordninja

def output_logs(filename, logs):
    with open(filename, 'w') as f:
        for blk_logs in tqdm(logs, desc='output {}'.format(filename)):
            for log in blk_logs:
                f.write('{} '.format(log))
            f.write('\n')
label_filename = 'anomaly_label.csv'
raw_filename = '../../logs/HDFS_full/HDFS_full.log'
structured_filename = '../../LogParserResult/HDFS_full/HDFS_full.log_structured.csv'
template_filename = '../../LogParserResult/HDFS_full/HDFS_full.log_templates.csv'
anomaly_tags = dict()

with open(label_filename, 'r') as f:
    reader = csv.DictReader(f)
    for row in tqdm(reader, 'load label'):
        tag = True if row['Label'] == 'Anomaly' else False
        anomaly_tags[row['BlockId']] = tag

line_block = [None]
with open(raw_filename, 'r') as f:
    for line in tqdm(f.readlines(), 'load raw'):
        res = re.search(r'blk_-?\d+', line)
        assert(res != None)
        blk_id = res.group(0)
        line_block.append(blk_id)

block_logs = dict()
event_tot = 0
idmap = dict()
with open(structured_filename, 'r') as f:
    reader = csv.DictReader(f)
    line_id = 0
    for row in tqdm(reader, 'load structured'):
        line_id += 1
        event_id = row['EventId']
        block_id = line_block[line_id]
        if event_id not in idmap:
            event_tot += 1
            idmap[event_id] = event_tot
            #if event_tot == 34:
            #    assert(False)
        if block_id not in block_logs:
            block_logs[block_id] = []
        block_logs[block_id].append(idmap[event_id])
temp_words = dict()
with open(template_filename, 'r') as f:
    reader = csv.DictReader(f)
    for row in tqdm(reader, 'load template'):
        event_id = row['EventId']
        event_id = idmap[event_id]
        temp = row['EventTemplate']
        new_temp = ''
        for ch in temp:
            if ch.isalpha():
                new_temp += ch
            else:
                new_temp += ' '
        word_list = new_temp.lower().split()
        temp_words[event_id] = []
        for word in word_list:
            temp_words[event_id].extend(wordninja.split(word))

normal_block_logs = []
abnormal_block_logs = []
for blk_id, is_anomaly in anomaly_tags.items():
    if is_anomaly:
        abnormal_block_logs.append(block_logs[blk_id])
    else:
        normal_block_logs.append(block_logs[blk_id])
random.shuffle(normal_block_logs)
train_logs_len = 6000
#train_logs_len = len(normal_block_logs)//50
train_normal_logs = normal_block_logs[:train_logs_len]
train_abnormal_logs = abnormal_block_logs[:train_logs_len]
normal_block_logs = normal_block_logs[train_logs_len:]

pickle.dump(temp_words, open('data/LogInsight_backup/template2words.pkl', 'wb'))
output_logs('data/LogInsight_backup/hdfs/my_hdfs_test_normal', normal_block_logs[train_logs_len:])
output_logs('data/LogInsight_backup/hdfs/my_hdfs_test_abnormal', abnormal_block_logs[train_logs_len:])
output_logs('data/LogInsight_backup/hdfs/my_hdfs_train_normal', normal_block_logs[:train_logs_len])
output_logs('data/LogInsight_backup/hdfs/my_hdfs_train_abnormal', abnormal_block_logs[:train_logs_len])
print('total events: {}'.format(event_tot))
