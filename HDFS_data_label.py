from tqdm import tqdm
import csv
import pickle
import re
import random
import wordninja
from datetime import datetime, timedelta

merge_map = dict()
def output_logs(filename, logs):
    with open(filename, 'w') as f:
        for blk_logs in tqdm(logs, desc='output {}'.format(filename)):
            for log in blk_logs:
                f.write('{} '.format(log))
            f.write('\n')
label_filename = '../anomaly_label.csv'
faults = 'ijlog.csv'
raw_filename = '../logs/HDFS_full/HDFS_full.log'
structured_filename = '../LogParserResult/HDFS_full/HDFS_full.log_structured.csv'
template_filename = '../LogParserResult/HDFS_full/HDFS_full.log_templates.csv'
anomaly_tags = dict()


def customShuffle(block_logs, block_ti, train_len):
    train_idices = random.sample(range(0, len(block_logs)), train_len)
    all_idxs = [i for i in range(0, len(block_logs))]
    test_idices = list(set(all_idxs) - set(train_idices))
    train_logs = [block_logs[i] for i in train_idices]
    train_ti = [block_ti[i] for i in train_idices]
    test_logs = [block_logs[i] for i in test_idices]
    test_ti = [block_ti[i] for i in test_idices]
    return train_logs, train_ti, test_logs, test_ti

test_block = [i for i in range(5000)]
test_ti = [i for i in range(5000)]
with open(label_filename, 'r') as f:
    reader = csv.DictReader(f)
    for row in tqdm(reader, 'load label'):
        tag = True if row['Label'] == 'Anomaly' else False
        anomaly_tags[row['BlockId']] = tag

noids = []
line_block = dict()
with open(raw_filename, 'r') as f:
    idx = 0
    for line in tqdm(f.readlines(), 'load raw'):
        idx += 1
        res = re.search(r'blk_-?\d+_?\d+', line)
        #assert(res != None)
        if (res == None):
            print(line)
            noids.append(line)
            continue
        blk_id = res.group(0)
        line_block[idx] = blk_id

block_logs = dict()

event_tot = 0
idmap = dict()
AnomalyTags = dict()
block_time = dict()
block_ti = dict()
#import pdb; pdb.set_trace()
with open(structured_filename, 'r') as f:
    reader = csv.DictReader(f)
    line_id = 0
    for row in tqdm(reader, 'load structured'):
        line_id += 1
        event_id = row['EventId']
        Date = row['Date']
        Time = row['Time']
        rt = datetime.strptime(Date + ' ' + Time, '%y%m%d %H%M%S')
        if line_id not in line_block: continue
        block_id = line_block[line_id]
        if event_id not in idmap:
            event_tot += 1
            idmap[event_id] = event_tot
        if block_id not in block_logs:
            block_ti[block_id] = []
            block_logs[block_id] = []
            block_time[block_id] = [rt]
        else:

            interval = rt - block_time[block_id][-1]
            block_ti[block_id].append(interval.seconds)
            block_time[block_id].append(rt)
        block_logs[block_id].append(idmap[event_id])
temp_words = dict()
with open(template_filename, 'r') as f:
    reader = csv.DictReader(f)
    for row in tqdm(reader, 'load template'):
        event_id = row['EventId']
        if event_id not in idmap: continue
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
performance_block_logs = []

normal_block_ti = []
abnormal_block_ti = []
performance_block_ti = []
def performance_injection(blk_id):
    ti = block_ti[blk_id]
    i = 0
    cnt = 5
    insert_idx = dict()
    while i < cnt:
        idx = random.randint(0, len(ti) - 1)
        if idx in insert_idx: continue
        if ti[idx] >= 0 and ti[idx] < 2:
           ti[idx] +=  3
        else: continue
        insert_idx[idx] = True
        i += 1
    block_ti[blk_id] = ti

for blk_id, is_anomaly in anomaly_tags.items():
    if is_anomaly:
        abnormal_block_logs.append(block_logs[blk_id])
        abnormal_block_ti.append(block_ti[blk_id])
    else:
        if random.random() < 0.05:
            performance_injection(blk_id)
            performance_block_logs.append(block_logs[blk_id])
            performance_block_ti.append(block_ti[blk_id])
        else:
            normal_block_logs.append(block_logs[blk_id])
            normal_block_ti.append(block_ti[blk_id])



train_logs_len = 6000
pickle.dump(temp_words, open('template2words.pkl', 'wb'))

normal_train_logs, normal_train_ti, normal_test_logs, normal_test_ti = customShuffle(normal_block_logs, normal_block_ti, train_logs_len)
abnormal_train_logs, abnormal_train_ti, abnormal_test_logs, abnormal_test_ti = customShuffle(abnormal_block_logs, abnormal_block_ti, train_logs_len)
perf_train_logs, perf_train_ti, perf_test_logs, perf_test_ti = customShuffle(performance_block_logs, performance_block_ti, train_logs_len)

output_logs('my_hdfs_train_normal', normal_train_logs)
output_logs('my_hdfs_test_normal', normal_test_logs)
output_logs('my_hdfs_train_abnormal', abnormal_train_logs)
output_logs('my_hdfs_test_abnormal', abnormal_test_logs)
output_logs('my_hdfs_train_perf', perf_train_logs)
output_logs('my_hdfs_test_perf', perf_test_logs)

output_logs('my_hdfs_train_normal_time', normal_train_ti)
output_logs('my_hdfs_test_normal_time', normal_test_ti)
output_logs('my_hdfs_train_abnormal_time', abnormal_train_ti)
output_logs('my_hdfs_test_abnormal_time', abnormal_test_ti)
output_logs('my_hdfs_train_perf_time', perf_train_ti)
output_logs('my_hdfs_test_perf_time', perf_test_ti)

print('total events: {}'.format(event_tot))
