from tqdm import tqdm
import csv
import pickle
import re
import random
import wordninja
import os
import datetime
merge_map = dict()

def output_logs(filename, logs):
    with open(filename, 'w') as f:
        for blk_logs in tqdm(logs, desc='output {}'.format(filename)):
            for log in blk_logs:
                f.write('{} '.format(log))
            f.write('\n')

# os.path.append('../')
label_filename = 'anomaly_label.csv'
raw_filename = '../logs/HDFS_full/HDFS_full.log'
structured_filename = '../logs/HDFS_full/HDFS_full.raw_structured.csv'
template_filename = '../LogParserResult/HDFS_full/HDFS_full.log_templates.csv'
output_file = '../logs/HDFS_full/HDFS_time_interval_data.pkl'
pd = 100
anomaly_tags = dict()

    

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
        time = str(row['Time'])
        date = str(row['Date'])
        d = '20' + date + ':' + time
        t = datetime.datetime.strptime(d,  '%Y%m%d:%H%M%S')
        block_id = line_block[line_id]
        if block_id not in block_logs:
            block_logs[block_id] = []
        block_logs[block_id].append(t)

interval_data = dict()
import pdb; pdb.set_trace()
for blk_id, seq in block_logs.items():
#    interval_data[blk_id] = []
#    for idx, line in enumerate(seq):
 #       if idx == 0:
  #          lt =  line
   #         pass
        # use second as a unit
    delta = (seq[-1] - seq[0]).seconds
    if blk_id not in interval_data:
        interval_data[blk_id] = []
    interval_data[blk_id].append(delta)

#        lt = line
import pdb; pdb.set_trace()
pickle.dump(interval_data, open(output_file, 'wb'))

