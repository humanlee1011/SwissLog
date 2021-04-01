from difflib import SequenceMatcher

from tqdm import tqdm

from layers.layer import Layer
import os
import pickle

class TreeNode:
    def __init__(self, tag=-1):
        self.childs = dict() # token->treenode
        self.tag = tag # non -1 denotes the id of the cluster

class Trie:
    def __init__(self):
        self.root = TreeNode()

    def insert(self, template_list, cluster_id):
        now = self.root
        for token in template_list:
            if token not in now.childs:
                now.childs[token] = TreeNode()
            now = now.childs[token]
        now.tag = cluster_id

    def find(self, template_list):
        now = self.root
        wd_count = 0
        for token in template_list:
            if token in now.childs:
                now = now.childs[token]
            elif '<*>' in now.childs:
                wd_count += 1
                now = now.childs['<*>']
            else:
                return -1
        if template_list and wd_count/len(template_list) > 0.5:
            return -1
        return now.tag

def maskdel(template):
    temp = []
    for token in template:
        if token == '<*>':
            temp.append('')
        else:
            temp.append(token)
    return temp

class MaskLayer(Layer):
    def __init__(self, sim_hash_dict: dict, max_mask_loop: int = 0):
        self.sim_hash_dict = sim_hash_dict
        self.max_mask_loop = max_mask_loop
        # self.output_file = output_file

    def replace_char(self, str, char, index):
        string = list(str)
        string[index] = char
        return ''.join(string)

    def getTemplate(self, lcs, seq):
        retVal = []
        if not lcs:
            return retVal

        lcs = lcs[::-1]
        i = 0
        for token in seq:
            i += 1
            if token == lcs[-1]:
                retVal.append(token)
                lcs.pop()
            else:
                retVal.append('<*>')
            if not lcs:
                break
        while i < len(seq):
            retVal.append('<*>')
            i += 1
        return retVal

    def LCS(self, seq1, seq2):
        lengths = [[0 for j in range(len(seq2)+1)] for i in range(len(seq1)+1)]
        # row 0 and column 0 are initialized to 0 already
        for i in range(len(seq1)):
            for j in range(len(seq2)):
                if seq1[i] == seq2[j]:
                    lengths[i+1][j+1] = lengths[i][j] + 1
                else:
                    lengths[i+1][j+1] = max(lengths[i+1][j], lengths[i][j+1])

        # read the substring out from the matrix
        result = []
        lenOfSeq1, lenOfSeq2 = len(seq1), len(seq2)
        while lenOfSeq1!=0 and lenOfSeq2 != 0:
            if lengths[lenOfSeq1][lenOfSeq2] == lengths[lenOfSeq1-1][lenOfSeq2]:
                lenOfSeq1 -= 1
            elif lengths[lenOfSeq1][lenOfSeq2] == lengths[lenOfSeq1][lenOfSeq2-1]:
                lenOfSeq2 -= 1
            else:
                assert seq1[lenOfSeq1-1] == seq2[lenOfSeq2-1]
                result.append(seq1[lenOfSeq1-1])
                lenOfSeq1 -= 1
                lenOfSeq2 -= 1
        result = result[::-1]
        return result

    def mask_simple(self, seq1, seq2):
        res = []
        for i in range(len(seq1)):
            if seq1[i] == seq2[i]:
                res.append(seq1[i])
            else:
                res.append('<*>')
        return res

    def run(self):
        templates = dict()
        template_list = list()
        template_dict = dict()
        # import pdb; pdb.set_trace()
        for key in tqdm(self.sim_hash_dict, desc='templates initialization'):
            sorted_list = [[x['message'], x['LineId']] for x in self.sim_hash_dict[key]]
            template = sorted_list[0][0]
            for index, rs in enumerate(sorted_list):
                if index == 0:
                    continue
                temp_template = self.getTemplate(self.LCS(rs[0], template), template)
                # temp_template = self.mask_simple(rs[0], template)
                if temp_template != '':
                    template = temp_template
            template_list.append([key, template])
            template_dict[key] = template
        # import pdb; pdb.set_trace()
        template_list = sorted(template_list, key=lambda entry: maskdel(entry[1]))
        trie = Trie()
        tot = 0
        for entry in tqdm(template_list, desc='trie group'):
            tag = trie.find(entry[1])
            if tag == -1:
                trie.insert(entry[1], entry[0])
                tot += 1
            else:
                self.sim_hash_dict[tag].extend(self.sim_hash_dict[entry[0]])
                self.sim_hash_dict.pop(entry[0])
        # import pdb; pdb.set_trace()
        for key in tqdm(self.sim_hash_dict, desc='generate output'):
            sorted_list = [[x['message'], x['LineId']] for x in self.sim_hash_dict[key]]
            clustIDs = list()
            for index, rs in enumerate(sorted_list):
                clustIDs.append(rs[1])
            template = ' '.join(template_dict[key])
            # print(len(clustIDs))
            templates[template] = clustIDs
            # print(templates)
            for index, rs in enumerate(self.sim_hash_dict[key]):
                rs['template'] = template
        pickle.dump(template_dict, open('templates.pkl', 'wb'))
        print('After mask layer finish, tot, total: {} bin(s)'.format(len(self.sim_hash_dict)))
        return self.sim_hash_dict, templates
