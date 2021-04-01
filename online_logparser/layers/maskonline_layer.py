from difflib import SequenceMatcher

from tqdm import tqdm

from layers.layer import Layer
import os

class TreeNode:
    def __init__(self, value, tag=-1):
        self.childs = dict() # token->treenode
        self.tag = tag # non -1 denotes the id of the cluster
        self.value = value

class Trie:
    def __init__(self):
        self.root = TreeNode(-1)

    def insert(self, template_list, cluster_id):
        now = self.root
        for token in template_list:
            if token not in now.childs:
                now.childs[token] = TreeNode(token)
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
    

    def update(self, old_template, template):
        # TODO: how to split the treenode
        # delete
        # self.print()
        paths = list()
        now = self.root
        for token in old_template:
            if token in now.childs:
                if len(now.childs) == 1:
                    paths.append(now)
                else:
                    paths = []
                now = now.childs[token]
        clusterID = now.tag
        if paths == []:
            import pdb; pdb.set_trace()
        paths[0].childs = dict()
        # insert
        self.insert(template, clusterID)
        return clusterID

    def print(self):
        now = self.root
        to_read = list()
        to_read.append(now)
        to_print = []
        while len(to_read):
            now = to_read[0]
            if now == 1:
                print(to_print)
                to_print = []
                del to_read[0]
                continue
            to_print.append(now.value)
            if now.childs != None:
                to_read.append(int(1))
                for key in now.childs.keys():
                    to_read.append(now.childs[key])
            del to_read[0]





def maskdel(template):
    temp = []
    for token in template:
        if token == '<*>':
            temp.append('')
        else:
            temp.append(token)
    return temp

class MaskOnlineLayer(Layer):
    def __init__(self, dict_layer, templates: dict, results: dict, debug=False):
        self.templates = templates
        self.debug = debug
        self.orderedTemplates = dict()
        self.dict_layer = dict_layer
        self.results = results
        self.cluster2Template = dict()
        tot = 0
        # Loading Offline Trie
        self.trie = Trie()
        for key, entry in tqdm(templates.items(), desc='constructing trie'):
            self.trie.insert(entry, tot)
            tot += 1

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

    def run(self, line, log_entry):
        updatedFlag = False
        insertFlag = False
        content = log_entry['Content']

        # final_template = log_entry
        if self.debug:
            print("Assigning template to wordset group...\n")
        # If wordset group already exists
        # import pdb; pdb.set_trace()
        wordset = tuple(sorted(line['dwords']))
        if wordset in self.templates:
            existTemplate = self.orderedTemplates.get(tuple(line['dwords']))
            if existTemplate is not None:
                self.results[log_entry['LineId']] = existTemplate
                return
            old_template = self.templates[wordset]
            updated_template = self.getTemplate(self.LCS(content, old_template), old_template)
            # import pdb; pdb.set_trace()
            if updated_template != old_template:
                # import pdb; pdb.set_trace()
                clusterID = self.trie.update(old_template, updated_template)
                
                self.results[log_entry['LineId']] = clusterID
                self.cluster2Template[clusterID] = updated_template
                self.templates[wordset] = updated_template
                updated_templates_dict = self.dict_layer.checkValid(updated_template)
                if updated_templates_dict:
                    self.orderedTemplates[tuple(updated_templates_dict)] = clusterID
            else:
                self.results[log_entry['LineId']] = self.trie.find(updated_template)
        # If wordset group does not exist
        else:
            self.templates[wordset] = content
            # import pdb; pdb.set_trace()
            tag = self.trie.find(content)
            if tag == -1:
                self.trie.insert(content, len(self.templates))
                tag = len(self.templates)
                updated_templates_dict = self.dict_layer.checkValid(content)
                if updated_templates_dict:
                    self.orderedTemplates[tuple(updated_templates_dict)] = len(self.templates)
                
                self.cluster2Template[len(self.templates)] = content
            # else: 
            #     import pdb; pdb.set_trace()
            self.results[log_entry['LineId']] = tag

        # if insertFlag:
        #     self.trie.insert(log_entry, len(self.templates))

        # if updatedFlag:
        #     final_template = self.trie.update(updated_template)

        if self.debug:
            if updatedFlag:
                print("Update templates...\n")
            if insertFlag:
                print("New templates...\n")
            # print("Final template: ", final_template)
            print("Finishing masking and merging prefix tree...\n")

