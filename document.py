# encoding: utf-8
import stemming
import re
import math


class Dictionary(dict):
    """redefine __missing__ function"""
    def __missing__(self, key):
        return 0


class Document:
    """a describe of a web page"""
    def __init__(self, url, doc_id, file_name, file_type):
        self.url = url
        self.doc_id = doc_id
        self.name = file_name       # doc_1.txt
        self.type = file_type
        self.s_name = ''            # doc_1_stem.txt
        self.title = ''
        self.term = Dictionary()    # key:term, value:frequency
        self.weight = Dictionary()  # key:term, value:weight
        self.score = 0

    def set_title(self, title):
        # set document title as web page title
        self.title = title

    def stem(self):
        # implement stemming algorithm
        split = self.name.split('.')
        self.s_name = split[0]+'_stem.txt'
        stemmer = stemming.PorterStemmer()
        with open(self.name, encoding='utf-8') as f:
            while 1:
                output = ''
                word = ''
                line = f.readline()     # read a line from file
                if line == '':          # if reach the end, break
                    break
                for c in line:          # for
                    if c.isalpha():
                        word += c.lower()
                    else:
                        if word:
                            output += stemmer.stem(word, 0, len(word) - 1)
                            word = ''
                        output += c.lower()
                with open(self.s_name, 'a', encoding='utf-8') as o:
                            o.write(output)

    def collection(self):
        # extract term and term frequency, store in Dictionary term
        with open(self.s_name, encoding='utf-8') as f:
            for line in f.readlines():   # read all lines

                # extract words and numbers (meaningfull tokens) into dictionary
                word_re = re.compile('[A-Za-z]+')
                words = word_re.findall(line)
                for word in words:
                    if self.term[word] == 0:
                        self.term[word] =1
                    else:
                        self.term[word] += 1

                num_re = re.compile('[0-9]+')
                numbers = num_re.findall(line)
                for number in numbers:
                    if self.term[word] == 0:
                        self.term[word] =1
                    else:
                        self.term[word] += 1

    def get_terms(self):
        return self.term

    def get_id(self):
        return self.doc_id

    def get_url(self):
        return self.url

    def get_title(self):
        return self.title

    def get_weight(self):
        return self.weight

    def set_score(self, score):
        self.score = score

    def get_score(self):
        return self.score

    def weight_doc(self, mode, *args):
        # para: mode = tfidf mode, args=idf
        if mode == 'tf':        # tf mode
            self.weight = self.term
        if mode =='tf-idf':     # tf-idf mode
            for idf in args:
                for word in self.term.keys():
                    self.weight[word] = self.term[word] * idf[word]
        if mode == 'log':       # log-tf-idf mode
            for idf in args:
                for word in self.term.keys():
                    self.weight[word] = (1 + math.log(self.term[word])) * idf[word]

    def normalize(self):
        # cosine normalization
        length = 0
        for word in self.weight.keys():
            length += math.pow(self.weight[word], 2)
        length = math.sqrt(length)

        for word in self.weight.keys():
            self.weight[word] = self.weight[word]/length

    def doc_display(self, n):
        # return string of n words of a document
        with open(self.name, encoding='utf-8') as f:
            content = f.read()

        match = re.compile('\w+')
        out = ''
        words = match.findall(content)
        for word in words:
            if n > 0:
                out = out + ' ' + word
                n = n - 1
        return out






