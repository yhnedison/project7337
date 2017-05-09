# encoding: utf-8
import re
from stemming import PorterStemmer
from document import Dictionary
import math
import operator


class Engine:

    def __init__(self, doc_list, idf, N):
        self.doc_list = doc_list
        self.idf = idf
        self.N = N              # number of result to show
        self.simi_dic = []
        self.simi_dic_build()
        self.stop_words = ['to', 'for', 'and', 'the', 'is', 'are', 'it', 'am', '1', '2', 'this', 'i', 'there', 'a']

    def simi_dic_build(self):
        # build the thesaurus dictionary, list of list
        stemmer = PorterStemmer()
        mark = re.compile('\w+')
        with open('similar.txt', encoding = 'utf-8') as f:
            for line in f.readlines():
                words = mark.findall(line)
                if len(words) != 0:
                    w = []
                    for word in words:
                        w.append(stemmer.stem(word, 0, len(word)-1))
                    self.simi_dic.append(w)

    def weight_docs(self):
        for doc in self.doc_list:
            doc.weight_doc('log', self.idf)
            doc.normalize()

            # print("Doc ID %d" % doc.get_id())
            # for key in doc.weight.keys():
            #     print("%s:  %f" % (key, doc.weight[key]))

    def query_split(self, query):
        # give a string of query, return a list of words, and stem
        query_terms = []            # stemmed list
        query = query.lower()       # change to lower case
        term_list = query.split()   # unstemmed list
        stemmer = PorterStemmer()
        for term in term_list:
            query_terms.append(stemmer.stem(term, 0, len(term)-1))
        return query_terms

    def stop(self, query_terms):
        for word in query_terms:
            if word in self.stop_words:
                query_terms.remove(word)
        return query_terms

    def query_vectorize(self, query_terms):
        # input a list of query_terms, return query_vector:Dictionary
        query_vector = Dictionary()
        for word in query_terms:
            query_vector[word] = 1
        return query_vector

    def weight_query(self, mode, query_vector):
        # calculate tf-idf for query vector
        # mode = tf or tf-idf
        if mode == 'tf':
            pass
        if mode == 'tf-idf':
            for key in query_vector.keys():
                query_vector[key] = query_vector[key] * self.idf[key]

    def query_vector_normalize(self, query_vector):
        # normalize query vector
        length = 0
        for key in query_vector.keys():
            length += math.pow(query_vector[key], 2)
        length = math.sqrt(length)

        for key in query_vector.keys():
            query_vector[key] = query_vector[key] / length

    def cal_doc_score(self, query_vector):
        for doc in self.doc_list:
            s = 0
            doc_vector = doc.get_weight()
            for key in query_vector.keys():
                s += query_vector[key] * doc_vector[key]
            doc.set_score(s)

    def extra_score(self, query_vector):
        # 0.5 extra points if query word in title
        for doc in self.doc_list:
            title = doc.get_title().lower()
            for key in query_vector.keys():
                if key in title:
                    doc.set_score(doc.get_score() + 0.5)

    def ranking(self):
        # return a list of tuples (index, score)
        number_of_docs = len(self.doc_list)
        doc_score_dict = Dictionary()
        for i in range(number_of_docs):
            doc_score_dict[i] = self.doc_list[i].get_score()

        # sort basing on score
        doc_score_sorted = sorted(doc_score_dict.items(), key=operator.itemgetter(1))
        doc_score_sorted = doc_score_sorted[::-1]

        return doc_score_sorted     # a list of tuple

    def display(self, doc_score_sorted):
        print("\nSEARCH RESULT: \n")
        n = self.N

        if len(self.doc_list) < n:
            r = len(self.doc_list)
        else:
            r = n
        for i in range(r):
            doc = self.doc_list[doc_score_sorted[i][0]]
            if doc.get_score() != 0:
                print("#%d, score %f: %s" % (i+1, doc.get_score(), doc.get_title()))
                print("    URL: %s" % doc.get_url())
                print("    " + doc.doc_display(20) + '\n')

    def query_expand(self, query_terms):
        # expand query using treasures
        query_terms_new = set()
        for term in query_terms:
            for words in self.simi_dic:
                if term in words:
                    for word in words:
                        query_terms_new.add(word)
                    break
        query_terms_new = list(query_terms_new)
        return query_terms_new

    def start(self):
        # call start() to run the engine, enter 'stop' to stop
        self.weight_docs()

        while 1:
            query = input("$ Please input your query: ")
            query = query.strip()
            if query == 'stop':
                break

            query_terms = self.query_split(query)
            query_terms = self.stop(query_terms)

            query_vector = self.query_vectorize(query_terms)

            self.weight_query('tf', query_vector)
            self.query_vector_normalize(query_vector)

            self.cal_doc_score(query_vector)
            self.extra_score(query_vector)

            doc_score_sorted = self.ranking()

            print('------------------------------------------')
            print("Query: %s" % query)
            print("True query terms: ")
            print(query_terms)

            self.display(doc_score_sorted)

            print('------------------------------------------')

            if doc_score_sorted[3][1] == 0:
                # if only top 3(or less) docs have score larger than 0, expand query
                query_terms_new = self.query_expand(query_terms)
                query_terms_new = self.stop(query_terms_new)

                query_vector = self.query_vectorize(query_terms_new)
                self.weight_query('tf', query_vector)
                self.query_vector_normalize(query_vector)
                self.cal_doc_score(query_vector)
                self.extra_score(query_vector)

                doc_score_sorted = self.ranking()
                new_query = ''
                for t in query_terms_new:
                    new_query = new_query + ' ' + t
                print('------------------------------------------')
                print("Expended Query: %s" % new_query)
                print("Stemmed query terms: ")
                print(query_terms_new)

                self.display(doc_score_sorted)
                print('------------------------------------------')

        print("ENGINE CLOSED")




