# encoding: utf-8

import urllib.request
import urllib
from urllib import parse
import re
from collections import deque
from bs4 import BeautifulSoup
from document import Document
from document import Dictionary
import operator
import math
from engine import Engine



class Spider:
    """the main spider class for crawling"""
    def __init__(self, url, limit, stop):

        # parameters:   url:   seed url
        #               limit: limit of number of pages to crawl
        #               stop: list of stop words
        self.queue = deque()        # the url frontier
        self.visited = set()        # set of urls that have been visited
        self.disallow = []          # list of urls that not allowed to access
        self.all_url = set()
        self.out_url = set()        # outgoing urls
        self.broken_url = set()     # broken urls
        self.image = set()          # image urls
        self.application = set()    # application urls
        self.url = url              # beginning page to crawl
        self.limit = limit
        self.number_of_doc = 0
        self.doc_list = []          # list containing all documents
        self.term = Dictionary()    # the main dictionary
        self.stop_words = stop      # a list of stop words

        self.idf = Dictionary()     #
        self.term_list = []         # list containing all terms
        self.headers = {'User-Agent': 'Mozilla/5.0', 'name': 'SuperduperBot'}

    def robots(self):
        # fetch robot.txt and get disallowed url
        mark = re.compile(r'Disallow:')
        robots_url = self.url + '/robots.txt'
        url_op = urllib.request.urlopen(robots_url)     # question
        for line in url_op:
            line = line.decode('utf-8')
            if mark.match(line):
                disallow = re.split(': /', line)
                disallow_url = disallow[1].strip()
                print("Disallowed URLs: " + disallow_url)
                self.disallow.append(disallow_url)

    def check_permit(self, url):
        # check whether url is disallowed.
        # return 0: allow
        # return 1: disallow
        for disallow_url in self.disallow:
            mark = re.compile(disallow_url)
            if mark.match(url):
                return 1
        return 0

    def url_formalize(self, current_url, raw_url):
        # ensure urls do not go out of root
        # transfer relative url to absolute url

        components = parse.urlparse(raw_url)
        formal_url = raw_url
        if self.check_permit(components.path) == 1:
            formal_url = ''
            print('    ----Not allowed')
            return formal_url
        if components.scheme == "http" or components.scheme == "https":     # if absolute url
            if components.netloc != 'lyle.smu.edu':     # out of root check
                self.out_url |= {raw_url}
                formal_url = ''
                print('    ----Out of root')
            else:
                mark = re.compile('/~fmoore')
                if mark.match(components.path) is None:     # out of root
                    self.out_url |= {raw_url}
                    formal_url = ''
                    print('    ----Out of root')

        elif components.scheme == "":        # if relative url
            # make relative url to absolute url
            formal_url = parse.urljoin(current_url, raw_url)
            mark = re.compile(self.url)
            if mark.match(formal_url) is None:              # Out of root
                formal_url = ''
        else:
            formal_url = ''

        return formal_url

    def url_duplicate(self, url):
        # eliminate duplicate url
        # return 0 for not duplicate, 1 for duplicate
        duplicate = 0
        if url in self.visited:
            duplicate = 1
        if url in self.queue:
            duplicate = 1
        return duplicate

    def duplicate_detection(self, doc1, doc2):
        # used in parse
        # detect near-duplicates
        d_term1 = doc1.get_terms()
        d_term2 = doc2.get_terms()
        term_set1 = set(d_term1.keys())
        term_set2 = set(d_term2.keys())
        jaccard = len(term_set1 & term_set2)/len(term_set1 | term_set2)
        print("Jaccard of doc%d and doc%d : %f" % (doc1.get_id(), doc2.get_id(), jaccard))
        if jaccard > 0.9:
            print(str(doc1.get_url()) + ' and ' + str(doc2.get_url()) + ' have duplicate content')
            return 1
        else:
            return 0

    def parse(self, url, content_type, data):
        #
        #
        if 'html' in content_type:
            # extract text from html file
            soup = BeautifulSoup(data, "html.parser")
            # kill all script and style elements
            for script in soup(["script", "style"]):
                script.extract()
            # get title
            title = soup.title.string
            # get text
            text = soup.body.get_text()
            # break into lines and remove leading adn trailing spaces
            lines = (line.strip() for line in text.splitlines())
            # break multi-headlines into a line each
            chunks = []
            for line in lines:
                for phrase in line.split("  "):
                    chunks.append(phrase.strip())
            # drop blank lines
            text = '\n'.join(chunk for chunk in chunks if chunk)

            # write to local file
            self.number_of_doc += 1
            filename = "doc_" + str(self.number_of_doc) + ".txt"
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(text)
            document = Document(url, self.number_of_doc, filename, 'html')
            document.set_title(title)

        else:      # txt file
            lines = (line.strip() for line in data.splitlines())
            # break multi-headlines into a line each
            chunks = []
            for line in lines:
                for phrase in line.split("  "):
                    chunks.append(phrase.strip())
            # drop blank lines
            text = '\n'.join(chunk for chunk in chunks if chunk)

            # write to local file
            self.number_of_doc += 1
            filename = "doc_" + str(self.number_of_doc) + ".txt"
            with open(filename, "w") as f:
                f.write(text)
            document = Document(url, self.number_of_doc, filename, 'txt')
            # self.docList.append(document)

        # term stemming and collection
        document.stem()
        document.collection()

        # duplicate detection
        duplicate = 0
        for d in self.doc_list:
            if self.duplicate_detection(document, d) == 1:
                print('Duplicate to %d' % d.get_id())
                duplicate = 1
                break
        if duplicate == 0:
            self.doc_list.append(document)
            return 0
        else:
            return 1

    def fetch(self):
        # fetching from page
        self.queue.append(self.url)
        cnt = 0

        self.robots()  # fetch robots.txt

        # if queue is not empty, get new URL, request, and parse

        while self.queue:
            # fetch url
            url = self.queue.popleft()
            if url in self.visited:  # url has been crawled
                continue
            self.visited |= {url}  # remark as visited

            print('Fetching: ' + str(cnt + 1) + '  url: ' + url)

            # limit
            cnt += 1
            if cnt > self.limit:
                break

            # crawling data
            request = urllib.request.Request(url, data=None, headers=self.headers)
            try:
                response = urllib.request.urlopen(request)
            except urllib.error.HTTPError:
                self.broken_url |= {url}
                print("HTTPError with " + url)
                print("----------------------------------------")
                continue
            except urllib.error.URLError:
                self.broken_url |= {url}
                print('URLError with ' + url)
                print('----------------------------------------')
                continue

            # deal with different response file type
            file_type = response.getheader('Content-Type')
            if 'text' in file_type:  # text file include txt, htm, html
                print('    Got an text/html: %s' % response.geturl())
                # address exception
                try:
                    data = response.read().decode('utf-8', 'ignore')
                except:
                    print("-------e-----------------------------------")
                    continue

                # parse data
                t = self.parse(response.geturl(), file_type, data)
                if t == 1:
                    print('Content duplicate\n')
                    print('------------------------------------------')
                    continue
                elif t == 0:
                    print('Content not duplicated\n')

                # fetch url from page
                linkre = re.compile('href=\s*"?(.+?)"?[ >]')
                for x in linkre.findall(data):
                    print("    Fetched a link in this page: %s" % x)
                    self.all_url |= {x}
                    formal_url = self.url_formalize(response.geturl(), x)
                    if formal_url != '':
                        d = self.url_duplicate(formal_url)  # duplicate check
                        if d == 0:
                            self.queue.append(formal_url)
                            print('    ----Found a new URL. Add ' + formal_url + ' to queue')
                        else:
                            print("    ----Already visited or queued.")

            elif 'image' in file_type:  # image
                print("    Got an image file")
                self.image |= {url}
                print("----------------------------------------")
                continue
            else:  # other type like pdf
                print("    Got an application")
                self.application |= {url}
                print('-----------------------------------------')
                continue

            print('------------------------------------------')
        print('**********END OF FETCHING*************')
        print('------------------------------------------')

    def collection(self):
        # collecting terms from all documents
        # and merge into self.term(the dictionary)
        for d in self.doc_list:
            doc_term_dic = d.get_terms()
            for key in doc_term_dic.keys():
                if self.term[key] != 0:
                    self.term[key] += 1
                else:
                    self.term[key] = 1

        print('\n Here is the dictionary with term frequency:\n')
        print(str(self.term) + '\n')
        print('Dictionary size: ' + str(len(self.term)) + '\n')
        print('------------------------------------------')
        # write dictionary to dictionary.txt after collection
        self.term_list = list(self.term.keys())
        with open("dictionary.txt", "a") as d:
            for word in self.term_list:
                d.write(word+'\n')

    def stop_words_eliminate(self):
        # eliminate stopwords
        for word in stop_words:
            self.term.pop(word, 1)

    def idf_build(self):
        num_of_doc = len(self.doc_list)
        print('\n*********\nNUMBER OF DOCS: %d\n*********\n' % num_of_doc)
        for word in self.term_list:
            df = 0
            for d in self.doc_list:
                doc_terms = d.get_terms()
                if doc_terms[word] != 0:
                    df += 1
            self.idf[word] = math.log(num_of_doc/df, 10)

    def get_idf(self):
        return self.idf

    def get_doc_list(self):
        return self.doc_list

    def report(self):
        # printing and report

        # print visited URLs
        print('Visited urls:' + str(len(self.visited)))
        for i in self.visited:
            print(i)
        print('--------------------------------------')

        # print queued URLs, should be 0 if we have fetched everything
        print('Queued urls: ')
        print(self.queue)

        print('------------------------------')
        print('Total number of distinct documents/pages: ' + str(len(self.doc_list)))
        print('And here are all the distinct docs: ')

        # print all documents in doc_list
        for d in self.doc_list:
            print('doc' + str(d.get_id()) + ': ' + str(d.get_url()))
            print('     title: ' + str(d.get_title()))

        print("\nBroken URLs: ")
        for e in self.broken_url:
            print(e)

        print("\nNumber of out-going URLs: " + str(len(self.out_url)))
        for e in self.out_url:
            print(e)

        print('\nNumber of image files: ' + str(len(self.image)))
        for e in self.image:
            print(e)
        print('\n')
        print('-------------------------------------------------------')

        # collecting terms from all documents and merge into self.term(the dictionary)
        self.collection()
        # eliminate stop words and form a new dictionary called new_terms
        self.stop_words_eliminate()
        self.idf_build()

        # ranking
        print('-------------------------------------------------------')
        print('Top 20 most common words:\n')
        sorted_term = sorted(self.term.items(), key=operator.itemgetter(1))
        i = 1
        while i <= 20:
            print(sorted_term[-i])
            i += 1


if __name__ == '__main__':
    """main process"""
    seed_url = 'http://lyle.smu.edu/~fmoore'
    n = 100
    stop_words = ['to', 'for', 'and', 'the', 'is', 'are', 'it', 'am', '1', '2', 'this', 'i', 'there', 'a']

    spider = Spider(url=seed_url, limit=n, stop=stop_words)
    spider.fetch()
    spider.report()
    print("\n\n")
    print("***************************************")
    print("**           Crawling complete       **")
    print("***************************************")
    my_engine = Engine(spider.get_doc_list(), spider.get_idf(), 6)
    my_engine.start()
