# encoding: UTF-8
__date__ = '03/20/2017'
__author__ = 'Hongning Yu'
__email__ = 'hongningy@smu.edu'

"""
Python spider project for CSE7337.
Part of this code is modified from https://github.com/chenshiqu/IR by Chenshiqu.

"""

import stemming
import urllib.request
import urllib
from urllib import parse
import re
from collections import deque
from bs4 import BeautifulSoup
import operator
import copy

class Dictionary(dict):
    # redefine __missing__ function
    def __missing__(self, key):
        return 0


class Document:
    # a describe of a web page
    #    @author: Shiqu Chen
    def __init__(self, url, docID, fileName, fileType):

        self.url = url
        self.docID = docID
        self.name = fileName            # doc_1.txt
        self.type = fileType            #
        self.sName = ''                 # doc_1_stem.txt
        self.title = ''
        self.term = Dictionary()

    def setTitle(self, title):
        '''set the title of the page'''

        self.title = title

    def stem(self):
        '''implement stemming algorithm'''
        split = self.name.split('.')
        self.sName = split[0] + '_stem' + '.txt'
        stemmer = stemming.PorterStemmer()
        with open(self.name) as f:
            while 1:
                output = ''
                word = ''
                line = f.readline()
                if line == '':      # if reach the end, break
                    break
                for c in line:
                    if c.isalpha():
                        word += c.lower()
                    else:
                        if word:
                            output += stemmer.stem(word, 0, len(word) - 1)
                            word = ''
                        output += c.lower()
                with open(self.sName, 'a') as o:
                    o.write(output)

    def collection(self):
        '''extract term and term frequency'''
        with open(self.sName) as f:
            for line in f.readlines():
                words = line.split()
                for word in words:
                    token = word.split('/')
                    for t in token:
                        t = t.rstrip(',.:-=)')
                        t = t.lstrip('(')
                        if t != '':
                            if self.term[t] == 0:
                                self.term[t] = 1
                            else:
                                self.term[t] += 1

    def getTerm(self):
        return self.term

    def getID(self):
        return self.docID

    def getUrl(self):
        return self.url

    def getTitle(self):
        return self.title


class Spider:
    #@author Shiqu Chen
    def __init__(self, url, limit):
        ''' paramaters
            @url :the begin url
            @limit : the limit on the number of pages to be retrieve
        '''
        self.queue = deque()    # containing urls yet to be fetched
        self.visited = set()    # set of url that have been fetched
        self.disallow = []      # containing urls that disallow to access
        self.allUrl = set()     # containing all url in the root url
        self.outUrl = set()     # containing all url that out root
        self.brokenUrl = set()  # saving broken url
        self.image = set()      # saving image url
        self.application = set()        # saving application file url
        self.url = url          # begin page to be crawled
        self.limit = limit      # limit on the number of pages to be retrieve
        self.docNumber = 0
        self.docList = []
        self.term = Dictionary()
        self.new_term = Dictionary()
        self.stop_words = ['to','for','and','the','is','are','it','am','1','2','this','i','there']

    def robots(self):
        # fetch robots.txt and get disallow url
        mark = re.compile(r'Disallow:')
        robots_url = self.url + '/robots.txt'
        url_op = urllib.request.urlopen(robots_url)
        for line in url_op:
            line = line.decode('utf-8')
            if mark.match(line):
                disallow = re.split(': /', line)
                disallow_url = disallow[1].strip()
                self.disallow.append(disallow_url)

    def checkPermit(self, url):
        #check weather access the url is disallow
        #    @return 0: allow
        #            1: disallow
        for disallow_url in self.disallow:
            mark = re.compile(disallow_url)
            if mark.match(url):
                return 1
        return 0

    def urlFormalize(self, currentUrl, rawUrl):
        #  1. ensure urls do not go out of root direction
        #  2. transfer relative url to absolute url

        components = parse.urlparse(rawUrl)
        formalUrl = rawUrl
        if self.checkPermit(components.path) == 1:  # if url is disallow
            formalUrl = ''
            print('    ----Not allowed')
            return formalUrl
        if components.scheme == "http" or components.scheme == "https":  # absolute url
            if components.netloc != 'lyle.smu.edu':  # out of root
                self.outUrl |= {rawUrl}
                formalUrl = ''
                print('    ----Out of root')
            else:
                mark = re.compile('/~fmoore')
                if mark.match(components.path) is None:  # out of root
                    self.outUrl |= {rawUrl}
                    formalUrl = ''
                    print("    ----Out of root")
        elif components.scheme == "":  # relative url
            # transfer relative url to absolute url
            formalUrl = parse.urljoin(currentUrl, rawUrl)
            mark = re.compile(self.url)
            if mark.match(formalUrl) is None:  # out of root
                formalUrl = ''
        else:
            formalUrl = ''

        # if url end with /, add index.html to the url
        # if formalUrl != '' and formalUrl[-1] == '/':
        #    formalUrl = formalUrl + 'index.html'

        return formalUrl

    def urlDuplicate(self, url):
        ''' eliminate duplicate url
            @return 0: not duplicate
                    1: duplicate
        '''
        duplication = 0
        if url in self.visited:
            duplication = 1
        if url in self.queue:
            duplication = 1
        return duplication

    def parse(self, url, contentType, data):
        ''' address response data
            @contentType
            @data
            @return 1:duplicate
        '''
        if 'htm' in contentType:
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
            self.docNumber += 1
            filename = "doc_" + str(self.docNumber) + ".txt"
            with open(filename, 'w') as f:
                f.write(text)
            document = Document(url, self.docNumber, filename, 'html')
            document.setTitle(title)

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
            self.docNumber += 1
            filename = "doc_" + str(self.docNumber) + ".txt"
            with open(filename, "w") as f:
                f.write(text)
            document = Document(url, self.docNumber, filename, 'txt')
            # self.docList.append(document)

        # term stemming and collection
        document.stem()
        document.collection()
        # duplicate detection
        duplicate = 0
        for d in self.docList:
            if self.duplicate_detection(document, d) == 1:
                print('Duplicate to %d' % d.getID())
                duplicate = 1
                break
        if duplicate == 0:
            self.docList.append(document)
            return 0
        else:
            return 1

    def fetch(self):
        '''whole fetching process'''

        self.queue.append(self.url)
        cnt = 0

        self.robots()  # fetch robots.txt

        while self.queue:
            # fetch url
            url = self.queue.popleft()
            if url in self.visited:  # url has been crawled
                continue
            self.visited |= {url}  # remark as visited

            print('Fetching: ' + str(cnt+1) + '  url: ' + url)

            # limit
            cnt += 1
            if cnt > self.limit:
                break

            # crawling data
            req = urllib.request.Request(url)
            try:
                urlop = urllib.request.urlopen(req)
            except urllib.error.HTTPError:
                self.brokenUrl |= {url}
                print("HTTPError with " + url)
                print("----------------------------------------")
                continue
            except urllib.error.URLError:
                self.brokenUrl |= {url}
                print('URLError with ' + url)
                print('----------------------------------------')
                continue

            # deal with different response file type
            fileType = urlop.getheader('Content-Type')
            if 'text' in fileType:  # text file include txt, htm, html
                print('    Got an text/html: %s' % urlop.geturl())
                # address exception
                try:
                    data = urlop.read().decode('utf-8')
                except:
                    print("-------e-----------------------------------")
                    continue

                # parse data
                t = self.parse(urlop.geturl(), fileType, data)
                if t == 1:
                    print('Content duplicate\n')
                    print('------------------------------------------')
                    continue
                elif t == 0:
                    print('Content not duplicated\n')

                # fetch url from page
                linkre = re.compile('href="(.+?)"')
                for x in linkre.findall(data):
                    print("    Fetched a link in this page: %s" % x)
                    self.allUrl |= {x}
                    formalUrl = self.urlFormalize(urlop.geturl(), x)
                    if formalUrl != '':
                        d = self.urlDuplicate(formalUrl)  # duplicate check
                        if d == 0:
                            self.queue.append(formalUrl)
                            print('    ----Found a new URL. Add ' + formalUrl + ' to queue')
                        else:
                            print("    ----Already visited or queued.")

            elif 'image' in fileType:  # image
                print("    Got an image file")
                self.image |= {url}
                print("----------------------------------------")
                continue
            else:                      # other type like pdf
                print("    Got an application")
                self.application |= {url}
                print('-----------------------------------------')
                continue

            print('------------------------------------------')
        print('**********END OF FETCHING*************')
        print('------------------------------------------')

    def collection(self):
        # term collection
        for d in self.docList:
            dTerm = d.getTerm()
            for key in dTerm.keys():
                if self.term[key] != 0:
                    self.term[key] += 1
                else:
                    self.term[key] = 1
        print('\n Here is the dictionary with term frequency:\n')
        print(str(self.term) + '\n')
        print('Dictionary size: ' + str(len(self.term)) + '\n')
        print('------------------------------------------')

    def stop_words_eliminate(self):
        # eliminate stop words
        self.new_term = copy.copy(self.term)
        for t in self.term.keys():
            if t in self.stop_words:
                self.new_term.pop(t,1)
        print('Now we have eliminated stop words: ')
        print(str(self.new_term) + '\n')


    def duplicate_detection(self, doc1, doc2):
        ''' using k-shingles to detect near-duplication
            here k=1
            doc1 and doc2: document object
            @return 1:duplicate 0: not duplicate
        '''
        dTerm1 = doc1.getTerm()
        dTerm2 = doc2.getTerm()
        termSet1 = set(dTerm1.keys())
        termSet2 = set(dTerm2.keys())

        Jaccard = len(termSet1 & termSet2) / len(termSet1 | termSet2)
        print("Jaccard of doc%d and doc%d : %f" % (doc1.getID(), doc2.getID(), Jaccard))
        if Jaccard > 0.9:
            print(str(doc1.getUrl()) + ' and ' + str(doc2.getUrl()) + ' have duplicate content')
            return 1
        else:
            return 0

    def report(self):
        print('Visited urls:' + str(len(self.visited)))
        for i in self.visited:
            print(i)
        print('--------------------------------------')

        print('Queued urls: ')
        print(self.queue)

        print('------------------------------')
        print('Total number of distinct documents/pages: ' + str(len(self.docList)))
        print('And here are all the distinct docs: ')
        for d in self.docList:
            print('doc' + str(d.getID()) + ': ' + str(d.getUrl()))
            print('     title: ' + str(d.getTitle()))

        print("\nBroken URLs: ")
        for e in self.brokenUrl:
            print(e)

        print("\nNumber of out-going URLs: "+ str(len(self.outUrl)))
        for e in self.outUrl:
            print(e)

        print('\nNumber of image files: ' + str(len(self.image)))
        for e in self.image:
            print(e)
        print('\n')
        print('-------------------------------------------------------')


        self.collection()
        self.stop_words_eliminate()


        # ranking
        print('-------------------------------------------------------')
        print('Top 20 most common words:\n')
        sorted_term = sorted(self.new_term.items(), key=operator.itemgetter(1))
        i = 1
        while i <= 20:
            print(sorted_term[-i])
            i += 1


if __name__ == '__main__':
    '''main process'''
    spider = Spider(url='http://lyle.smu.edu/~fmoore', limit=50)
    spider.fetch()
    spider.report()
