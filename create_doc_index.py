#
# Homework 5:   exercise 2
# Author:       Federico Lombardi
# Notes:        
# instructions: just start the script

import sys

import os
import re
import string
import nltk
import codecs

from math import log
#from itertools import izip

sentences_tokenizer  = nltk.tokenize.punkt.PunktSentenceTokenizer()
stemmer = nltk.stem.porter.PorterStemmer()
num_pattern = re.compile('[\-\+]?([0-9]+[\.,]?)+([eE]?[\-\+]?[0-9]+)?')

punctuation = string.punctuation + u"—’“”…"

# list of english stopwords
stopwords = nltk.corpus.stopwords.words('english')

def isnumber(s):
    return num_pattern.search(s) is not None


def ispunct(s):
    for char in s:
        if char not in punctuation:
            return False
    return True


def single_letter(w):
    # also matches initials such as "J." in "Homer J. Simpson"
    return len(w.strip('.')) == 1


def get_sentence_words(sentence):

    # list of words
    words = nltk.word_tokenize(sentence)

    # remove trailing unwonted characters from words and convert to lowercase
    # (w.replace('.','') normalizes acronyms)
    words = [w.replace('.','').strip(',:;*`\'-_+').lower() for w in words]

    # 1) remove single letters, punctuation, stopwords and numbers
    # 2) stem words
    words = [stemmer.stem(w) for w in words if not single_letter(w) and not ispunct(w) and w not in stopwords and not isnumber(w)]

    return words



def get_words(abstract):

    # tokenize sentences
    sentences = sentences_tokenizer.tokenize(abstract)

    words = []

    for sentence in sentences:
        words += get_sentence_words(sentence)

    return words



class Counters():

    def __init__(self):
        self.counters = dict()

    def increase(self, key):
        if key in self.counters:
            self.counters[key] += 1
        else:
            self.counters[key] = 1

    def __getitem__(self, key):
        return self.counters[key]

    def __len__(self):
        return len(self.counters)

    def iteritems(self):
        return self.counters.items()



def get_doc_words(filename):
    encodings = ('utf-8', 'latin-1')

    for encoding in encodings:
        try:
            return __get_doc_words(filename, encoding)
        except UnicodeDecodeError:
            pass # do nothing, try next encoding

    return Counters() # empty word list



def header_end(line):
    line = line.upper()
    return (("END" in line) and ("THE SMALL PRINT" in line)) or ("START OF THE PROJECT GUTENBERG" in line)


def footer_start(line):
    return "PROJECT GUTENBERG" in line.upper()


def __get_doc_words(filename, encoding):

    words = Counters()

    with codecs.open(filename, 'r', encoding) as fin:

        # skip header
        while True:
            line = fin.readline()
            if header_end(line): break # End of header

        while True:
            line = fin.readline()
            if line == '': break # EOF

            if footer_start(line): break # End of the ebook

            line = line.strip()
            if line == '': continue # empty line

            line_words = get_words(line)

            for word in line_words:
                words.increase(word)

    return words



class Doc:

    def __init__(self, id, words):
        self.id = id

        # list of (word, tf) pairs sorted by word
        self.words = sorted([(word, count/float(len(words))) for word, count in words.iteritems()])

        self.tfidf = []

    def compute_tfidf(self, word_doc_count, num_docs):
        self.tfidf = [tf * log(num_docs/float(word_doc_count[word])) for (word, tf) in self.words]


    def __str__(self):
        words_tfidf = ','.join(("%s:%f" % (word[0], tfidf) for (word, tfidf) in zip(self.words, self.tfidf)))
        return str("%s\t%s" % (self.id, words_tfidf))



def main(args):
    if len(args) < 2:
        sys.stderr.write('usage: pyhton %s documents-dir\n' % (args[0],))
        sys.exit(1)

    n = 0

    docs_dir = args[1]

    # For each word contains the number of documents that contains that word
    word_doc_count = Counters()

    docs = []

    for current_dir in os.listdir(docs_dir):
        for doc_name in os.listdir(docs_dir+"\\"+current_dir):
            doc_path = os.path.join(docs_dir+"\\"+current_dir, doc_name)
            if os.path.isfile(doc_path):
                sys.stderr.write("Processing file: %s (%d)\n" % (doc_path, len(docs) + 1))
                
                words = get_doc_words(doc_path)
                
                doc = Doc(doc_name, words)
                docs.append(doc)

                for word, _ in doc.words:
                    word_doc_count.increase(word)

    out = open("GutenbergBook.csv","w",encoding='utf-8')           
    for doc in docs:
        doc.compute_tfidf(word_doc_count, len(docs))
        #sys.stdout.write("%s\n" % doc)
        #print("%s\n" % doc.__str__())
        out.write(doc.__str__()+"\n")
    sys.stderr.write("%d files processed.\n" % (len(docs),))



if __name__ == "__main__":
    sys.stdout = codecs.getwriter('utf8')(sys.stdout.buffer)
    main(sys.argv)

