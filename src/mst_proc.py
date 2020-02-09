from smart_open import open,s3_iter_bucket

import json
import numpy as np
import random

from pyspark import SparkContext

SparkContext.setSystemProperty('spark.dynamicAllocation.executorIdleTimeout', '10m')
SparkContext.setSystemProperty('spark.executor.memory', '5g')
SparkContext.setSystemProperty('spark.driver.memory', '5g')

sc = SparkContext.getOrCreate()
sc.setLogLevel("ERROR")


sc.addFile('util.py')
sc.addFile('util_es.py')

from util import *
from util_es import *

es = initES()


INDEX_NAME = 'base'
TYPE_NAME = '_doc'


BUCKET = 'buck-jan'
PREFIX = 'input/wiki/contents/'

keys, files = [], []
for key, file in s3_iter_bucket(BUCKET,prefix=PREFIX,accept_key=lambda key:'part' in key):
    keys.append(key)
    files.append(file)


records = []
for file in files:
    temp = file.decode('utf-8').strip().split('\n')
    if isinstance(temp, str):
        records.append(temp)
    elif isinstance(temp,list):
        records += temp


nltkSentenceParser = nltk.data.load('tokenizers/punkt/english.pickle')

for record in records:
    record = dictContentDecode(json.loads(textClear(record)))

    sentences_parsed = nltkSentenceParser.tokenize(record['content'].strip())
    sentences_rdd = sc.parallelize(sentences_parsed)
    vectors = sentences_rdd.map(bertVectorizeSingleSentence)\
                   .reduce(lambda x,y: str(x)+VECTOR_SEPARATOR+str(y))\
                   .split(VECTOR_SEPARATOR)
    COUNT = 0
    for i,sentence in enumerate(sentences_parsed):

        es.index(index=INDEX_NAME,
                 doc_type=TYPE_NAME,
                 id=record['title']+str(COUNT).zfill(16),
                 body = {'title':record['title'],
                         'content':sentence,
                         'vector':vectors[i],
                         'url':record['url']
                        }
                )
        COUNT += 1
