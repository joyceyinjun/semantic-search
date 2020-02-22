from pyspark import SparkContext

SparkContext.setSystemProperty('spark.dynamicAllocation.executorIdleTimeout', '10m')
SparkContext.setSystemProperty('spark.executor.memory', '5g')
SparkContext.setSystemProperty('spark.driver.memory', '5g')

sc = SparkContext.getOrCreate()
sc.setLogLevel("ERROR")


sc.addFile('util.py')
sc.addFile('util_es.py')

from util import getRecordFromBucket,importToES
from util_es import esdoc


BUCKET = ''
PREFIX = ''


def importToES(records,esdoc):
    for record in records:
        record = dictContentDecode(json.loads(textClear(record)))
            sentences_parsed = nltkSentenceParser.tokenize(record['content'].strip())

            sentences_rdd = sc.parallelize(sentences_parsed)
            vectors = sentences_rdd.map(bertVectorizeSingleSentence)\
                           .reduce(lambda x,y: str(x)+VECTOR_SEPARATOR+str(y))\
                           .split(VECTOR_SEPARATOR)

            COUNT = 0
            for i,sentence in enumerate(sentences_parsed):
                doc_id = record['title']+str(COUNT).zfill(NUM_ID_LENGTH)
                body =  {'title':record['title'],
                         'content':sentence,
                         'vector':vectors[i],
                         'url':record['url']
                        }
                esdoc.PutIdBody(doc_id,body)
                COUNT += 1



if __name__ == "__main__":

    records = getRecordFromBucket(BUCKET,PREFIX)

    importToES(records,esdoc)
