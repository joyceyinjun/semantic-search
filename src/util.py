from smart_open import s3_iter_bucket
import numpy as np
import nltk.data
from bert_embedding import BertEmbedding
import faiss
import redis


nltkSentenceParser = nltk.data.load('tokenizers/punkt/english.pickle')

bert = BertEmbedding()
DIM_BERT = 768

NUM_ID_LENGTH = 16

SENTENCE_SEPARATOR = 'JJJJJJJ'
JSON_SEPARATOR = 'KKKKKKK'
VECTOR_SEPARATOR = 'LLLLLLL'

CODE_SINGLE_QUOTE = '1111111'
CODE_DOUBLE_QUOTE = '2222222'
CODE_SLASH = '7777777'
CODE_BACKSLASH = '8888888'


def sentEncode(str):
    return str.replace('\'',CODE_SINGLE_QUOTE)\
              .replace('\"',CODE_DOUBLE_QUOTE)\
              .replace('\/',CODE_SLASH)\
              .replace('\\',CODE_BACKSLASH)

def sentDecode(str):
    return str.replace(CODE_SINGLE_QUOTE,'\'')\
              .replace(CODE_DOUBLE_QUOTE,'\"')\
              .replace(CODE_SLASH,'\/')\
              .replace(CODE_BACKSLASH,'\\')

def dictContentEncode(dict):
    dict['content'] = sentEncode(dict['content'])
    return dict

def dictContentDecode(dict):
    dict['content'] = sentDecode(dict['content'])
    return dict

def textClear(x):
    return x.replace('\\xa0',' ')\
            .replace('\\200e','')\
            .replace('\'','\"')




def extractAttr(res,attribute_name,decode_func=lambda x:x):
    ids, vectors = [], []
    for hit in res:
        ids.append(hit['_id'])
        vectors.append(decode_func(hit['_source'][attribute_name]))
    return ids, vectors


def bertVectorize(str):

    sentences = [x.strip() for x in nltkSentenceParser.\
                tokenize(str.strip()) if x.strip()]
    sentences = [x for x in sentences if len(x)>3]
    results = bert(sentences)

    split_sentences = []
    vectors = []
    for k, result in enumerate(results):
        split_sentences.append(' '.join(result[0]))
        vectors.append(np.sum(np.array(result[1]),axis=0).astype('float32')[:DIM_BERT].toli$
    return SENTENCE_SEPARATOR.join(split_sentences), vectors

def convertDict(record):
    bert = bertVectorize(record['content'].strip())
    record['content'] = bert[0]
    record['vectors'] = bert[1]
    return record


def bertVectorizeSingleSentence(str):
    return np.sum(np.array(bert([str])[0][1]),axis=0).tolist()


def getRecordFromBucket(BUCKET,PREFIX):

    records = []
    for key, file in s3_iter_bucket(BUCKET,prefix=PREFIX,\
                accept_key=lambda key:'part' in key):

        temp = file.decode('utf-8').strip().split('\n')
        if isinstance(temp, str):
            records.append(temp)
        elif isinstance(temp,list):
            records += temp

    return records


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



def similarity(vec1,vec2):
    return np.inner(vec1,vec2)/\
           (np.linalg.norm(vec1)*np.linalg.norm(vec2))

def distance(vec1,vec2):
    return np.linalg.norm(np.array(vec1)-np.array(vec2))


def faissLoadKmeans(KEMANS_PKL):

    kmeans = pickle.load(open(KMEANS_PKL,'rb'))
    vectors = np.array(kmeans.cluster_centers_).astype('float32')

    faissIndex = faiss.IndexFlatIP(DIM_BERT)
    faissIndex.add(vectors)

    return faissIndex


def fetchRedis():
    return redis.Redis.from_url('',
                 decode_responses=True)




