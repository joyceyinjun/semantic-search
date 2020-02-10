import numpy as np
import nltk.data
import redis
from bert_embedding import BertEmbedding
bert = BertEmbedding()

DIM_BERT = 768

JSON_SEPARATOR = 'KKKKKKK'


def bertVectorizeSingleSentence(str):
    return np.sum(np.array(bert([str])[0][1]),axis=0).tolist()


def extractAttr(res,attribute_name,decode_func=lambda x:x):
    ids, vectors = [], []
    for hit in res:
        ids.append(hit['_id'])
        vectors.append(decode_func(hit['_source'][attribute_name]))
    return ids, vectors


def getIdFromCentroidId(rds,centroid_indices):
#     doc_ids = ''
#     for i in centroid_indices:
#         doc_ids += rds[str(i)]+JSON_SEPARATOR
#     doc_ids = doc_ids.strip(JSON_SEPARATOR).split(JSON_SEPARATOR)
#     return doc_ids

    return [id for i in centroid_indices for id in rds[str(i)]


def similarity(vec1,vec2):
    return np.inner(vec1,vec2)/\
           (np.linalg.norm(vec1)*np.linalg.norm(vec2))

def distance(vec1,vec2):
    return np.linalg.norm(np.array(vec1)-np.array(vec2))


def fetchRedis():
    return redis.Redis.from_url('',
                 decode_responses=True)




