from util import *
from util_es import *
import numpy as np
from time import time
import pickle, sys

import faiss

es = initES(1000000)

INDEX_NAME = 'vecs'
TYPE_NAME = '_doc'
scroll_size = 500
ACCEPTANCE_RATE = .8

ids = []
vectors = []
time_beg = time()

res = es.search(index=INDEX_NAME,
                doc_type=TYPE_NAME,
                scroll='2m',
                size=scroll_size,
                body={'query':{'match_all':{}}}
               )

sid = res['_scroll_id']

res_id, res_vec = esExtractIdVectors(res,ACCEPTANCE_RATE)
ids += res_id
vectors += res_vec


while scroll_size>0:
    res = es.scroll(scroll_id=sid,scroll='2m')
    sid = res['_scroll_id']
    scroll_size = len(res['hits']['hits'])
    res_id, res_vec = esExtractIdVectors(res,ACCEPTANCE_RATE)
    ids += res_id
    vectors += res_vec
    COUNT += 1



vectors = np.array(vectors).astype('float32')
faissIndex = faiss.IndexFlatIP(DIM_BERT)
faissIndex.add(vectors)



def webQuery(faissIndex,query,n_query_return=10):

    query_vector = np.expand_dims(np.array(\
                   bertVectorizeSingleSentence(query)),axis=0).astype('float32')

    D, I = faissIndex.search(query_vector,n_query_return)
    I = [x for x in I[0] if x >= 0]

    lines_print = []
    for k,i in enumerate(I):
        id = ids[i]
        INDEX_NAME, TYPE_NAME = 'base', '_doc'
        records = esGetById(es,INDEX_NAME,TYPE_NAME,id,neighbors=True)

        try:
            left_neighbor = records['left_neighbor']['content']
        except:
            left_neighbor = None
        try:
            right_neighbor = records['right_neighbor']['content']
        except:
            right_neighbor = None

        line_print = {
                      'similarity': str(similarity(
                          np.squeeze(query_vector),
                          np.squeeze(json.loads(records['current']['vector']))
                                              ))[:8],
                      'url': records['current']['url'],
                      'title': records['current']['title'],
                      'current': records['current']['content'],
                      'left_neighbor': left_neighbor,
                      'right_neighbor': right_neighbor
                     }
        lines_print.append(line_print)

    return sorted(lines_print,key=lambda x: x['similarity'],reverse=True)


if __name__ == '__main__':

    query = input('Enter your query sentence: ')
    webQuery(faissIndex,query=2)
