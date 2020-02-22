import numpy as np
import faiss
from time import time
import pickle, sys
from smart_open import open


from util import *
from util_es import esdoc

rds = fetchRedis()


N_CENTROIDS = 100
N_QUERY_RETURN = 20

KMEANS_PKL = ''
faissKmeansIndex = faissLoadKmeans(KMEANS_PCK)



def webQuery(faissKmeansIndex,query,n_query_return=N_QUERY_RETURN):

    doc_ids = []
    faissIndex = faiss.IndexFlatIP(DIM_BERT)

    ## part 1: import vectors from kmeans model
    doc_ids_kmeans,vectors_kmeans = esdoc.LoadFromKmeans(
                       query,faissKmeansIndex,N_CENTROIDS,rds)
    doc_ids += doc_ids_kmeans
    faissIndex.add(np.array(vectors_kmeans).astype('float32'))


    ## part 2: import vectors from direct match
    doc_ids_match, vectors_match = \
               esdoc.LoadFromMatch(query)
    doc_ids += doc_ids_match
    faissIndex.add(np.squeeze(np.array(vectors_match)).astype('float32'))



    distanes, indices = faissIndex.search(query_vector,n_query_return)
    indices = [x for x in indices[0] if x >= 0]

    return  esdoc.ExtractLinesForDisplay(doc_ids,indices)



if __name__ == '__main__':

    query = input('Enter your query sentence: ')

    webQuery(faissIndex,query,n_query_return=6)



