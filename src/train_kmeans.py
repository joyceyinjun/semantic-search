from smart_open import open
import pickle,sys
from sklearn.cluster import KMeans

from util import JSON_SEPARATOR,fetchRedis
from util_es import esdoc

rds = fetchRedis()

scroll_size = 1000
attribute_name = 'vector'
decode_func = lambda x: json.loads('['+x+']')


if __name__ == "__main__":

    _, n_centroids, OUT_PICKLE = sys.argv
    n_centroids = int(n_centroids)

    vectors = esdoc.ScrollExtractAttr(scroll_size,
                                  attribute_name,decode_func)

    kmeans = KMeans(n_centroids)
    kmeams.fit(vectors)


    f = open(OUT_PICKLE,'wb')
    pickle.dump(km,f)

    for i in range(kmeans.n_clusters):
        group_i = [x for k,x in enumerate(ids) if cluster_ids[k]==i]
        rds.set(i,JSON_SEPARATOR.join(group_i))

