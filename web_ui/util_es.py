import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch.helpers import scan,bulk
from requests_aws4auth import AWS4Auth
import json

from util import extractAttr,NUM_ID_LENGTH


class esDocument:

    def __init__(self,host,region,INDEX_NAME,TYPE_NAME):

        self.NUM_ID_LENGTH = NUM_ID_LENGTH

        self.host = host
        self.region = region
        self.INDEX_NAME = INDEX_NAME
        self.TYPE_NAME = TYPE_NAME
        self.es = self._initES()


    def _initES(self):
        service = 'es'
        credentials = boto3.Session().get_credentials()
        awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, \
                          region, service, session_token=credentials.token)

        return Elasticsearch(\
                hosts = [{'host': self.host, 'port': 443}],
                http_auth = awsauth,
                use_ssl = True,
                verify_certs = True,
                connection_class = RequestsHttpConnection
                                 )

    def GetById(self,id,neighbors=False):
        try:
            current = self.es.get(index=self.INDEX_NAME,
                         doc_type=self.TYPE_NAME,
                         id=id
                                 )
        except:
            current = {'_source': None}
        if not neighbors:
            return current['_source']

        records = {'current': current['_source'],
                  'left_neighbor': None,
                   'right_neighbor': None}
        local_id = int(id[-self.NUM_ID_LENGTH:])
        l_id = id[:-self.NUM_ID_LENGTH]+\
                 str(local_id-1).zfill(self.NUM_ID_LENGTH)
        records['left_neighbor'] = self.GetById(
                 self.es,self.INDEX_NAME,self.TYPE_NAME,l_id)
        r_id = id[:-self.NUM_ID_LENGTH]+\
                 str(local_id+1).zfill(self.NUM_ID_LENGTH)
        records['right_neighbor'] = self.GetById(
                  self.es,self.INDEX_NAME,self.TYPE_NAME,r_id)

        return records


    def PutIdBody(self,id,body):
        self.es.index(index=self.INDEX_NAME,
                      doc_type=self.TYPE_NAME,
                      id=id,
                      body=body
                     )

    def QueryAttr(self,attr_name,query,size=1000):
        body = {"size": size,
                "query": {
                  "match" : {
                    "content":query
                            }
                         }
                }
        return self.es.search(index=self.INDEX_NAME,
                              doc_type=self.TYPE_NAME,
                              body=body)


    def GetVectorFromId(self,doc_ids):
        res = self.es.mget(index=self.INDEX_NAME,
                   doc_type=self.TYPE_NAME,
                   body={'ids':doc_ids}
                  )
        decode_func = lambda x: json.loads('['+x+']')
        return extractAttr(res['docs'],'vector',decode_func)


    def LoadFromKmeans(self,query,faissKmeansIndex,N_CENTROIDS,rds):
        query_vector = np.expand_dims(np.array(\
                       bertVectorizeSingleSentence(query)),axis=0).astype('float32')

        _, centroid_indices = faissKmeansIndex.search(query_vector,N_CENTROIDS)
        centroid_indices = [x for x in centroid_indices[0] if x >= 0]

        doc_ids = getIdFromCentroidId(rds,centroid_indices)
        vectors = esGetVectorFromId(doc_ids)

        return doc_ids, vectors


    def LoadFromMatch(self,query):
        attr_name = 'content'
        res = self.QueryAttr(attr_name,query,size=1000)

        return extractAttr(res['hits']['hits'],attr_name)


    def ExtractLinesForDisplay(self,doc_ids,indices_picked):
        lines_print = []
        for k,i in enumerate(indices_picked):
            id = doc_ids[i]
            records = self.GetById(id,neighbors=True)

            if records['left_neighbor']:
                content_left_neighbor = records['left_neighbor']['content']
            else:
                content_left_neighbor = '...'
            if records['right_neighbor']:
                content_right_neighbor = records['right_neighbor']['content']
            else:
                content_right_neighbor = '...'

            line_print = {
                      'similarity': str(similarity(
                          np.squeeze(query_vector),
                          np.squeeze(json.loads(records['current']['vector']))
                                              ))[:8],
                      'distance': str(distance(
                          np.squeeze(query_vector),
                          np.squeeze(json.loads(records['current']['vector']))
                                              ))[:8],
                      'url': records['current']['url'],
                      'title': records['current']['title'],
                      'current': records['current']['content'],
                      'left_neighbor': content_left_neighbor,
                      'right_neighbor': content_right_neighbor
                          }
            lines_print.append(line_print)

        return sorted(lines_print,key=lambda x: x['similarity'],reverse=True)



host = ''
region = ''
INDEX_NAME = 'index'
TYPE_NAME = '_doc'

esdoc = edDocument(host,region,INDEX_NAME,TYPE_NAME)

