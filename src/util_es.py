import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

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

    def ScrooExtractAttr(self,scroll_size,attribute_name,decode_func):

        vectors = []

        res = self.es.search(index=self.INDEX_NAME,
                    doc_type=self.TYPE_NAME,
                    scroll='2m',
                    size=scroll_size,
                    body={'query':{'match_all':{}}}
                             )

        sid = res['_scroll_id']

        _, res_vec = extractAttr(res['hits']['hits'],attribute_name,decode_func)
        vectors += res_vec

        while scroll_size > 0:
            res = self.es.scroll(scroll_id=sid,scroll='2m')
            sid = res['_scroll_id']
            scroll_size = len(res['hits']['hits'])
            _, res_vec = extractAttr(res['hits']['hits'],attribute_name,decode_func)
            vectors += res_vec

        return vectors


host = ''
region = ''
INDEX_NAME = 'index'
TYPE_NAME = '_doc'

esdoc = edDocument(host,region,INDEX_NAME,TYPE_NAME)
