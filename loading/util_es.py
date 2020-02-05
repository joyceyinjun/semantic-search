import json

import redis

def fetchRedis():
    return redis.Redis.from_url('',
                 decode_responses=True)

def redisPutVectors(rds,res):
    for hit in res['hits']['hits']:
        if len(hit['_source']['content'].split()) >= 3:
            rds.set(hit['_id'],hit['_source']['vector'])


import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch.helpers import scan,bulk
from requests_aws4auth import AWS4Auth

NUM_ID_LENGTH = 16

def initES(flag=0,timeout=100000):
    host = ''
    region = ''

    service = 'es'
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, \
                       region, service, session_token=credentials.token)

    return Elasticsearch(
                hosts = [{'host': host, 'port': 443}],
                http_auth = awsauth,
                use_ssl = True,
                verify_certs = True,
                connection_class = RequestsHttpConnection,
                timeout=timeout
                )


def esMatch(es,INDEX_NAME,TYPE_NAME,key,query_entry):
    res = es.search(index=INDEX_NAME,
                    doc_type=TYPE_NAME,
                    body=
                      {'query':
                        {'match':
                          {'title':query_entry}
                        }
                      }
                   )
    return res['hits']['hits']['total']


def esGetById(es,INDEX_NAME,TYPE_NAME,id,neighbors=False):

    try:
        current = es.get(index=INDEX_NAME,
                         doc_type=TYPE_NAME,
                         id=id
                        )
    except:
        current = {'_source': None}
    if not neighbors:
        return current['_source']

    records = {'current': current['_source'],
               'left_neighbor': None,
               'right_neighbor': None}
    local_id = int(id[-NUM_ID_LENGTH:])
    l_id = id[:-16]+str(local_id-1).zfill(NUM_ID_LENGTH)
    records['left_neighbor'] = esGetById(es,INDEX_NAME,TYPE_NAME,l_id)
    r_id = id[:-16]+str(local_id+1).zfill(NUM_ID_LENGTH)
    records['right_neighbor'] = esGetById(es,INDEX_NAME,TYPE_NAME,r_id)

    return records


def esPutVectors(es,INDEX_NAME,TYPE_NAME,res,MIN_LENGTH):
    for hit in res['hits']['hits']:
        if len(hit['_source']['content'].split()) >= MIN_LENGTH:
            vec = hit['_source']['vector'].strip()[1:-1].split(',')
            vec = ','.join([x.strip()[:7] for x in vec])
            es.index(index=INDEX_NAME,
                     doc_type=TYPE_NAME,
                     id=hit['_id'],
                     body={'vector': vec}
                    )

def esExtractIdVectors(res,acceptance_rate=1):
    ids, vectors = [], []
    for hit in res['hits']['hits']:
        if acceptance_rate==1:
            ids.append(hit['_id'])
            vectors.append(json.loads('['+hit['_source']['vector']+']'))
        else:
            from random import random
            if random() < acceptance_rate:
                ids.append(hit['_id'])
                vectors.append(json.loads('['+hit['_source']['vector']+']'))
    return ids, vectors
