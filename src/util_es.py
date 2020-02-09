import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

def initES():
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
                connection_class = RequestsHttpConnection
                )