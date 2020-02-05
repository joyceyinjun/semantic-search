import numpy as np
import nltk.data
from bert_embedding import BertEmbedding
bert = BertEmbedding()

DIM_BERT = 768

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

def bertVectorize(str):

    nltkSentenceParser = nltk.data.load('tokenizers/punkt/english.pickle')
    sentences = [x.strip() for x in nltkSentenceParser.\
                tokenize(str.strip()) if x.strip()]
    sentences = [x for x in sentences if len(x)>3]
    results = bert(sentences)

    split_sentences = []
    vectors = []
    for k, result in enumerate(results):
        split_sentences.append(' '.join(result[0]))
        vectors.append(np.sum(np.array(result[1]),axis=0).astype('float32')[:DIM_BERT].tolist())
    return SENTENCE_SEPARATOR.join(split_sentences), vectors

def convertDict(record):
    bert = bertVectorize(record['content'].strip())
    record['content'] = bert[0]
    record['vectors'] = bert[1]
    return record

def bertVectorizeSingleSentence(str):
    return np.sum(np.array(bert([str])[0][1]),axis=0).tolist()

def similarity(vec1,vec2):
    return np.inner(vec1,vec2)/\
           (np.linalg.norm(vec1)*np.linalg.norm(vec2))

