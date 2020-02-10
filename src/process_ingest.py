from pyspark import SparkContext

SparkContext.setSystemProperty('spark.dynamicAllocation.executorIdleTimeout', '10m')
SparkContext.setSystemProperty('spark.executor.memory', '5g')
SparkContext.setSystemProperty('spark.driver.memory', '5g')

sc = SparkContext.getOrCreate()
sc.setLogLevel("ERROR")


sc.addFile('util.py')
sc.addFile('util_es.py')

from util import getRecordFromBucket,importToES
from util_es import esdoc


BUCKET = ''
PREFIX = ''


if __name__ == "__main__":

    records = getRecordFromBucket(BUCKET,PREFIX)

    importToES(records,esdoc)
