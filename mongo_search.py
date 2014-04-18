import json
import datetime
import csv
from bson.code import Code
from pymongo import MongoClient
import csv
import sys
import matplotlib.dates as dt
import matplotlib.pyplot as plt
import numpy as np
from operator import itemgetter

class mongo_host(object):
    def __init__(self,mongo_db):
        self.client = MongoClient(mongo_db['host'], mongo_db['default_port'])
        self.db = self.client[mongo_db['db']]
        self.collection = self.db[mongo_db['collection']]
        self.query_field = '$' + mongo_db['query_field']

    def get_data(self):
        try:
            for doc in self.collection.find({"geo.type":"Point"}):
                result = {
                    'time' : doc['created_at'],
                    'geo' : doc['geo']['coordinates']
                }
                yield result
                
        except:
            pass

if __name__ == '__main__':
    total = len(sys.argv)

    if total < 4:
        print "Utilization: python traffic_aggregrate.py <mongo_host> <mongo_db> <mongo_collection> <query_field>"
        exit(0)

    mongo_db = {
        'host' : str(sys.argv[1]),
        'default_port' : 27017,
        'db' : str(sys.argv[2]),
        'collection' : str(sys.argv[3]),
        'query_field' : str(sys.argv[4]),
    }
    
    conn = mongo_host(mongo_db)

    for result in conn.get_data():
        print result