import json
import datetime
import csv
from bson.code import Code
from pymongo import MongoClient
import csv
import sys

class mongo_host(object):
    def __init__(self,mongo_db):
        self.client = MongoClient(mongo_db['host'], mongo_db['default_port'])
        self.db = self.client[mongo_db['db']]
        self.collection = self.db[mongo_db['collection']]
        self.time_field = '$' + mongo_db['time_field']

    def get_traffic(self):
        try:
            tw_stuff = self.collection.aggregate([
                {"$group":{
                    "_id": {
                        "y": { "$year": self.time_field },
                        "m": { '$month': self.time_field },
                        "d": { "$dayOfMonth": self.time_field },
                        #h: { '$hour': self.time_field },
                        #i: { '$minute': self.time_field },
                    },
                    "count": {"$sum": 1},
                    }
                }
                ])
            return tw_stuff
        except:
            return False


if __name__ == '__main__':
    total = len(sys.argv)

    if total < 4:
        print "Utilization: python traffic_aggregrate.py <mongo_host> <mongo_db> <mongo_collection> <time_field>"
        exit(0)

    mongo_db = {
        'host' : str(sys.argv[1]),
        'default_port' : 27017,
        'db' : str(sys.argv[2]),
        'collection' : str(sys.argv[3]),
        'time_field' : str(sys.argv[4]),
    }
    
    conn = mongo_host(mongo_db)

    print conn.get_traffic()