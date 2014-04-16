import json
import datetime
import csv
from bson.code import Code
from pymongo import MongoClient
import csv

          
if __name__ == '__main__':
    client = MongoClient()
    db = client.twitter
    tw_traffic = db.twitter_geo_indexed

    tw_stuff = db.twitter_geo_indexed.aggregate([
        {"$group":{
            "_id": {
                "y": { "$year": "$date" },
                "m": { '$month': "$date" },
                "d": { "$dayOfMonth": "$date" },
                #h: { '$hour': '$date' },
                #i: { '$minute': '$date' },
            },
            "count": {"$sum": 1},
            }
        }
        ])

    print tw_stuff