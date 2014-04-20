import json
import datetime
import csv
from pymongo import MongoClient
import csv
import sys
# import matplotlib.dates as dt
# import matplotlib.pyplot as plt
import numpy as np
from operator import itemgetter
import plotly

class mongo_host(object):
    def __init__(self,mongo_db):
        self.client = MongoClient(mongo_db['host'], mongo_db['default_port'])
        self.db = self.client[mongo_db['db']]
        self.collection = self.db[mongo_db['collection']]
        self.time_field = '$' + mongo_db['time_field']

    def get_traffic(self):
        try:
            results = self.collection.aggregate([
                {"$group":{
                    "_id": {
                        "y": { "$year": self.time_field },
                        "m": { '$month': self.time_field },
                        "d": { "$dayOfMonth": self.time_field },
                        "h": { '$hour': self.time_field },
                        #i: { '$minute': self.time_field },
                    },
                    # "$match" : {"$gt" : {self.time_field : "2012-10-26T12:00:00.000Z"}
                    "count": {"$sum": 1},
                    }
                }
                ])
            return results
        except Exception as e:
            print "here", e
            return False

def plot_data(results):
    traffic = []
    dates = []
    counts = []

    year=month=day=hour=mins = 0

    for each in results['result']:
        if 'y' in each.get('_id', {}):
            year = int(each['_id']['y'])
        if 'm' in each.get('_id', {}):
            month = int(each['_id']['m'])
        if 'd' in each.get('_id', {}):
            day = int(each['_id']['d'])
        if 'h' in each.get('_id', {}):
            hour = int(each['_id']['h'])
        if 'i' in each.get('_id', {}):
            mins = int(each['_id']['i'])

        date_time = datetime.datetime(year,month,day,hour,mins)

        traffic.append([date_time,each['count']])

    traffic = sorted(traffic, key=itemgetter(0))

    print traffic

    with open('output_traffic.csv', 'wb') as csvfile:
        traffic_data = csv.writer(csvfile)
        traffic_data.writerow(['time','count'])

        for result in traffic:
            #print result
            traffic_data.writerow([result[0],result[1]])

    #plotly stuff
    py = plotly.plotly("somyamohanty", "94wksibtod")

    x = []
    y = []

    for each in traffic:
        x.append(each[0])
        y.append(each[1])

    layout = {'title': 'Sandy Keyword - Traffic Statistics',
            'annotations': [{
                'text':'The date-formatted x-axis will increase it\'s time-resolution when you zoom.'+\
                        '<br>Click-and-drag your mouse on the plot to see how it responds!',
                'xref': 'paper', 'yref': 'paper', 'showarrow': False, 'x':0, 'y': 0}]}

    py.iplot([{'x':x, 'y':y, 'mode':'markers'}], layout=layout, filename='Sandy Keyword', fileopt='overwrite', width=1000, height=650)

    # mathplotlib stuff
    # for each in traffic:
    #     dates.append(dt.date2num(each[0]))
    #     counts.append(each[1])


    # fig, ax = plt.subplots()

    # ax.plot(dates,counts, "r-o")
    # ax.set_xticks(dates)
    # ax.set_xticklabels(
    #     [date.strftime("%Y-%m-%d %H:%M") for (date, count) in traffic]
    #     )
    # ax.autoscale_view()
    # ax.grid(True)
    # fig.autofmt_xdate() 
    # plt.ylabel('Number of tweets')
    # plt.xlabel('Time')
    # plt.show()

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

    results = conn.get_traffic()
    if results:
        plot_data(results)