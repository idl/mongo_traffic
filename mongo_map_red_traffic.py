import json
import datetime
import csv
from bson.code import Code
from pymongo import MongoClient
import csv


# def map_points(input_file):

#     tw_stuff = tw_traffic.map_reduce(mapper,reducer, "results")

#     for doc in tw_stuff:
#         print doc
    
    # count = 0

    # geo_dict = {}

    # with open('./traffic.csv', 'wb') as csvfile:
    #     mapwriter = csv.writer(csvfile)

    #     mapwriter.writerow(['time','count'])


    #     for doc in tw_stuff.find():
    #         geo_dict['user'] = doc['_id']['user']

    #         value = doc['value']

    #         if 'Geo' in value:
    #             geo_dict['location'] = [value['Geo']]
    #             geo_dict['n_loc'] = 1
    #         elif 'Geo_list' in value:
    #             geo_dict['location'] = value['Geo_list']
    #             geo_dict['n_loc'] = value['n_pts']

    #         geo_data = closest_negighbor(geo_dict)

    #         if geo_data != {}:
    #             mapwriter.writerow([geo_data['user'],geo_data['loc'][0],geo_data['loc'][1], geo_data['n_loc']])

    #         print geo_data
    #         count += 1

    #     print count

            
if __name__ == '__main__':
    client = MongoClient()
    db = client.twitter
    tw_traffic = db.twitter_geo

    mapper = Code("""function () {
            var timestamp = new Date(this.created_at);
            var curr_date = timestamp.getDate();
            var curr_month = timestamp.getMonth() + 1;
            var curr_year = timestamp.getFullYear();
            var curr_hour = timestamp.getHours();
            var curr_minutes = timestamp.getMinutes();
            if(curr_month < 10){
                curr_month = "0"+curr_month;
              }
            if(curr_date < 10){
                curr_date = "0"+curr_date;
              }
            if(curr_hour < 10){
                curr_hour = "0"+curr_hour;
              }
            if(curr_minutes < 10){
                curr_minutes = "0"+curr_minutes;
              }
            var time_str = curr_month + "/" + curr_date + "/" + curr_year + " " + curr_hour + ":" + curr_minutes;
            emit(time_str,1);
    }
    """
    )



    reducer = Code("""function(key,values) {
        var total = 0;
        for (var i=0; i < values.length; i++) {
            total += values[i];
        }
        return total;
        }
        """
        )
    
    #tw_stuff = tw_traffic.map_reduce(mapper,reducer, "results", query={"created_at":{'$regex':'Wed Oct 30/'}})
    tw_stuff = tw_traffic.map_reduce(mapper,reducer, "results")

    #print tw_stuff['results']
    for doc in tw_stuff.find():
        print doc
    # for doc in tw_stuff:
    #     print doc
