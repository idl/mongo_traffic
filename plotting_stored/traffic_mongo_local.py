import ast
import datetime
import matplotlib.dates as dt
import matplotlib.pyplot as plt
import numpy as np
from operator import itemgetter

def get_data(f_name):
	f = open(f_name,'rb')
	tmp = f.readline()
	d = ast.literal_eval(tmp)
	return d

def main():
	d = get_data("mongo_local_traffic.txt")

	traffic = []
	dates = []
	counts = []

	for each in d['result']:
		year = int(each['_id']['y'])
		month = int(each['_id']['m'])
		day = int(each['_id']['d'])
		hour = int(each['_id']['h'])
		mins = int(each['_id']['i'])

		date_time = datetime.datetime(year,month,day,hour,mins)

		traffic.append([date_time,each['count']])

	traffic = sorted(traffic, key=itemgetter(0))

	for each in traffic:
		dates.append(each[0])
		counts.append(each[1])
	
	#print traffic
	fig, ax = plt.subplots()

	ax.plot(dates,counts, "-")
	ax.autoscale_view()
	ax.grid(True)
	fig.autofmt_xdate()	
	#plt.ylabel('number of tweets')
	plt.show()

if __name__ == "__main__":
	main()


