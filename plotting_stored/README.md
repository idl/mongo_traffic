#Plotting Stored
Mathlib Plot of stored traffic aggregreates in mongo_local_traffic.txt


## Requirements
1. Tweepy
2. PyMongo for MongoDB storage

#Utilization:
To start streaming:

```
python twitter_streaming.py
```

For restarting the streaming client in case of network issues:
```
* * * * * <path to python>/python2.7 <path to restart script>/restart.py
```