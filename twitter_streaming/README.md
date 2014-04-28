#Twitter Streaming
=============
Streaming Tweets from the public spitzer hose (1% sample) using tweepy. Currently the search query is for tracking any geo-coded tweets from US.


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