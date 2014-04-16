import sys
import tweepy
import json
from pymongo import MongoClient

consumer_key="BpkCXkd6KYSxc0WetI3jpw"
consumer_secret="sT3qkQk0lMbD9YFi2nGLbdpoAHeSWS2xfhu0wvFGYZU"
access_key = "616747288-ARmQaB5E2s3HHKlXsK6ZFelsLeM3Phva53T1CrOU"
access_secret = "GWJdsPF1jLJkmSQIFZm0qyXtTVj7DeLBD1V6bLpNj9g" 


auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key, access_secret)
api = tweepy.API(auth)

client = MongoClient()
db = client.twitter
tw_collection = db.twitter_geo



@classmethod                    
def parse(cls, api, raw):
        status = cls.first_parse(api, raw)
        setattr(status, 'json', json.dumps(raw))
        return status

tweepy.models.Status.first_parse = tweepy.models.Status.parse
tweepy.models.Status.parse = parse

class CustomStreamListener(tweepy.StreamListener):
    def on_status(self, status):
        #print status.json
        tweet = json.loads(status.json)
        #print tweet
        tw_collection.insert(tweet)

    def on_error(self, status_code):
        print >> sys.stderr, 'Encountered error with status code:', status_code
        return True # Don't kill the stream

    def on_timeout(self):
        print >> sys.stderr, 'Timeout...'
        return True # Don't kill the stream

sapi = tweepy.streaming.Stream(auth, CustomStreamListener())
#sapi.filter(track=['test'])
#sapi.filter(locations=[-124.7625, 24.5210, -66.9326, 49.3845],track=['fire']) #OR operation between geo and fire
sapi.filter(locations=[-124.7625, 24.5210, -66.9326, 49.3845])