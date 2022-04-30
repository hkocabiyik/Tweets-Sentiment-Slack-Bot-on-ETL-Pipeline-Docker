import pymongo
import time
from sqlalchemy import create_engine
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import re

time.sleep(3)  # seconds

# Establish a connection to the MongoDB server
client = pymongo.MongoClient(host="mongodb", port=27017)

# Select the database you want to use withing the MongoDB server
db = client.twitter
collection = db.tweets


#connection to postgres
engine = create_engine('postgresql://postgres:postgres@postgres:5432/tweets', echo=True)

# create a table
engine.execute('''
    CREATE TABLE IF NOT EXISTS tweets(
    id  VARCHAR(30),
    text TEXT,
    created_at TIMESTAMP,
    sentiment NUMERIC
);
''')

# Clean your tweets
mentions_regex= '@[A-Za-z0-9]+'
url_regex='https?:\/\/\S+' #this will not catch all possible URLs
hashtag_regex= '#'
rt_regex= 'RT\s'

def clean_tweets(tweet):
    tweet = re.sub(mentions_regex, '', tweet)  #removes @mentions
    tweet = re.sub(hashtag_regex, '', tweet) #removes hashtag symbol
    tweet = re.sub(rt_regex, '', tweet) #removes RT to announce retweet
    tweet = re.sub(url_regex, '', tweet) #removes most URLs
    
    return tweet

# creating a vader sentiment analyzer
analyzer = SentimentIntensityAnalyzer()

docs = db.tweets.find()
for current in docs:
    tweet_text = current['tweet']
    c_tweet_text=clean_tweets(tweet_text)
    tweet_id=current["tweet_id"]
    tweet_created_at=current["created_at"]
    sentiment = analyzer.polarity_scores(c_tweet_text)  # assuming your JSON docs have a text field
    score = sentiment['compound']
    query = "INSERT INTO tweets VALUES (%s, %s, %s, %s);"
    engine.execute(query, (tweet_id,c_tweet_text,tweet_created_at,score))