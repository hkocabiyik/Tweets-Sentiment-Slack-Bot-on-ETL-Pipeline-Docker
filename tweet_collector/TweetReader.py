import tweepy
import twitter_keys
import pymongo

# connecting to Twitter API
def connect_to_twitterAPI():
    client = tweepy.Client(bearer_token=twitter_keys.Bearer_Token)
    return client
def connect_to_Mongo(host, port):
    client_docker = pymongo.MongoClient(host=host,port=port)
    db = client_docker.twitter
    return db
def write_to_mongo(client,db, search_query):
    search_tweets = client.search_recent_tweets(query=search_query,tweet_fields=['id','created_at','text'], max_results=100)
    for tweet in search_tweets.data:
        record = {'tweet': tweet.text, 'tweet_id': tweet.id, 'created_at': tweet.created_at}
        db.tweets.insert_one(document=record)

if __name__ == '__main__':
    search_query = "#Ukraine lang:en -is:retweet -is:reply -is:quote -has:links"
    client=connect_to_twitterAPI()
    db=connect_to_Mongo(host="mongodb",port=27017)
    write_to_mongo(client,db,search_query)
    
    