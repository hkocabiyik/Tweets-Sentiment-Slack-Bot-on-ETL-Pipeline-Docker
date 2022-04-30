import requests
from sqlalchemy import create_engine
import pandas as pd
import time

time.sleep(10)  # seconds
# Conneting to postgres & creating an engine
engine = create_engine('postgresql://postgres:postgres@postgres:5432/tweets', echo=True)

# The query to get the worse score tweet
query_worst = f"""
    SELECT id, text, created_at, sentiment
    FROM   tweets
    ORDER BY sentiment LIMIT 1
    """
# The query to get the best score tweet 
query_best = f"""
    SELECT id, text, created_at, sentiment
    FROM   tweets
    ORDER BY sentiment DESC LIMIT 1
    """

# reading query and write to variable with pandas:
tweet_worst = pd.read_sql_query(query_worst, con=engine)
tweet_best = pd.read_sql_query(query_best, con=engine)

# get value of text and sentiment
text_tweet_best = tweet_best['text'].iloc[0]
time_tweet_best = tweet_best['created_at'].iloc[0]
sentiment_tweet_best =  tweet_best['sentiment'].iloc[0]
    
text_tweet_worst = tweet_worst['text'].iloc[0]
time_tweet_worst = tweet_worst['created_at'].iloc[0]
sentiment_tweet_worst =  tweet_worst['sentiment'].iloc[0]

# Create the JSON data object
data = {
    "blocks": [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*The Best Tweet Received*\n:+1: With a Score of {sentiment_tweet_best}\n{text_tweet_best}\n*Created at*:{time_tweet_best}"
            }
        },
        {
            "type": "divider"
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*The Worst Tweet Received*\n:-1: With a Score of {sentiment_tweet_worst}\n{text_tweet_worst}\n*Created at*:{time_tweet_worst}"
            }
        }
    ]
}
# Create a Webhook object to connect to SLACK
webhook_url = "https://hooks.slack.com/services/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"

# Post the data to the Slack
requests.post(url=webhook_url, json = data)