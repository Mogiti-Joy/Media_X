import os
import requests
import psycopg2
from textblob import TextBlob

class MeltwaterXCollector:
    def __init__(self, api_key, db_url):
        self.api_key = api_key
        self.db_url = db_url
        self.base_url = "https://api.twitterapi.io/twitter/tweet/advanced_search"

    def get_sentiment(self, text):
        analysis = TextBlob(text)
        score = analysis.sentiment.polarity
        if score > 0: return "Positive", score
        elif score < 0: return "Negative", score
        else: return "Neutral", score

    def save_to_db(self, tweet_id, text, author_id, created_at, sentiment, score, followers, location):
        try:
            conn = psycopg2.connect(self.db_url)
            cur = conn.cursor()
            query = """
                INSERT INTO social_media_feeds 
                (tweet_id, content, author, created_at, sentiment, sentiment_score, follower_count, user_location)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (tweet_id) DO NOTHING;
            """
            cur.execute(query, (str(tweet_id), text, str(author_id), created_at, sentiment, score, followers, location))
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            print(f"Database error: {e}")

    def run_ingestion(self, keyword):
        headers = {"X-API-Key": self.api_key}
        params = {
            "query": f"({keyword}) -is:retweet lang:en",
            "queryType": "Latest"
        }
        
        try:
            response = requests.get(self.base_url, headers=headers, params=params)
            if response.status_code == 200:
                data = response.json()
                tweets = data.get('tweets', [])
                
                for tweet in tweets:
                    # 1. Sentiment Analysis
                    sent_label, sent_score = self.get_sentiment(tweet['text'])
                    
                    # 2. Enrichment (twitterapi.io provides user data in the same object)
                    user = tweet.get('author', {})
                    followers = user.get('followersCount', 0)
                    loc = user.get('location', "Unknown")

                    # 3. Save to Neon
                    self.save_to_db(
                        tweet['id'], tweet['text'], user.get('id'), tweet['createdAt'],
                        sent_label, sent_score, followers, loc
                    )
                print(f"Successfully ingested {len(tweets)} tweets.")
            else:
                print(f"API Error {response.status_code}: {response.text}")
        except Exception as e:
            print(f"Pipeline Error: {e}")

if __name__ == "__main__":
    key = os.getenv('X_BEARER_TOKEN') # This is your twitterapi.io key
    db_url = os.getenv('DATABASE_URL')

    if not key or not db_url:
        raise ValueError("FATAL: Missing GitHub Secrets")

    collector = MeltwaterXCollector(api_key=key, db_url=db_url)
    
    targets = (
        "@SafaricomPLC OR @MTNGroup OR @DangoteGroup OR @KCBGroup OR @EquityBank OR "
        "@AbsaSouthAfrica OR @NetflixNigeria OR @NCBABankKenya OR \"Ministry of Health\" OR "
        "@AWF_Official OR @BidcoGroup OR \"East African Breweries\" OR @Mastercard OR "
        "#AFCON OR #WorldCup OR #TechInAfrica OR #AI OR #AMR"
    )
    
    collector.run_ingestion(keyword=targets)
