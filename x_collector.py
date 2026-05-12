import os
import tweepy
import psycopg2
from textblob import TextBlob

class MeltwaterXCollector:
    def __init__(self, bearer_token, db_url):
        self.client = tweepy.Client(bearer_token=bearer_token)
        self.db_url = db_url

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
        query = f"({keyword}) -is:retweet lang:en"
        try:
            # We add user_fields and expansions to get 'Enrichment' data
            response = self.client.search_recent_tweets(
                query=query,
                tweet_fields=['created_at', 'author_id', 'public_metrics'],
                user_fields=['location', 'public_metrics'],
                expansions='author_id',
                max_results=100
            )
            
            if response.data:
                users = {u.id: u for u in response.includes['users']} if 'users' in response.includes else {}
                for tweet in response.data:
                    # Sentiment Analysis
                    sent_label, sent_score = self.get_sentiment(tweet.text)
                    
                    # Enrichment
                    user = users.get(tweet.author_id)
                    followers = user.public_metrics['followers_count'] if user else 0
                    loc = user.location if user else "Unknown"

                    self.save_to_db(tweet.id, tweet.text, tweet.author_id, tweet.created_at, 
                                   sent_label, sent_score, followers, loc)
                    print(f" Stored & Enriched: {tweet.id}")
            else:
                print("No new data found.")
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    token = os.getenv('X_BEARER_TOKEN')
    db_url = os.getenv('DATABASE_URL')

    if not token or not db_url:
        raise ValueError("FATAL: Missing GitHub Secrets")

    collector = MeltwaterXCollector(bearer_token=token, db_url=db_url)
    
    # FIXED TARGETS: No spaces in handles, quotes around phrases
    targets = (
        "@SafaricomPLC OR @MTNGroup OR @DangoteGroup OR @KCBGroup OR @EquityBank OR "
        "@AbsaSouthAfrica OR @NetflixNigeria OR @NCBABankKenya OR \"Ministry of Health\" OR "
        "@AWF_Official OR @BidcoGroup OR \"East African Breweries\" OR @Mastercard OR "
        "#AFCON OR #WorldCup OR #TechInAfrica OR #AI OR #AMR"
    )
    
    collector.run_ingestion(keyword=targets)
