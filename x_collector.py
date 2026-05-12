import os
import tweepy
import psycopg2

class MeltwaterXCollector:
    def __init__(self, bearer_token, db_url):
        # We use v2 Client for professional ingestion
        self.client = tweepy.Client(bearer_token=bearer_token)
        self.db_url = db_url

    def save_to_db(self, tweet_id, text, author_id, created_at):
        try:
            conn = psycopg2.connect(self.db_url)
            cur = conn.cursor()
            query = """
                INSERT INTO social_media_feeds (tweet_id, content, author, created_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (tweet_id) DO NOTHING;
            """
            cur.execute(query, (str(tweet_id), text, str(author_id), created_at))
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            print(f"Database error: {e}")

    def run_ingestion(self, keyword):
        # The '-is:retweet' ensures we get original 'Meltwater-quality' data
        query = f"({keyword}) -is:retweet lang:en"
        
        try:
            response = self.client.search_recent_tweets(
                query=query,
                tweet_fields=['created_at', 'author_id'],
                max_results=100
            )
            if response.data:
                for tweet in response.data:
                    self.save_to_db(tweet.id, tweet.text, tweet.author_id, tweet.created_at)
                    print(f"Stored Tweet: {tweet.id}")
            else:
                print("No new data found in this window.")
        except tweepy.errors.Unauthorized:
            print("401 Unauthorized: Your Bearer Token is invalid or permissions are restricted.")
        except Exception as e:
            print(f"API Error: {e}")

if __name__ == "__main__":
    token = os.getenv('X_BEARER_TOKEN')
    db_url = os.getenv('DATABASE_URL')

    if not token or not db_url:
        raise ValueError("FATAL: Missing GitHub Secrets (X_BEARER_TOKEN or DATABASE_URL)")

    collector = MeltwaterXCollector(bearer_token=token, db_url=db_url)
    
    # Clean, professional query without illegal spaces or symbols
    targets = "@SafaricomPLC OR @MTNGroup OR @DangoteGroup OR @KCB OR @Equity OR @ABSA OR @SFA OR @Netflix OR @NCBA OR @Ministry of health OR @AWF OR @BIDCO OR @East African Breweries Limited OR @MasterCard OR @MasterCard Foundation" "#AFCON OR #WorldCup OR #TechInAfrica #AI OR #AMR"            
    
    collector.run_ingestion(keyword=targets)
