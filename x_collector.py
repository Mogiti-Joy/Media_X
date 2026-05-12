import os
import tweepy
import psycopg2

class MeltwaterXCollector:
    def __init__(self, bearer_token, db_url):
        self.client = tweepy.Client(bearer_token=bearer_token)
        self.db_url = db_url

    def save_to_db(self, tweet_id, text, author_id, created_at):
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

    def run_ingestion(self, keyword):
        response = self.client.search_recent_tweets(
            query=f"{keyword} -is:retweet lang:en",
            tweet_fields=['created_at', 'author_id'],
            max_results=100
        )
        if response.data:
            for tweet in response.data:
                self.save_to_db(tweet.id, tweet.text, tweet.author_id, tweet.created_at)
                print(f"Stored: {tweet.id}")

if __name__ == "__main__":
    token = os.getenv('X_BEARER_TOKEN')
    db_url = os.getenv('DATABASE_URL')
    collector = MeltwaterXCollector(bearer_token=token, db_url=db_url)
    africa_targets = (
        "@SafaricomPLC OR @MTNGroup OR @DangoteGroup OR KCB OR Equity OR ABSA OR SFA OR Netflix OR NCBA OR MOH OR AWF OR BIDCO OR East African Breweries Limited (EABL) OR MasterCard OR MasterCard Foundation" 
        "#AFCON OR #WorldCup OR #TechInAfrica #AI OR #AMR"            
    )

    collector.run_ingestion(keyword=africa_targets)
