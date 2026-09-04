import os
import time
import logging
import asyncio
import asyncpg
import httpx
from textblob import TextBlob
from datetime import datetime
from typing import Optional
import urllib.parse

# Structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("MediaPulseXCollector")


class MediaPulseXCollector:
    BASE_URL = "https://api.twitterapi.io/twitter/tweet/advanced_search"
    MAX_RETRIES = 5
    BATCH_SIZE = 100

    def __init__(self, api_key: str, db_url: str):
        self.api_key = api_key
        self.db_url = db_url
        self.pool: Optional[asyncpg.Pool] = None

    # DB: connection pool (one pool for the lifetime of the process)

    async def _init_pool(self):
        self.pool = await asyncpg.create_pool(
            dsn=self.db_url,
            min_size=2,
            max_size=10,
            command_timeout=30
        )
        logger.info("Database connection pool initialised.")

    async def _close_pool(self):
        if self.pool:
            await self.pool.close()

    # Sentiment 
    @staticmethod
    def get_sentiment(text: str) -> tuple[str, float]:
        score = TextBlob(text).sentiment.polarity
        label = "Positive" if score > 0 else ("Negative" if score < 0 else "Neutral")
        return label, round(score, 4)

    # DB: batch upsert
    async def _batch_save(self, records: list[tuple]):
        if not records:
            return
        async with self.pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO social_media_feeds
                    (tweet_id, content, author, created_at,
                     sentiment, sentiment_score, follower_count, user_location)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                ON CONFLICT (tweet_id) DO NOTHING
                """,
                records
            )
        logger.info(f"Batch saved {len(records)} tweets.")

    # HTTP: single page fetch with exponential backoff on rate limits
    async def _fetch_page(
        self,
        client: httpx.AsyncClient,
        params: dict,
        attempt: int = 0
    ) -> Optional[dict]:
        try:
            response = await client.get(
                self.BASE_URL,
                headers={"X-API-Key": self.api_key},
                params=params,
                timeout=15
            )

            if response.status_code == 200:
                return response.json()

            if response.status_code == 429:          # Rate limited
                wait = 2 ** attempt                  # 1s, 2s, 4s, 8s, 16s …
                logger.warning(f"Rate limited. Retrying in {wait}s (attempt {attempt+1}/{self.MAX_RETRIES})")
                await asyncio.sleep(wait)
                if attempt < self.MAX_RETRIES:
                    return await self._fetch_page(client, params, attempt + 1)

            logger.error(f"API error {response.status_code}: {response.text[:200]}")
            return None

        except httpx.RequestError as e:
            logger.error(f"Network error: {e}")
            if attempt < self.MAX_RETRIES:
                await asyncio.sleep(2 ** attempt)
                return await self._fetch_page(client, params, attempt + 1)
            return None

    # Core ingestion: paginate through ALL results, not just page 1
    async def run_ingestion(self, keyword: str):
        await self._init_pool()
        total = 0
        next_cursor = None

        async with httpx.AsyncClient() as client:
            while True:
                params = {
                    "query": keyword,
                    "queryType": "Latest",
                    "count": 100          # max per page
                }
                if next_cursor:
                    params["cursor"] = next_cursor

                data = await self._fetch_page(client, params)
                if not data:
                    break

                tweets = data.get("tweets", [])
                if not tweets:
                    logger.info("No more tweets returned. Pagination complete.")
                    break

                # Build batch
                batch = []
                for tweet in tweets:
                    try:
                        text = tweet.get("text", "")
                        user = tweet.get("author", {})
                        sentiment, score = self.get_sentiment(text)

                        # Parse timestamp safely
                        raw_ts = tweet.get("createdAt", "")
                        try:
                            created_at = datetime.strptime(raw_ts, "%a %b %d %H:%M:%S +0000 %Y")
                        except ValueError:
                            created_at = None

                        batch.append((
                            str(tweet["id"]),
                            text,
                            str(user.get("id", "")),
                            created_at,
                            sentiment,
                            score,
                            user.get("followersCount", 0),
                            user.get("location", "Unknown")
                        ))
                    except KeyError as e:
                        logger.warning(f"Skipping malformed tweet — missing field: {e}")

                await self._batch_save(batch)
                total += len(batch)

                # Check for next page cursor
                next_cursor = data.get("next_cursor") or data.get("nextCursor")
                if not next_cursor:
                    break  

                logger.info(f"Fetched {total} tweets so far. Fetching next page…")
                await asyncio.sleep(0.5)  # Polite pause between pages

        logger.info(f"Ingestion complete. Total tweets saved: {total}")
        await self._close_pool()

# Query — cleaned up and validated
TARGETS = (
    "(#TechInAfrica OR #AfricaTech OR #NairobiTech OR #AfricanSummit OR "
    "#LagosTech OR #CapeTownTech OR #AfricanStartups OR "
    "@SafaricomPLC OR @MTNGroup OR @DangoteGroup OR @KCBGroup OR @EquityBank OR "
    "@AbsaSouthAfrica OR @Netflix OR @NCBABankKenya OR "
    "@AfricanWildlifeFoundation OR @KenyattaNationalHospital OR "
    "@MastercardAfricasecondaryeducation OR "
    "@MastercardAfricatransitions OR "
    "@MastercardAfricacentreforinnovativeteachingandlearning OR "
    "@MastercardAfricaCITL OR "
    "@MastercardAfricascholars OR "
    "#AFCON OR #WorldCup OR #AI OR #AMR) "
    "-is:retweet lang:en"
)
if __name__ == "__main__":
    key = os.getenv("X_BEARER_TOKEN")
    raw_db_url = os.getenv("DATABASE_URL")

    if not key or not raw_db_url:
        raise ValueError("FATAL: Missing environment variables (X_BEARER_TOKEN, DATABASE_URL)")

    # 1. Parse the connection string
    parsed = urllib.parse.urlparse(raw_db_url)
    
    # 2. Extract and URL-encode only the password component if it exists
    if parsed.password:
        encoded_password = urllib.parse.quote_plus(parsed.password)
        # Rebuild the netloc with the safely encoded password
        netloc = f"{parsed.username}:{encoded_password}@{parsed.hostname}"
        if parsed.port:
            netloc += f":{parsed.port}"
        
        # 3. Reconstruct the clean URL
        db_url = parsed._replace(netloc=netloc).geturl()
    else:
        db_url = raw_db_url

    collector = MediaPulseXCollector(api_key=key, db_url=db_url)
    asyncio.run(collector.run_ingestion(keyword=TARGETS))
