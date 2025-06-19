# backend/ingest.py
import os
import time
import praw  # Python Reddit API Wrapper
from datetime import datetime
from sentiment import analyze_sentiment  # Assuming sentiment.py exists in the same directory
from dynamo import insert_headline_sentiment  # Assuming dynamo.py exists

# --- Configuration ---
REDDIT_CLIENT_ID = os.getenv('REDDIT_CLIENT_ID')
REDDIT_CLIENT_SECRET = os.getenv('REDDIT_CLIENT_SECRET')
REDDIT_USER_AGENT = 'realtime-sentiment-dashboard/0.1 by YourRedditUsername' # Replace with your Reddit username
SUBREDDITS = ['investing', 'stocks', 'finance']
POLL_INTERVAL_SECONDS = 60 # How often to poll Reddit (e.g., every minute)

def initialize_reddit():
    """Initializes and returns a PRAW Reddit instance."""
    if not REDDIT_CLIENT_ID or not REDDIT_CLIENT_SECRET:
        raise ValueError("REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET environment variables must be set.")
    
    return praw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_CLIENT_SECRET,
        user_agent=REDDIT_USER_AGENT
    )

def fetch_and_process_headlines(reddit_instance):
    """
    Fetches new headlines from specified subreddits, analyzes sentiment,
    and stores them in DynamoDB.
    """
    print(f"[{datetime.now()}] Polling Reddit for new headlines...")
    for subreddit_name in SUBREDDITS:
        try:
            subreddit = reddit_instance.subreddit(subreddit_name)
            # Fetch 'hot' posts or 'new' posts. 'New' is better for real-time.
            # You might want to keep track of already processed post IDs to avoid duplicates.
            for submission in subreddit.new(limit=10): # Fetch top 10 new posts
                headline = submission.title
                post_id = submission.id
                url = submission.url
                created_utc = int(submission.created_utc) # Unix timestamp

                # Optional: Check if post_id already processed (e.g., from DynamoDB)
                # For simplicity, we'll process all fetched for now.

                print(f"  Processing: '{headline}' from r/{subreddit_name}")

                # --- Sentiment Analysis ---
                try:
                    sentiment_result = analyze_sentiment(headline)
                    sentiment_label = sentiment_result['label']
                    sentiment_score = sentiment_result['score']
                    print(f"    Sentiment: {sentiment_label} (Score: {sentiment_score:.4f})")
                except Exception as e:
                    print(f"    Error analyzing sentiment for '{headline}': {e}")
                    sentiment_label = "ERROR"
                    sentiment_score = 0.0 # Default/error value

                # --- Store in DynamoDB ---
                try:
                    insert_headline_sentiment(
                        post_id=post_id,
                        headline=headline,
                        sentiment_label=sentiment_label,
                        sentiment_score=sentiment_score,
                        subreddit=subreddit_name,
                        url=url,
                        timestamp=created_utc
                    )
                    print(f"    Stored in DynamoDB.")
                except Exception as e:
                    print(f"    Error storing '{headline}' in DynamoDB: {e}")

        except Exception as e:
            print(f"Error accessing subreddit r/{subreddit_name}: {e}")

def main():
    """Main function to run the Reddit ingestion loop."""
    reddit = initialize_reddit()
    print("Reddit API initialized successfully.")

    while True:
        fetch_and_process_headlines(reddit)
        print(f"Sleeping for {POLL_INTERVAL_SECONDS} seconds...")
        time.sleep(POLL_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()