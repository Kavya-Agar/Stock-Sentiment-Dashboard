# backend/dynamo.py
import boto3
import os
from datetime import datetime

# Configure DynamoDB table name via environment variable
DYNAMODB_TABLE_NAME = os.getenv('DYNAMODB_TABLE_NAME', 'MarketNewsSentiment')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(DYNAMODB_TABLE_NAME)

def insert_headline_sentiment(post_id, headline, sentiment_label, sentiment_score, subreddit, url, timestamp):
    """
    Inserts a processed headline and its sentiment into DynamoDB.
    """
    try:
        table.put_item(
            Item={
                'PostId': post_id,
                'Timestamp': timestamp, # Unix timestamp for sorting
                'Headline': headline,
                'SentimentLabel': sentiment_label,
                'SentimentScore': float(sentiment_score), # Ensure float type
                'Subreddit': subreddit,
                'Url': url,
                'IngestionTime': int(datetime.utcnow().timestamp()) # When we ingested it
            }
        )
        return True
    except Exception as e:
        print(f"Error inserting item into DynamoDB: {e}")
        return False

def query_sentiment_data(start_time, end_time, subreddit=None, limit=100):
    """
    Queries sentiment data from DynamoDB within a given time range.
    Optionally filters by subreddit.
    """
    # Assuming 'Timestamp' is a sort key and 'Subreddit' or a composite key
    # is the partition key for efficient querying.
    # For a simple design, PostId could be the partition key and Timestamp the sort key.
    # If you expect to query by time range often, you might need a GSI on Timestamp.

    # Example query assuming PostId is PK and Timestamp is SK (not ideal for time range)
    # If you want to query by time range primarily, consider DynamoDB best practices
    # for time series data (e.g., using a fixed partition key + Timestamp as sort key,
    # or GSI on Timestamp).

    # For now, let's assume a scan or a more complex query approach if direct
    # time-based queries on a single partition key are needed.
    # A better approach for time series in DynamoDB often involves a fixed partition
    # key (e.g., 'sentiment_data') and 'Timestamp' as the sort key.
    # Or, as you mentioned, Amazon Timestream might be a better fit for time series.

    # For demonstration, a simple scan (not efficient for large datasets):
    response = table.scan(
        FilterExpression=boto3.dynamodb.conditions.Attr('Timestamp').between(start_time, end_time)
    )
    items = response['Items']
    while 'LastEvaluatedKey' in response:
        response = table.scan(
            FilterExpression=boto3.dynamodb.conditions.Attr('Timestamp').between(start_time, end_time),
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        items.extend(response['Items'])

    if subreddit:
        items = [item for item in items if item.get('Subreddit') == subreddit]

    # Sort by timestamp for consistent charting
    items.sort(key=lambda x: x.get('Timestamp', 0))
    return items[:limit] # Return up to limit items