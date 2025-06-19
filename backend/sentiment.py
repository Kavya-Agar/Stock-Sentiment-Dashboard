# backend/sentiment.py
import boto3, json
import os

# Ensure this environment variable is set, e.g., in docker-compose.yml
ENDPOINT_NAME = os.getenv('SAGEMAKER_ENDPOINT_NAME', 'sentiment-endpoint')
client = boto3.client('sagemaker-runtime')

def analyze_sentiment(text):
    """
    Invokes the SageMaker endpoint to get sentiment analysis for the given text.
    Assumes the SageMaker model returns a list containing a dict like
    {'label': 'POSITIVE', 'score': 0.999}.
    """
    try:
        response = client.invoke_endpoint(
            EndpointName=ENDPOINT_NAME,
            ContentType='application/json',
            Body=json.dumps({'inputs': [text]}) # SageMaker expects 'inputs' key
        )
        result = json.loads(response['Body'].read().decode('utf-8'))
        # Adjust based on your model's actual output format
        if isinstance(result, list) and len(result) > 0 and 'label' in result[0] and 'score' in result[0]:
            return result[0] # Return the first item in the list
        else:
            print(f"Warning: Unexpected SageMaker response format: {result}")
            return {'label': 'NEUTRAL', 'score': 0.5} # Fallback
    except Exception as e:
        print(f"Error invoking SageMaker endpoint '{ENDPOINT_NAME}': {e}")
        return {'label': 'ERROR', 'score': 0.0} # Indicate an error