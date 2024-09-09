import requests
import json
from kafka import KafkaProducer
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import time
import logging

# Subreddit name for example usage
subreddit_name = "EDC"

# Default args for Airflow DAG
default_args = {
    'owner': 'pimpas',
    'start_date': datetime(2024, 9, 9),
}

def get_data(subreddit_name):
    base_url = f'https://oauth.reddit.com/r/{subreddit_name}/new.json'
    headers = {'User-Agent': 'Mozilla/5.0'}
    params = {'limit': 50}  # Adjust limit as needed

    # Fetch posts from the subreddit
    res = requests.get(base_url, headers=headers, params=params)
    data = res.json()

    # Collect post titles with more than 10 upvotes
    post_data = []
    for post in data['data']['children']:
        upvotes = post['data']['ups']
        if upvotes > 1:
            post_info = {
                'title': post['data']['title'],
                'upvotes': upvotes,
            }
            post_data.append(post_info)

    return post_data

def stream_data():

    # Kafka Producer setup
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)

    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60:
            break
        try:
            # Get filtered Reddit data
            res = get_data(subreddit_name)
            # Organize data for Kafka stream (no printing)
            organized_data = json.dumps(res, indent=2)
            # Send the organized data to Kafka topic 'reddit_post'
            producer.send('reddit_post', organized_data.encode('utf-8'))
        except Exception as e:
            logging.error(f"Error: {e}")
            continue

with DAG(
    'user_automation',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )
