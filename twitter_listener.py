# Copyright 2020 Google LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Collects tweets using hashtags."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import requests
import datetime
import json
import logging
import os
import sys
from tenacity import retry, retry_if_exception_type, stop_after_attempt, \
    wait_exponential
import time
import urllib3


from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from config import get_publisher
from config import get_topic


logger = logging.getLogger()

_HASH_TAGS = ['#covid19']
_RATE_LIMIT_RETRIES = 3
_RETRY_DELAY = 1
_RETRY_MULTIPLIER = 1
_RETRY_MAX_DELAY = 4
# Timestamp format for tweet
_TIME_FORMAT = '%Y-%m-%dT%H:%M:%S.000Z'
_CREATED_FORMAT = '%Y-%m-%d %H:%M:%S'

_TWEET_FIELDS = ['text',  'id', 'user_id','posted_at','favorite_count','retweet_count']
_PROCESSED_TWEET_FIELDS = ['id',  'text','created_at','user_id','lang','favorite_count','retweet_count']

Tweet = collections.namedtuple('Tweet', _TWEET_FIELDS)
ProcessedTweet = collections.namedtuple('ProcessedTweet',
                                        _PROCESSED_TWEET_FIELDS)

publisher = get_publisher()
topic_path = get_topic(publisher)




# Method to push messages to pubsub
def write_to_pubsub(data):
    """

    :param data:
    :return:
    """
    try:

        posted_at = datetime.datetime.fromtimestamp(
            data['created_at']).strftime(_CREATED_FORMAT)
        tweet = Tweet(data['text'], data['id'],data['user_id'],posted_at,data['favorite_count'],data['retweet_count'])
        tweet_data = json.dumps(tweet._asdict()).encode('utf-8')
        if data['lang'] == 'en':
            publisher.publish(topic_path,
                              data=tweet_data,
                              tweet_id=str(data['id']).encode('utf-8'))
    except Exception as e:
        raise e


# Method to format a tweet from tweepy.
def reformat_tweet(tweet):
    """

    :param tweet:
    :return:
    """

    tweetid = tweet["data"]["id"]
    # Extract Text.
    text = tweet["data"]["text"]

    lang = tweet["data"]["lang"]

    user_id = tweet["data"]["author_id"]

    retweet_count = tweet["data"]["public_metrics"]["retweet_count"]

    like_count = tweet["data"]["public_metrics"]["like_count"]



    processed_tweet = ProcessedTweet(id=tweetid,
                                     text=text,created_at=time.mktime(
                                         time.strptime(tweet["data"]['created_at'],
                                                       _TIME_FORMAT)),user_id=user_id,lang=lang,favorite_count=like_count,retweet_count=retweet_count)
    logging.info(processed_tweet._asdict())
    return processed_tweet._asdict()


@retry(retry=retry_if_exception_type(urllib3.exceptions.ReadTimeoutError),
       stop=stop_after_attempt(_RATE_LIMIT_RETRIES),
       wait=wait_exponential(multiplier=_RETRY_MULTIPLIER,
                             min=_RETRY_DELAY,
                             max=_RETRY_MAX_DELAY),
       reraise=True,)





def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


def get_rules(headers, bearer_token):
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", headers=headers
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))
    return response.json()


def delete_all_rules(headers, bearer_token, rules):
    if rules is None or "data" not in rules:
        return None

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        headers=headers,
        json=payload
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    print(json.dumps(response.json()))


def set_rules(headers, delete, bearer_token):
    # You can adjust the rules if needed
    sample_rules = [
        {"value": "#covid19", "tag": "covid19"},
    ]
    payload = {"add": sample_rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        headers=headers,
        json=payload,
    )
    if response.status_code != 201:
        raise Exception(
            "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))


def get_stream(headers, set, bearer_token):


    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream?tweet.fields=created_at,author_id,lang,public_metrics", headers=headers, stream=True,
    )
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Cannot get stream (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    for response_line in response.iter_lines():
        if response_line:
            json_response = json.loads(response_line)

            print("Print each key-value pair from JSON response")
            for key, value in json_response.items():
                print(key, ":", value)
            write_to_pubsub(reformat_tweet(json_response))

def disconnect(headers, set, bearer_token):
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream?tweet.fields=created_at,author_id,lang,public_metrics", headers=headers, stream=True,
    )
    response.close()



def main():
    bearer_token = os.getenv('BEARER_TOKEN')
    headers = create_headers(bearer_token)
    rules = get_rules(headers, bearer_token)
    delete = delete_all_rules(headers, bearer_token, rules)
    set = set_rules(headers, delete, bearer_token)




    try:
        logging.info('Start Twitter streaming...')
        get_stream(headers, set, bearer_token)
    except KeyboardInterrupt:
        logging.exception('Stopped.')
    finally:
        logging.info('Done.')
        disconnect(headers, set, bearer_token)
        logging.info('Connection Closed')




if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()
