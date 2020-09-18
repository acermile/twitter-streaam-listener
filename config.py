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
"""Creates API object to contact Twitter streams"""
import tweepy
import logging
import os
from google.cloud import pubsub_v1

logger = logging.getLogger()




def get_topic(publisher):
    """

    :param publisher:
    :return:
    """
    project_id = os.getenv('PROJECT_ID', None)
    pubsub_topic = os.getenv('PUBSUB_TOPIC', None)
    topic_path = "projects/cloudarchitectexam/topics/uat_twitter_topic"
    return topic_path


def get_publisher():
    return pubsub_v1.PublisherClient()
