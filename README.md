## Twitter Stream listener based on Twitter Early Access v2 endpoints

### Overview

This Python script collects information from Twitter stream and
pushes Tweets to PubSub.
The Twitter API gives developers access to most of Twitter’s functionality.
You can use the API to read and write information related to Twitter
entities such as tweets, users, and trends.

Technically, the API exposes dozens of HTTP endpoints related to:

  - Tweets
  - Retweets
  - Likes
  - Direct messages
  - Favorites
  - Trends
  - Media


### Configuration

Install dependencies in `requirements.txt` file Create a Twitter
developer account and get Authentication information
[here](https://developer.twitter.com/)

Twitter API requires that all requests use OAuth to authenticate. You
need to:
  - Apply for a Twitter Developer Account
  - Create an application
  - Create the Authentication credentials

Once you create the authentication credentials you will be able to use
the API. These credentials are the following text strings:

- Bearer Token

### Google Cloud information

```
export PROJECT_ID=""
export PUBSUB_TOPIC=""
```

### Twitter authentication

Make sure you create the Twitter information in advanced.


```
export BEARER_TOKEN=""
```

### Running application

You can build Docker container or run the Python script directly. In
this case we will show you how to build and run the Docker container.
