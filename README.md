# Twitter Stream Analysis

### Intro
Like the name suggests, this project is about analyzing streams of tweets. The underlying streaming
framework is [Apache Flink](https://ci.apache.org/projects/flink/flink-docs-stable/). Flink provides
the structure and orchestration of the streaming data pipeline defined by this project's Java source code.
There are currently two "jobs" this project is capable of performing: a simple counter for tweets, and
a more interesting sentiment analysis pipeline for the contents of tweets. Details about each can be found
below.

### Tweet Counts
Using Flink's built-in [TwitterSource](https://ci.apache.org/projects/flink/flink-docs-/dev/connectors/twitter.html)
, the WindowedTweetCounts class creates a pipeline which ingests a stream of tweets from Twitter's
[Public API](https://help.twitter.com/en/rules-and-policies/twitter-api). It counts the past 15 minutes of tweets,
evaluated in a sliding window one minute at a time.

### Tweet Sentence Sentiment Analysis
Like WindowedTweetCounts, WindowedTwitterPublicSentimentAnalysis ingests a stream of tweets from the 
public Twitter API. It then filters out tweets not labeled by Twitter as being from an english language account,
and runs a mapping on them to generate TwitterSentiments objects, which contain counts of "positive", "neutral"
and "negative" sentiment-sentences per tweet. Sentences containing links and @username tokens are not included,
as the model tends to classify them as "negative". Once per minute, the last fifteen minutes of these TwitterSentiments
objects are reduced down to a single TwitterSentiments object containing the aggregate counts of all tweets
ingested during that timespan. The counts are then inserted into a MongoDB collection as a single document via the 
MongoDBOutputFormat class defined in the outputFormats package.

### Build & Run Sentiment Analysis
Coming soon.