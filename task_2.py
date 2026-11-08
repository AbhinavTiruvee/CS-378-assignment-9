# 1. Student Name: Abhinav Tiruveedhula
#
#     Student UT EID: AST2772
#
# 2. Student Name: Kevin Le
#
#    Student UT EID: kl37995
#
#
# Course Name: CS378 - Cloud Computing 
#
# Unique Number: 51515
#
# Date Created: 11/7/2026

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.ml.feature import StopWordsRemover

import sys
from operator import add

from collections import deque, Counter
import re

#helper functions
from tweet_helper import parse_json, is_retweet, get_text, get_tweet_from_retweet, get_tweet_id, get_userid, get_hashtag, tokenize

last_5 = deque(maxlen=5)

def parse(line):
    #given a line, parse it
    tweet = parse_json(line) #now is a dict
    
    if tweet is None:
        return []
    
    tweet = get_tweet_from_retweet(tweet)
    text = get_text(tweet) #we have the text now
    
    #find the word data
    if not re.search(r"\bdata\b", text.lower()):
        return [] #data not in the tweet
    
    hts = get_hashtag(tweet)
    if not hts:
        return []

    word_count = Counter(tokenize(text, STOP_WORDS))
    return [(t, (1, word_count)) for t in hts]

def combiner(first, second):
    return (first[0] + second[0], first[1] + second[1])

def results(batch_time, rdd):
    top = rdd.takeOrdered(5, key=lambda row: (-row[1][0], row[0]))
    header = ["Time: " + str(batch_time), "(Hashtag, Tweet Count, Top 5 Words)"]
    batch = []
    batch.extend(header)
    for ht, (tweet_count, word_count) in top:
        top_words = sorted(word_count.items(), key=lambda xy: (-xy[1], xy[0]))[:5]
        batch.append(repr((ht, tweet_count, top_words)))
    
    if not top:
        #no lines found
        batch.append("No tweets at this batch time")
        
    to_print = "\n".join(batch)
    print(to_print, flush=True)
    last_5.append(to_print)
    
    #save last 5 blocks to text file
    with open("task_2_output.txt", "w", encoding="utf-8") as file:
        file.write("\n".join(last_5) + "\n")
# create spark configuration
conf = SparkConf()
conf.setAppName("Task 2")

# create spark context with the above configuration
sc = SparkContext(conf=conf)

# sc.setLogLevel("INFO")
# sc.setLogLevel("ERROR")
sc.setLogLevel("FATAL")


# create the Streaming Context from the above spark context with interval size 5 seconds
ssc = StreamingContext(sc, 5)

# read data from port 9009
dataStream = ssc.socketTextStream("localhost", 9009)

STOP_WORDS = set(StopWordsRemover.loadDefaultStopWords("english"))
tweets = dataStream.flatMap(parse)

window = tweets.window(300, 5)
counts = window.reduceByKey(combiner)
counts.foreachRDD(results)


# start the streaming computation
ssc.start()

try:
    ssc.awaitTerminationOrTimeout(120)
finally:
    ssc.stop(stopSparkContext=True, stopGraceFully=True)




