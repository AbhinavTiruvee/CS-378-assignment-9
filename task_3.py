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

KEYWORDS = ["fever", "ache", "pain", "sick"]

last_5 = deque(maxlen=5)

def parse(line):
    #given a line, parse it
    tweet = parse_json(line) #now is a dict
    
    if tweet is None:
        return []
    
    tweet = get_tweet_from_retweet(tweet)
    text = get_text(tweet).lower() #we have the text now

    return [(keyword, 1) for keyword in KEYWORDS if re.search(r"\b" + keyword + r"\b", text)]

# def combiner(first, second):
#     return (first[0] + second[0], first[1] + second[1])

def results(batch_time, rdd):
    count = rdd.collectAsMap()
    header = ["Time: " + str(batch_time)]
    batch = []
    batch.extend(header)
    for keyword in KEYWORDS:
        batch.append(f"{keyword}: {count.get(keyword, 0)}")
        
    to_print = "\n".join(batch)
    print(to_print, flush=True)
    last_5.append(to_print)
    
    #save last 5 blocks to text file
    with open("task_3_output.txt", "a", encoding="utf-8") as file:
        file.write(to_print + "\n")
# create spark configuration
conf = SparkConf()
conf.setAppName("Task 3")

# create spark context with the above configuration
sc = SparkContext(conf=conf)

# sc.setLogLevel("INFO")
# sc.setLogLevel("ERROR")
sc.setLogLevel("FATAL")


# create the Streaming Context from the above spark context with interval size 5 seconds
ssc = StreamingContext(sc, 5)

# read data from port 9009
dataStream = ssc.socketTextStream("localhost", 9009)

# STOP_WORDS = set(StopWordsRemover.loadDefaultStopWords("english"))
tweets = dataStream.flatMap(parse)

window = tweets.window(10, 5)
counts = window.reduceByKey(add)
counts.foreachRDD(results)


# start the streaming computation
ssc.start()

try:
    ssc.awaitTerminationOrTimeout(120)
finally:
    ssc.stop(stopSparkContext=True, stopGraceFully=True)




