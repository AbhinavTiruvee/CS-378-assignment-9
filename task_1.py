
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
import sys
from operator import add

from collections import deque
import re

#helper functions
from tweet_helper import parse_json, is_retweet, get_text, get_tweet_from_retweet, get_tweet_id, get_userid

last_5 = deque(maxlen=5)

def parse(line):
    #given a line, parse it
    tweet = parse_json(line) #now is a dict
    
    if tweet is None:
        return []
    
    #if it is a retweet, get the tweet
    retweet = is_retweet(tweet)
    
    tweet_2 = get_tweet_from_retweet(tweet)
    text = get_text(tweet_2) #we have the text now
    
    #find the word data
    if not re.search(r"\bdata\b", text.lower()):
        return [] #data not in the tweet
    
    #get the author id and tweet id
    author_id, tweet_id = get_userid(tweet_2), get_tweet_id(tweet_2)
    if author_id is None or tweet_id is None:
        return [] # missing
    
    group = (author_id, tweet_id, text)
    count = 1 if retweet else 0
    #need like this so we can group it together when we reduce
    return [(group, count)]

def combiner(first, second):
    # in form (total retweets, best tweet count, best tweet text)
    total_retweets = first[0] + second[0]
    #choose tweet with higher retweet count
    if first[1] > second[1]:
        best = first
    elif first[1] < second[1]:
        best = second
    else:
        #equal so compare text
        if first[2] > second[2]:
            best = second
        else:
            best = first
    return (total_retweets, best[1], best[2])

def results(batch_time, rdd):
    top = rdd.takeOrdered(20, key=lambda row: (-row[1][0], row[0]))
    header = ["Time: " + str(batch_time), "(UserID, Retweet Count, Tweet Text)"]
    batch = []
    batch.extend(header)
    for user_id, (total_retweets, retweet_count, tweet_text) in top:
        batch.append(repr((user_id, total_retweets, tweet_text)))
    
    if not top:
        #no lines found
        batch.append("No tweets at this batch time")
        
    to_print = "\n".join(batch)
    print(to_print, flush=True)
    last_5.append(to_print)
    
    #save last 5 blocks to text file
    with open("task_1_output.txt", "w", encoding="utf-8") as file:
        file.write("\n".join(last_5) + "\n")
# create spark configuration
conf = SparkConf()
conf.setAppName("Task 1")

# create spark context with the above configuration
sc = SparkContext(conf=conf)

# sc.setLogLevel("INFO")
# sc.setLogLevel("ERROR")
sc.setLogLevel("FATAL")


# create the Streaming Context from the above spark context with interval size 5 seconds
ssc = StreamingContext(sc, 5)

# read data from port 9009
dataStream = ssc.socketTextStream("localhost", 9009)


tweets = dataStream.flatMap(parse)

window = tweets.window(300, 5)
counts = window.reduceByKey(lambda x, y: x + y)
#change key to user id
by_user = counts.map(lambda row: (row[0][0], (row[1], row[1], row[0][2])))
user_counts = by_user.reduceByKey(combiner)
user_counts.foreachRDD(results)


# start the streaming computation
ssc.start()

try:
    ssc.awaitTerminationOrTimeout(300)
finally:
    ssc.stop(stopSparkContext=True, stopGraceFully=True)




