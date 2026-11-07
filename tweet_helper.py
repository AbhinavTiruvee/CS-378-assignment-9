import json
import html
import re


# Ex: {"created_at": "Sat Apr 02 03:38:25 +0000 2022", "id": 1510099307663396864, "id_str": "1510099307663396864", "text": "Get me a UCONN shirt somebody cause for the first time ever I will root for UCONN\ud83d\ude02\ud83d\ude02", "source": "<a href=\"http://twitter.com/download/iphone\" rel=\"nofollow\">Twitter for iPhone</a>", "truncated": false, "in_reply_to_status_id": null, "in_reply_to_status_id_str": null, "in_reply_to_user_id": null, "in_reply_to_user_id_str": null, "in_reply_to_screen_name": null, "user": {"id": 31298409, "id_str": "31298409", "name": "DeAndre", "screen_name": "Dre_got_you", "location": "somewhere in the boot", "url": null, "description": "just a laid back dude #gramfam, Love music and Sports. Fan of Lakers,Saints,Pelicans \u2757\ufe0fFOLLOW ME", "translator_type": "none", "protected": false, "verified": false, "followers_count": 1199, "friends_count": 1526, "listed_count": 31, "favourites_count": 261, "statuses_count": 124239, "created_at": "Wed Apr 15 01:39:54 +0000 2009", "utc_offset": null, "time_zone": null, "geo_enabled": true, "lang": null, "contributors_enabled": false, "is_translator": false, "profile_background_color": "FFCC4D", "profile_background_image_url": "http://abs.twimg.com/images/themes/theme14/bg.gif", "profile_background_image_url_https": "https://abs.twimg.com/images/themes/theme14/bg.gif", "profile_background_tile": false, "profile_link_color": "4A913C", "profile_sidebar_border_color": "000000", "profile_sidebar_fill_color": "000000", "profile_text_color": "000000", "profile_use_background_image": true, "profile_image_url": "http://pbs.twimg.com/profile_images/1211459336968507394/OWvijBnr_normal.jpg", "profile_image_url_https": "https://pbs.twimg.com/profile_images/1211459336968507394/OWvijBnr_normal.jpg", "profile_banner_url": "https://pbs.twimg.com/profile_banners/31298409/1442285583", "default_profile": false, "default_profile_image": false, "following": null, "follow_request_sent": null, "notifications": null, "withheld_in_countries": []}, "geo": null, "coordinates": null, "place": {"id": "c7ef5f3368b68777", "url": "https://api.twitter.com/1.1/geo/id/c7ef5f3368b68777.json", "place_type": "city", "name": "Baton Rouge", "full_name": "Baton Rouge, LA", "country_code": "US", "country": "United States", "bounding_box": {"type": "Polygon", "coordinates": [[[-91.218994, 30.354161], [-91.218994, 30.564039], [-90.962605, 30.564039], [-90.962605, 30.354161]]]}, "attributes": {}}, "contributors": null, "is_quote_status": false, "quote_count": 0, "reply_count": 0, "retweet_count": 0, "favorite_count": 0, "entities": {"hashtags": [], "urls": [], "user_mentions": [], "symbols": []}, "favorited": false, "retweeted": false, "filter_level": "low", "lang": "en", "timestamp_ms": "1648870705892"}


def parse_json(line):
    #takes a line and converts to a tweet dict
    try:
        tweet = json.loads(line)
    except json.JSONDecodeError:
        return None
    if not isinstance(tweet, dict):
        return None
    
    return tweet

def is_retweet(tweet):
    #checks if the given tweet was a retweet
    return isinstance(tweet.get("retweeted_status"), dict)

def get_tweet_from_retweet(tweet):
    if is_retweet(tweet):
        return tweet["retweeted_status"]
    
    return tweet

def get_text(tweet):
    #gets the text from a given tweet
    return tweet.get("extended_tweet", {}).get("full_text") or tweet.get("full_text") or tweet.get("text", "")

def get_userid(tweet):
    user_id = tweet.get("user", {}).get("id_str") or tweet.get("user", {}).get("id")
    
    return str(user_id) if user_id is not None else None

def get_tweet_id(tweet):
    tweet_id = tweet.get("id_str") or tweet.get("id")
    return str(tweet_id) if tweet_id is not None else None

def get_hashtag(tweet): 
    return {hashtag["text"] for hashtag in tweet.get("extended_tweet", tweet).get("entities", {}).get("hashtags", [])}

def tokenize(tweet_text, stop):
    tweet_text = re.sub(r"http\S+", "", html.unescape(tweet_text.lower()))
    words = re.findall(r"(?<![\w#@'])[a-z]{3,}(?![\w'])", tweet_text)
    return [word for word in words if word not in stop]