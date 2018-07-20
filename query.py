from __future__ import print_function
import sys
import json
import datetime
import MySQLdb
import logging as log
from time import sleep
import random
from concurrent.futures import ThreadPoolExecutor
h_list = ["Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.1",
                "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36",
                "Mozilla/5.0 (Windows; U; Windows NT 6.1; x64; fr; rv:1.9.2.13) Gecko/20101203 Firebird/3.6.13",
                "Mozilla/5.0 (compatible, MSIE 11, Windows NT 6.3; Trident/7.0; rv:11.0) like Gecko",
                "Mozilla/5.0 (Windows; U; Windows NT 6.1; rv:2.2) Gecko/20110201",
                "Opera/9.80 (X11; Linux i686; Ubuntu/14.10) Presto/2.12.388 Version/12.16",
                "Mozilla/5.0 (Windows NT 5.2; RW; rv:7.0a1) Gecko/20091211 SeaMonkey/9.23a1pre"]

try:
    from urllib.parse import urlparse, urlencode, urlunparse
except ImportError:
    # Python 2 imports
    from urlparse import urlparse, urlunparse
    from urllib import urlencode

import requests
from abc import ABCMeta
from abc import abstractmethod
from bs4 import BeautifulSoup

dot  = []
db_host = 'localhost'
db_user = 'scrapper'
db_pass = 'scrapper'
db_name = 'prueba'
query1 = ''

__author__ = 'Tom Dickinson'

__author__ = 'Daniel Guerrero'


class TwitterSearch(object):

    __meta__ = ABCMeta

    def __init__(self, rate_delay, error_delay=5):
        """
        :param rate_delay: How long to pause between calls to Twitter
        :param error_delay: How long to pause when an error occurs
        """
        self.rate_delay = rate_delay
        self.error_delay = error_delay

    def search(self, query):
        self.perform_search(query)

    def perform_search(self, query):
        """
        Scrape items from twitter
        :param query:   Query to search Twitter with. Takes form of queries constructed with using Twitters
                        advanced search: https://twitter.com/search-advanced
        """
        url = self.construct_url(query)
        continue_search = True
        min_tweet = None
        response = self.execute_search(url)
        while response is not None and continue_search and response['items_html'] is not None:
            tweets = self.parse_tweets(response['items_html'])

            # If we have no tweets, then we can break the loop early
            if len(tweets) == 0:
                break

            # If we haven't set our min tweet yet, set it now
            if min_tweet is None:
                min_tweet = tweets[0]

            continue_search = self.save_tweets(tweets)

            # Our max tweet is the last tweet in the list
            max_tweet = tweets[-1]
            if min_tweet['tweet_id'] is not max_tweet['tweet_id']:
                if "min_position" in response.keys():
                    max_position = response['min_position']
                else:
                    max_position = "TWEET-%s-%s" % (max_tweet['tweet_id'], min_tweet['tweet_id'])
                url = self.construct_url(query, max_position=max_position)
                # Sleep for our rate_delay
                sleep(self.rate_delay)
                response = self.execute_search(url)

    def execute_search(self, url):
        """
        Executes a search to Twitter for the given URL
        :param url: URL to search twitter with
        :return: A JSON object with data from Twitter
        """
        try:
            # Specify a user agent to prevent Twitter from returning a profile card
            headers = {
                'user-agent': random.choice(h_list)
            }
            req = requests.get(url, headers=headers)
            # response = urllib2.urlopen(req)
            data = json.loads(req.text)

            return data

        # If we get a ValueError exception due to a request timing out, we sleep for our error delay, then make
        # another attempt
        except Exception as e:
            log.error(e)
            log.error("Sleeping for %i" % self.error_delay)
            sleep(self.error_delay)
            return self.execute_search(url)

    @staticmethod
    def parse_tweets(items_html):
        """
        Parses Tweets from the given HTML
        :param items_html: The HTML block with tweets
        :return: A JSON list of tweets
        """
	soup = BeautifulSoup(items_html, "html.parser")
        tweets = []
	dat = [db_host, db_user, db_pass, db_name]
	conn = MySQLdb.connect(*dat, use_unicode=True, charset="utf8mb4") # Conectar a la base de datos
    	cursor = conn.cursor()         # Crear un cursor
	fmt1 = "%Y-%m-%d  %H:%M:%S"

        for li in soup.find_all("li", class_='js-stream-item'):

            # If our li doesn't have a tweet-id, we skip it as it's not going to be a tweet.
            if 'data-item-id' not in li.attrs:
                continue

            tweet = {
               'tweet_id': li['data-item-id'],
               'text': None,
               'user_id': None,
               'user_screen_name': None,
               'user_name': None,
               'created_at': None,
               'retweets': 0,
               'favorites': 0
            }


	    # Tweet date
            date_span = li.find("span", class_="_timestamp")
            if date_span is not None:
              	tweet['created_at'] = float(date_span['data-time-ms'])
		t1 = datetime.datetime.fromtimestamp((tweet['created_at']/1000))
		t1.strftime(fmt1)

                # Tweet Text
                text_p = li.find("p", class_="tweet-text")
                if text_p is not None:
                    tweet['text'] = text_p.get_text()

           	# Tweet User ID, User Screen Name, User Name
                user_details_div = li.find("div", class_="tweet")
                if user_details_div is not None:
                	tweet['user_id'] = user_details_div['data-user-id']
                	tweet['user_screen_name'] = user_details_div['data-screen-name']
                	tweet['user_name'] = user_details_div['data-name']

                # Tweet Retweets
            	retweet_span = li.select(
                	"span.ProfileTweet-action--retweet > span.ProfileTweet-actionCount")
            	if retweet_span is not None and len(retweet_span) > 0:
                	tweet['retweets'] = int(retweet_span[0]['data-tweet-stat-count'])

            	# Tweet Favourites
            	favorite_span = li.select(
                	"span.ProfileTweet-action--favorite > span.ProfileTweet-actionCount")
            	if favorite_span is not None and len(retweet_span) > 0:
                	tweet['favorites'] = int(favorite_span[0]['data-tweet-stat-count'])

       	    	tweets.append(tweet)
            	det = tweet['text']
            	us = (tweet['user_screen_name']).encode('utf-8')
            	rt = tweet['retweets']
            	fav = tweet['favorites']
            	
            	dut = us.replace("'", "").encode('utf-8')
	    	dot = det.replace("'", "").encode('utf-8')
	    	"""
	    	listaPalabras = dot.split()
	    	
	    	frecuenciaPalab = []
	    	for w in listaPalabras:
	    	    frecuenciaPalab.append(listaPalabras.count(w))
	    	
	    	print("Cadena\n" + dot +"\n")
	    	print("Lista\n" + str(listaPalabras) + "\n")
                print("Frecuencias\n" + str(frecuenciaPalab) + "\n")
                print("Pares\n" + str(zip(listaPalabras, frecuenciaPalab)))
	    	"""
	       	query1 = "SELECT count(*) from  ejemplo "
		cursor.execute(query1)
		dats = cursor.fetchall()
		if len(dats) == 0:

	    		query1 = "INSERT INTO ejemplo (contenido, fecha, usuario, c_rts, favs) VALUES ('%s', '[%s]', '@%s', '%d', '%d')" %(dot, t1.strftime(fmt1), dut, rt, fav)
			
		else:
			query1 = "SELECT id FROM ejemplo WHERE fecha = '[%s]'" %t1.strftime(fmt1)
			cursor.execute(query1)
			dats = cursor.fetchall()
			if len(dats) == 0:
				query1 = "INSERT INTO ejemplo (contenido, fecha, usuario, c_rts, favs) VALUES ('%s', '[%s]', '@%s', '%d', '%d')" %(dot, t1.strftime(fmt1), dut, rt, fav) 
				
		cursor.execute(query1)
		conn.commit()
		dats=None

	cursor.close()                 # Cerrar el cursor
        conn.close()                   # Cerrar la conexion


        return tweets

    @staticmethod
    def construct_url(query, max_position=None):
        """
        For a given query, will construct a URL to search Twitter with
        :param query: The query term used to search twitter
        :param max_position: The max_position value to select the next pagination of tweets
        :return: A string URL
        """

        params = {
            # Type Param
            'f': 'tweets',
            # Query Param
            'q': query
        }

        # If our max_position param is not None, we add it to the parameters
        if max_position is not None:
            params['max_position'] = max_position

        url_tupple = ('https', 'twitter.com', '/i/search/timeline', '', urlencode(params), '')
        return urlunparse(url_tupple)

    @abstractmethod
    def save_tweets(self, tweets):
        """
        An abstract method that's called with a list of tweets.
        When implementing this class, you can do whatever you want with these tweets.
        """


class TwitterSearchImpl(TwitterSearch):

    def __init__(self, rate_delay, error_delay, max_tweets):
        """
        :param rate_delay: How long to pause between calls to Twitter
        :param error_delay: How long to pause when an error occurs
        :param max_tweets: Maximum number of tweets to collect for this example
        """
        super(TwitterSearchImpl, self).__init__(rate_delay, error_delay)
        self.max_tweets = max_tweets
        self.counter = 0

    def save_tweets(self, tweets):
        """
        Just prints out tweets
        :return:
        """
        for tweet in tweets:
            # Lets add a counter so we only collect a max number of tweets
            self.counter += 1

            if tweet['created_at'] is not None:
                t = datetime.datetime.fromtimestamp((tweet['created_at']/1000))
                fmt = "%Y-%m-%d %H:%M:%S"
		log.info("%i - [%s] : @%s - %s" % (self.counter, t.strptime(fmt), tweet['user_screen_name'], tweet['text']))

            # When we've reached our max limit, return False so collection stops
            if self.max_tweets is not None and self.counter >= self.max_tweets:
                return False

        return True
