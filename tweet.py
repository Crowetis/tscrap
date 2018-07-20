from __future__ import division
import random
import requests
import datetime as dt
import json
import requests
from abc import ABCMeta
from abc import abstractmethod
from bs4 import BeautifulSoup
from functools import partial
from concurrent.futures import ThreadPoolExecutor
from multiprocessing.pool import Pool
from query import TwitterSearch

class TwitterSlicer(TwitterSearch):
    """
    Inspired by: https://github.com/simonlindgren/TwitterScraper/blob/master/TwitterSucker.py
    The concept is to have an implementation that actually splits the query into multiple days.
    The only additional parameters a user has to input, is a minimum date, and a maximum date.
    This method also supports parallel scraping.
    """

    def __init__(self, rate_delay, error_delay, since, until, n_threads=1):
        super(TwitterSlicer, self).__init__(rate_delay, error_delay)
        self.since = since
        self.until = until
        self.n_threads = n_threads
        self.counter = 0

    def search(self, query):
        n_days = (self.until - self.since).days
        tp = ThreadPoolExecutor(max_workers=self.n_threads)
        for i in range(0, n_days):
            since_query = self.since + datetime.timedelta(days=i)
            until_query = self.since + datetime.timedelta(days=(i + 1))
            day_query = "%s since:%s until:%s" % (query, since_query.strptime("%Y-%m-%d"),
                                                  until_query.strptime("%Y-%m-%d"))
            tp.submit(self.perform_search, day_query)
        tp.shutdown(wait=True)

    def save_tweets(self, tweets):
        """
        Just prints out tweets
        :return: True always
        """

 	for tweet in tweets:
            # Lets add a counter so we only collect a max number of tweets
            self.counter += 1
            if tweet['created_at'] is not None:
                t = datetime.datetime.fromtimestamp((tweet['created_at']/1000))
                fmt = "%Y-%m-%d %H:%M:%S"
                log.info("%i - [%s] : @%s - %s" % (self.counter, t.strptime(fmt), tweet['user_screen_name'], tweet['text']))

 	return True



