#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Common config params to distribute with the other code
"""
__author__ = 'Aaron Daubman <adaubman@echonest.com>'
__date__ = '9/7/12 8:07 PM'

PORT = 50000
HOST = '127.0.0.1'
AUTHKEY = 'pass'
#BASE_URL = 'http://127.0.0.1:8609/solr/playlist/select' #Playlist
BASE_URL = 'http://127.0.0.1:8616/solr/artists/select' #Artist
BASE_OUT_FILE = 'SolrPerfTestResults_'
DELAY_MULT = 0.8
FILTER_LINE = 'artistIDs' #Playlist
#FILTER_LINE = 'qt=/isq' #Artists
REPLACE_TERM = 'qt=/cfq'
REPLACE_WITH = 'qt=/grp'
NUM_CONSUMERS = 2 #Typically the same or one greater than the number of producers
DELAY_IN_PRODUCER = True