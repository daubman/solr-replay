#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Common config params to distribute with the other code
"""
__author__ = 'Aaron Daubman <adaubman@echonest.com>'
__date__ = '9/7/12 8:07 PM'

PORT = 50000
#HOST = '127.0.0.1'
HOST = 'dca-perf-solr01.p.echonest.net'  # Where the manager is running
AUTHKEY = 'pass'
BASE_URL = 'http://dca-solr13.p.echonest.net:8502/solr/'
BASE_OUT_FILE = 'ArtistsPerf_'
DELAY_MULT = 0.1  # set to 0 to disable delay, 1 for same delay, <1 for speedup, >1 for slowdown..
REPLACE_TERM = 'wt=python'
REPLACE_WITH = 'wt=json'
#REPLACE_TERM = None
#REPLACE_WITH = None
MANAGER_STARTS_REQGENS = 20  # Typically the same or one greater than the number of producers, 0 if running distributed Consumers
DELAY_IN_PRODUCER = False
STAT_INTERVAL = 1