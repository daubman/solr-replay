#!/usr/bin/env python -u
# -*- coding: utf-8 -*-
"""
Adds parsed log file info as tuple (delaytime, query, origQTime) to the centrally managed queue
"""

from multiprocessing.managers import BaseManager
from datetime import datetime
from Config import HOST, PORT, AUTHKEY, DELAY_MULT, FILTER_LINE, REPLACE_TERM, REPLACE_WITH, DELAY_IN_PRODUCER
import sys
import time

__author__ = 'Aaron Daubman <adaubman@echonest.com>'
__date__ = '9/7/12 3:22 PM'


class Producer():
    def __init__(self, host=None, port=None, authkey=None, delmult=1, filterline='', replaceterm=None, replacewith=None):
        BaseManager.register('get_work_queue')
        self.m = BaseManager(address=(host, port), authkey=authkey)
        self.m.connect()
        self.queue = self.m.get_work_queue()
        l = sys.stdin.readline()
        last_ts = self.get_ts(l)
        self.queue.put((0, self.get_url(l), self.get_qt(l)))
        for l in sys.stdin:
            if filterline not in l or 'status=0' not in l:
                print 'Ignoring line not matching filter: ' + filterline
                pass
            ts = self.get_ts(l)
            url = self.get_url(l)

            if replaceterm is not None and replacewith is not None:
                print 'replacing: ' + replaceterm + ' with: ' + replacewith
                url = self.get_url(l).replace(replaceterm, replacewith, 1)

            #we're just looking at a max diff of at most a few seconds here...
            td = ts - last_ts
            delay = (td.microseconds + (td.seconds * 1000000.0)) / 1000000
            #Delay in producer if just one producer - closest to real-world,
            #Otherwise, delay in consumer to approximate traffic distribution
            if DELAY_IN_PRODUCER:
                time.sleep(delay * delmult)
            self.queue.put((delay, url, self.get_qt(l)))
            last_ts = ts


def get_ts(self, l):
    return datetime.strptime(l[:l.find(' ')], '%H:%M:%S,%f')


def get_url(self, l):
    return l[l.find('{') + 1:l.find('}')]


def get_qt(self, l):
    #return l[l.rfind('QTime=')+6:l.rfind(' ')] #if not last
    return int(l[l.rfind('QTime=') + 6:].strip()) #if last


if __name__ == "__main__":
    p = Producer(host=HOST, port=PORT, authkey=AUTHKEY, delmult=DELAY_MULT, filterline=FILTER_LINE, replaceterm=REPLACE_TERM, replacewith=REPLACE_WITH)