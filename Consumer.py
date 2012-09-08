#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Read delay,url tuples from the central queue and issue request after waiting delay seconds
Requires: urllib3 - https://github.com/shazow/urllib3
    pip install urllib3
"""
from multiprocessing.managers import BaseManager
from datetime import datetime
import time
from Config import HOST, PORT, AUTHKEY, BASE_URL, DELAY_IN_PRODUCER
import json
import urllib3

__author__ = 'Aaron Daubman <adaubman@echonest.com>'
__date__ = '9/7/12 3:25 PM'


class Consumer():
    def __init__(self, host=None, port=None, authkey=None, baseurl=None):
        BaseManager.register('get_work_queue')
        BaseManager.register('get_result_queue')
        self.m = BaseManager(address=(host, port), authkey=authkey)
        self.m.connect()
        self.work_queue = self.m.get_work_queue()
        self.result_queue = self.m.get_result_queue()
        self.http = urllib3.PoolManager()
        while 1:
            try:
                delay, query, oqt = self.work_queue.get()
                if not DELAY_IN_PRODUCER:
                    #Delay here if multiple producers
                    if delay > 2: #safeguard with a max delay of 2 seconds...
                        delay = 2
                    time.sleep(delay)
                ts, taken, qt, nf, sz = self.request_url(query, baseurl)
                #self.result_queue.put('Request time taken: ' + str(taken) + ', QTime: ' + str(qt) + ', numFound: ' + nf + ', Orig QTime: ' + str(oqt))
                self.result_queue.put((ts, taken, qt, nf, sz, oqt))
            except KeyboardInterrupt:
                pass

    def request_url(self, query, baseurl):
        #myreq = urllib3.Request(baseurl)
        #myreq.add_data(query)
        req_start = datetime.now()
        ts = time.time()
        #with contextlib.closing(urllib2.urlopen(myreq)) as c:
        #    r = json.load(c)
        r = self.http.request('GET', baseurl + '?' + query)
        d = r.data
        td = datetime.now() - req_start
        sz = len(str(d))
        j = json.loads(d)
        taken = int(round((td.microseconds + (td.seconds * 1000000.0)) / 1000))
        #qt = r[r.find('QTime":') + 7:r.find('}')] #attempt to parse QTime from response
        qt = j.get("responseHeader", {}).get("QTime", -1.0)
        nf = j.get("response", {}).get("numFound", -1)
        return ts, taken, qt, nf, sz


if __name__ == "__main__":
    p = Consumer(host=HOST, port=PORT, authkey=AUTHKEY, baseurl=BASE_URL)