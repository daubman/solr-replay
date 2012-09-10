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
import json
import urllib3
from Config import HOST, PORT, AUTHKEY, BASE_URL, DELAY_IN_PRODUCER

__author__ = 'Aaron Daubman <adaubman@echonest.com>'
__date__ = '9/7/12 3:25 PM'

class RequestGenerator():
    def __init__(self, host=None, port=None, authkey=None, baseurl=None, name='1', wq=None, rq=None, delinprod=True):
        self.name = name
        self.baseurl = baseurl
        self.delinprod = delinprod
        self.m = None
        if wq is None or rq is None:
            print 'Initializing RequestGenerator: ' + self.name + ' as BaseManager(address=(' + host + ', ' + str(port) + ', authkey=' + authkey + ') with remote queues'
            BaseManager.register('get_work_queue')
            BaseManager.register('get_result_queue')
            self.m = BaseManager(address=(host, port), authkey=authkey)
            self.m.connect()
            self.work_queue = self.m.get_work_queue()
            self.result_queue = self.m.get_result_queue()
        else:
            print 'Initializing RequestGenerator: ' + self.name + ' with shared local queues'
            self.work_queue = wq
            self.result_queue = rq
        self.work_queue.cancel_join_thread()
        self.result_queue.cancel_join_thread()
        self.http = urllib3.PoolManager()

    def run(self):
        self.running = True
        print 'Running Consumer: ' + self.name
        while self.running:
            try:
                query, oqt = self.work_queue.get()
                ts, taken, qt, nf, sz = self.request_url(query, self.baseurl)
                self.result_queue.put((ts, taken, qt, nf, sz, oqt))
            except KeyboardInterrupt:
                print '\nKeyboardInterrupt detected in Consumer: ' + self.name + ', attempting to exit...'
                #sys.exc_clear()
                self.stop()
                return

    def stop(self):
        self.running = False
        #self.work_queue.close()
        #self.result_queue.close()
        if self.m is not None:
            self.m.shutdown()


    def request_url(self, query, baseurl):
        #myreq = urllib2.Request(baseurl)
        #myreq.add_data(query)
        req_start = datetime.now()
        ts = time.time()
        #with contextlib.closing(urllib2.urlopen(myreq)) as c:
        #    r = json.load(c)
        r = self.http.request('GET', baseurl + '?' + query)
        d = r.data
        td = datetime.now() - req_start
        taken = int(round((td.microseconds + (td.seconds * 1000000.0)) / 1000))
        sz = len(str(d))
        qt = -1
        nf = -1
        try:
            j = json.loads(d)
            #qt = r[r.find('QTime':') + 7:r.find('}')] #attempt to parse QTime from response
            qt = j.get('responseHeader', {}).get('QTime', -1)
            nf = j.get('response', {}).get('numFound', -1)
        except ValueError:
            print 'ValueError raised attempting to parse json: ' + repr(d)
        return ts, taken, qt, nf, sz


if __name__ == '__main__':
    p = RequestGenerator(host=HOST, port=PORT, authkey=AUTHKEY, baseurl=BASE_URL, delinprod=DELAY_IN_PRODUCER)
    p.run()