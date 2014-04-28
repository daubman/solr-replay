#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Adds parsed log file info as tuple (delaytime, query, origQTime) to the centrally managed queue
"""

from multiprocessing.managers import BaseManager
from datetime import datetime
from Config import HOST, PORT, AUTHKEY, DELAY_MULT, REPLACE_TERM, REPLACE_WITH, DELAY_IN_PRODUCER
import sys
import time

__author__ = 'Aaron Daubman <adaubman@echonest.com>'
__date__ = '9/7/12 3:22 PM'


class LogParser():
    def __init__(self, host=None, port=None, authkey=None, delmult=1, replaceterm=None, replacewith=None, name='1', delinprod=True):
        self.name = name
        self.host = host
        self.port = port
        self.authkey = authkey
        self.delinprod = delinprod
        print 'Initializing LogParser: ' + self.name + ' as BaseManager(address=(' + host + ', ' + str(port) + ', authkey=' + authkey + ') with remote queues'
        BaseManager.register('get_log_queue')
        self.m = BaseManager(address=(host, port), authkey=authkey)
        self.m.connect()
        self.queue = self.m.get_log_queue()
        self.delmult = delmult
        self.replaceterm = replaceterm
        self.replacewith = replacewith

    def run(self):
        print 'Running LogParser: ' + self.name + ' as BaseManager(address=(' + self.host + ', ' + str(self.port) + ', authkey=' + self.authkey + ') with remote queues'
        l = sys.stdin.readline().replace('\t', ' ')
        last_ts = datetime.strptime(self._find_between(' ', ' ', l), '%H:%M:%S,%f')
        self.queue.put((0, self.get_url(l), self.get_qt(l)))
        for l in sys.stdin:
            l = l.replace('\t', ' ')
            #filter in your tail/grep, rather than here...
            #if self.filterline not in l or 'status=0' not in l:
            #    print 'Ignoring line not matching filter: ' + self.filterline
            #    continue
            ts_str = self._find_between(' ', ' ', l)
            try:
                ts = datetime.strptime(ts_str, '%H:%M:%S,%f')
                #we're just looking at a max diff of at most a few seconds here...
                td = ts - last_ts
                delay = (td.microseconds + (td.seconds * 1000000.0)) / 1000000
                last_ts = ts
            except ValueError:
                print 'Could not parse ts: "{0}"'.format(ts_str)
                delay = self.delmult
                last_ts += datetime.timedelta(seconds=self.delmult)
            #Delay in producer if just one producer - closest to real-world,
            #Otherwise, delay in consumer to approximate traffic distribution
            if self.delinprod:
                if delay > 1 or delay < 0:
                   delay = 1
                time.sleep(delay * self.delmult)

            #url = self.replace(l)
            url = self.get_url(l)

            self.queue.put((delay, url, self.get_qt(l)))

    @staticmethod
    def _find_between(start_pattern, end_pattern, instr, start_rfind=False, end_rfind=False):
        offset = len(start_pattern)
        if start_rfind is False:
            start_idx = instr.index(start_pattern) + offset
        else:
            start_idx = instr.rindex(start_pattern) + offset

        if end_rfind is False:
            end_idx = instr.index(end_pattern, start_idx)
        else:
            end_idx = instr.rindex(end_pattern, start_idx)

        return instr[start_idx:end_idx]

    def get_url(self, l):
        core = self._find_between('[', ']', l)
        # Just assume solr, that's all that is handled right now:
        # webapp = self._find_between('webapp=/', ' ', l)
        path = self._find_between('path=/', ' ', l)
        params = self._find_between('{', '}', l, end_rfind=True)
        if self.replaceterm is not None and self.replacewith is not None:
            params = params.replace(self.replaceterm, self.replacewith, 1)
        url = '{0}/{1}?{2}'.format(core, path, params)
        return url

    def get_qt(self, l):
        #return l[l.rfind('QTime=')+6:l.rfind(' ')] #if not last
        return int(l[l.rfind('QTime=') + 6:].strip()) #if last

    #def replace(self, l):
    # Now included in get_url
    #    url = self.get_url(l)
    #    if self.replaceterm is not None and self.replacewith is not None and self.replaceterm in url:
    #        print 'replacing: ' + self.replaceterm + ' with: ' + self.replacewith
    #        return url.replace(self.replaceterm, self.replacewith, 1)
    #    return url

if __name__ == "__main__":
    p = LogParser(host=HOST, port=PORT, authkey=AUTHKEY, delmult=DELAY_MULT, replaceterm=REPLACE_TERM,
                  replacewith=REPLACE_WITH, delinprod=DELAY_IN_PRODUCER)
    p.run()
