#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Manage the queues, start up consumers on same host if desired
"""

from multiprocessing.managers import BaseManager
from multiprocessing import Queue, Process
from threading import Timer
from Config import HOST, PORT, AUTHKEY, BASE_OUT_FILE, MANAGER_STARTS_CONSUMERS, STAT_INTERVAL, BASE_URL
import contextlib
import time
from datetime import datetime
import sys
import Consumer

__author__ = 'Aaron Daubman <adaubman@echonest.com>'
__date__ = '9/7/12 3:26 PM'

class Manager():
    def __init__(self, host=None, port=None, authkey=None, filepfx=BASE_OUT_FILE, statint=0):
        self.filepfx = filepfx
        self.statint = statint
        self.work_queue = Queue()
        self.result_queue = Queue()
        #self.work_queue.cancel_join_thread()
        #self.result_queue.cancel_join_thread()
        BaseManager.register('get_work_queue', callable=lambda: self.work_queue)
        BaseManager.register('get_result_queue', callable=lambda: self.result_queue)
        self.m = BaseManager(address=(host, port), authkey=authkey)
        print 'Initialzed: Manager\'s BaseManager(address=(' + host + ', ' + str(port) + '), authkey=' + authkey + ')'

    def run(self):
        self.m.start() #Need to start here or multiple Process issues silently close the socket
        self.running = True
        print 'Running Manager'
        if self.statint > 0:
            self.ps = Timer(self.statint, self.print_stats)
            self.ps.start()
        with contextlib.closing(open(self.filepfx + datetime.now().strftime('%Y-%m-%d-%H%M%S') + '_log.csv', 'w+b', 1)) as f:
            print 'Writing results to: ' + f.name
            #taken, qt, nf, sz, oqt
            f.write('TimeStamp,FullTime,QTime,numFound,ResponseLen,OrigQTime\n')
            while self.running:
                try:
                    res = self.result_queue.get()
                    f.write(','.join(map(str, res)) + '\n')
                    time.sleep(0.1)
                except KeyboardInterrupt:
                    #TODO: no idea how to prevent os.waitpid related exceptions...
                    print '\nKeyboardInterrupt detected in Manager, attempting to exit...'
                    #sys.exc_clear()
                    self.stop()
                    return

    def stop(self):
        self.running = False
        self.ps.cancel()
        self.work_queue.close()
        self.result_queue.close()
        self.m.shutdown()

    def print_stats(self):
        print 'Work queue full: ' + ('yes' if self.work_queue.full() else 'no') + ', empty: ' + ('yes' if self.work_queue.empty() else 'no') + ' / Result queue full: ' + (
            'yes' if self.result_queue.full() else 'no') + ', empty: ' + ('yes' if self.result_queue.empty() else 'no')
        self.ps = Timer(self.statint, self.print_stats)
        self.ps.start()

    def get_wq(self):
        return self.work_queue

    def get_rq(self):
        return self.result_queue


def main():
    p = []
    try:
        m = Manager(host=HOST, port=PORT, authkey=AUTHKEY, filepfx=BASE_OUT_FILE, statint=STAT_INTERVAL)
        p.append(Process(target=m.run, name='Manager'))
        p[0].start()
        if MANAGER_STARTS_CONSUMERS > 0:
            c = []
            print 'Initializing: ' + str(MANAGER_STARTS_CONSUMERS) + (' Consumers' if MANAGER_STARTS_CONSUMERS > 1 else ' Consumer')
            for i in xrange(MANAGER_STARTS_CONSUMERS):
                c.append(Consumer.Consumer(host=HOST, port=PORT, authkey=AUTHKEY, baseurl=BASE_URL, name=str(i), wq=m.get_wq(), rq=m.get_rq()))
                p.append(Process(target=c[i].run, name='Consumer ' + str(i)))
            print 'Starting: ' + str(MANAGER_STARTS_CONSUMERS) + (' Consumers' if MANAGER_STARTS_CONSUMERS > 1 else ' Consumer')
            for i in xrange(1, len(p)):
                p[i].start()
    except KeyboardInterrupt:
        #TODO: doesn't work, still get errors on ^C
        print 'Caught KeyboardInterrupt in Manager main...'
        for proc in reversed(p):
            print 'Attempting to terminate: ' + proc.name
            proc.terminate()


if __name__ == '__main__':
    main()

