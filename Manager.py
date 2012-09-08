#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Manage the queues
"""

from multiprocessing.managers import BaseManager
from multiprocessing import Queue
from threading import Timer
from Config import HOST, PORT, AUTHKEY, BASE_OUT_FILE
import contextlib
import time
from datetime import datetime
import sys
import Consumer

__author__ = 'Aaron Daubman <adaubman@echonest.com>'
__date__ = '9/7/12 3:26 PM'

class Manager():
    def __init__(self, host=None, port=None, authkey=None, filepfx=BASE_OUT_FILE):
        self.filepfx = filepfx
        self.work_queue = Queue()
        self.result_queue = Queue()
        BaseManager.register('get_work_queue', callable=lambda: self.work_queue)
        BaseManager.register('get_result_queue', callable=lambda: self.result_queue)
        self.m = BaseManager(address=(host, port), authkey=authkey)
        #self.s = self.m.get_server()
        self.ps = Timer(10, self.print_stats)
        self.ps.start()
        self.m.start()
        #TODO: figure out how to accept and break for KeyboardInterrupt so that ^C can actually kill this

        with contextlib.closing(open(self.filepfx + datetime.now().strftime('%Y-%m-%d-%H%M%S') + '_log.csv', 'w+b', 1)) as f:
            print "Writing results to: " + f.name
            #taken, qt, nf, sz, oqt
            f.write('TimeStamp,FullTime,QTime,numFound,ResponseLen,OrigQTime\n')
            while 1:
                try:
                    res = self.result_queue.get()
                    f.write(','.join(map(str, res)) + '\n')
                    time.sleep(0.1)
                except KeyboardInterrupt:
                    print "^C detected, attempting to exit..."
                    self.ps.cancel()
                    self.work_queue.close()
                    self.result_queue.close()
                    self.m.shutdown()
                    sys.exit()

    def print_stats(self):
        print 'Is Work queue full: ' + ('yes' if self.work_queue.full() else 'no') + ', empty: ' + ('yes' if self.work_queue.empty() else 'no')
        print 'Is Result queue full: ' + ('yes' if self.result_queue.full() else 'no') + ', empty: ' + ('yes' if self.result_queue.empty() else 'no')
        self.ps = Timer(10, self.print_stats)
        self.ps.start()


if __name__ == "__main__":
    print HOST + ", " + str(PORT) + ", " + AUTHKEY
    m = Manager(host=HOST, port=PORT, authkey=AUTHKEY, filepfx=BASE_OUT_FILE)
    #p = Producer.Producer()
    c = Consumer.Consumer()