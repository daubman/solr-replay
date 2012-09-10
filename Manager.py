#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Manage the queues, start up consumers on same host if desired
"""

from multiprocessing.managers import BaseManager
from multiprocessing import Queue, Process
from threading import Timer
import threading
from Config import HOST, PORT, AUTHKEY, BASE_OUT_FILE, MANAGER_STARTS_REQGENS, STAT_INTERVAL, BASE_URL, DELAY_IN_PRODUCER, DELAY_MULT
import contextlib
import time
from datetime import datetime
import RequestGenerator

__author__ = 'Aaron Daubman <adaubman@echonest.com>'
__date__ = '9/7/12 3:26 PM'

class Manager():
    def __init__(self, host=None, port=None, authkey=None, filepfx=BASE_OUT_FILE, statint=0, delmult=1, delinprod=True):
        self.delmult = delmult
        self.delinprod = delinprod
        self.filepfx = filepfx
        self.statint = statint
        self.log_queue = Queue()
        self.work_queue = Queue()
        self.result_queue = Queue()
        #self.work_queue.cancel_join_thread()
        #self.result_queue.cancel_join_thread()
        BaseManager.register('get_log_queue', callable=lambda: self.log_queue)
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
        rt = threading.Thread(target=self.result_writer_thread, args=(self,))
        lt = threading.Thread(target=self.log_to_work_delay_thread, args=(self,))
        lt.start()
        rt.start()
        rt.join()
        lt.join()

    def result_writer_thread(self):
        with contextlib.closing(open(self.filepfx + datetime.now().strftime('%Y-%m-%d-%H%M%S') + '_log.csv', 'w+b', 1)) as f:
            print 'Writing results to: ' + f.name
            #taken, qt, nf, sz, oqt
            f.write('TimeStamp,FullTime,QTime,numFound,ResponseLen,OrigQTime\n')
            while self.running:
                try:
                    res = self.result_queue.get()
                    f.write(','.join(map(str, res)) + '\n')
                except KeyboardInterrupt:
                    #TODO: no idea how to prevent os.waitpid related exceptions...
                    print '\nKeyboardInterrupt detected in Manager (rwt), attempting to exit...'
                    #sys.exc_clear()
                    self.stop()
                    return

    def log_to_work_delay_thread(self):
        while self.running:
            try:
                delay, query, oqt = self.log_queue.get()
                if not self.delinprod:
                    #Delay here if multiple producers
                    if delay > 2: #safeguard with a max delay of 2 seconds...
                        delay = 2
                    time.sleep(self.delmult * delay)
                self.work_queue.put((query,oqt))
            except KeyboardInterrupt:
                #TODO: no idea how to prevent os.waitpid related exceptions...
                print '\nKeyboardInterrupt detected in Manager (ltwt), attempting to exit...'
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
        m = Manager(host=HOST, port=PORT, authkey=AUTHKEY, filepfx=BASE_OUT_FILE, statint=STAT_INTERVAL, delmult=DELAY_MULT, delinprod=DELAY_IN_PRODUCER)
        p.append(Process(target=m.run, name='Manager'))
        p[0].start()
        if MANAGER_STARTS_REQGENS > 0:
            c = []
            print 'Initializing: ' + str(MANAGER_STARTS_REQGENS) + (' Consumers' if MANAGER_STARTS_REQGENS > 1 else ' RequestGenerator')
            for i in xrange(MANAGER_STARTS_REQGENS):
                c.append(RequestGenerator.RequestGenerator(host=HOST, port=PORT, authkey=AUTHKEY, baseurl=BASE_URL, name=str(i), wq=m.get_wq(), rq=m.get_rq(), delinprod=DELAY_IN_PRODUCER))
                p.append(Process(target=c[i].run, name='RequestGenerator ' + str(i)))
            print 'Starting: ' + str(MANAGER_STARTS_REQGENS) + (' RequestGenerators' if MANAGER_STARTS_REQGENS > 1 else ' RequestGenerator')
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

