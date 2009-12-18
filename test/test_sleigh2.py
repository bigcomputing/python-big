#!/usr/bin/env python

import unittest, time, string
from nws.client import SINGLE
from nws.sleigh import Sleigh, sshcmd
from random import randint
from math import sqrt
from itertools import repeat, imap, starmap, cycle
from operator import mul

try:
    from logging import getLogger, basicConfig, DEBUG
except ImportError:
    DEBUG = 1
    class Logger:
        def __init__(self):
            self._level = 0

        def info(self, msg):
            if self._level:
                print msg

        def setLevel(self, level):
            self._level = level

    def getLogger(ign):
        return Logger()

    def basicConfig():
        pass

def broadcastWorker(n):
    for i in xrange(n):
        SleighNws.store('broadcast', randint(1, 1000))

    workers = SleighNws.fetch('workers') - 1
    SleighNws.store('workers', workers)
    if workers == 0:
        SleighNws.store('broadcast', 0)

    total = 0
    for t in SleighNws.ifind('broadcast'):
        if t == 0: break
        total += t

    return total

def monitorWorker():
    return list(SleighNws.ifind('monitor'))

def nothing(n):
    return n

class SleighTest(unittest.TestCase):
    log = getLogger('SleighTest')
    wsname = 'unit test ws'

    def setUp(self):
        self.log.info('Setting up')
        self.s = Sleigh(**slargs)

    def tearDown(self):
        self.log.info('Tearing down')
        try: self.s.stop()
        except: pass

    def testMonitor(self):
        """Test one-to-all broadcast with a single mode variable"""
        for n, s in [(1000, 0.0), (100, 0.1), (10, 1.0)]:
            pending = self.s.eachWorker(monitorWorker, blocking=False)
            time.sleep(3) # not necessary for test to pass
            self.s.nws.declare('monitor', SINGLE)
            for i in xrange(n):
                self.s.nws.store('monitor', i)
                time.sleep(s) # not necessary for test to pass
            time.sleep(3) # not necessary for test to pass
            self.s.nws.deleteVar('monitor')
            results = pending.wait()
            self.assertEquals(len(results), self.s.workerCount)
            for r in results:
                self.assert_(len(r) <= n)
                if len(r) > 0:
                    p = r.pop(0)
                    for t in r:
                        self.assert_(t > p)
                        p = t
                else:
                    print 'WARNING: got zero length result'

    def testString(self):
        """Test when elementArgs is a string"""
        s = "Hello, world"
        act = self.s.eachElem(ord, s)
        exp = map(ord, s)
        self.assertEquals(act, exp)

    def testListOfString(self):
        """Test when elementArgs is a list containing a string"""
        s = "This is another string."
        act = self.s.eachElem(ord, [s])
        exp = map(ord, s)
        self.assertEquals(act, exp)

    def testIterables(self):
        """Test when elementArgs is a list of unequal length iterables"""
        act = self.s.eachElem(cmp, [xrange(10), xrange(20)])
        exp = [0] * 10
        self.assertEquals(act, exp)

    def testDefaultElementArgs(self):
        """Test the default value of elementArgs"""
        act = self.s.eachElem(cmp)
        exp = []
        self.assertEquals(act, exp)

    def testElementArgsIFind(self):
        """Test using ifindTry to specify elementArgs for eachElem"""
        n = 10
        for i in xrange(n):
            self.s.userNws.store('i', i)

        exp = map(lambda x: x + x, xrange(n))
        for i in xrange(3):
            act = self.s.eachElem(lambda x: x + x, self.s.userNws.ifindTry('i'))
            self.assertEqual(act, exp)

    def testElementArgsStoreFile(self):
        """Test the use of storeFile with an elementArgs ifindTry"""
        f = open("test_sleigh2.py", "rb")
        try:
            # write values of up to 100 bytes until EOF
            while self.s.userNws.storeFile('test_sleigh2.py', f, n=100):
                pass
        finally:
            f.close()

        exp = string.upper(open("test_sleigh2.py", "rb").read())
        for i in xrange(3):
            act = "".join(self.s.eachElem(string.upper, self.s.userNws.ifindTry('test_sleigh2.py')))
            self.assertEqual(act, exp)

    def testAccumulator(self):
        """Test the use of the accumulator argument to eachElem"""
        exp = string.uppercase

        # do it the standard way
        act = "".join(self.s.eachElem(string.upper, string.lowercase))
        self.assertEqual(act, exp)

        # do it with an accumulator
        accum = Accum_1()
        self.s.eachElem(string.upper, string.lowercase, accumulator=accum.setresults)
        act = "".join(accum.getresults())
        self.assertEqual(act, exp)

    def testEachWorkerAccum(self):
        """Test the use of the accumulator argument to eachWorker"""
        n = 42
        exp = [n] * self.s.workerCount
        accum = Accum_1()
        self.s.eachWorker(nothing, n, accumulator=accum.setresults)
        act = accum.getresults()
        self.assertEqual(act, exp)

    def testNewStyleAccum(self):
        """Test the new-style accumulators"""
        exp = sum(xrange(300))
        accum = Accum_2()
        act = self.s.eachElem(sum,
                [[range(100), range(100, 200), range(200, 300)]],
                accumulator=accum)
        self.assertEqual(act, exp)

    def testImap(self):
        """Test imap method"""
        self.assertEqual(list(self.s.imap(sqrt, xrange(10))), map(sqrt, xrange(10)))
        self.assertEqual(list(self.s.imap(mul, xrange(10), repeat(6))),
                         list(imap(mul, xrange(10), repeat(6))))
        axpy = lambda a, x, y: a * x + y
        self.assertEqual(list(self.s.imap(axpy, repeat(10), xrange(100), cycle(xrange(10)))),
                         list(imap(axpy, repeat(10), xrange(100), cycle(xrange(10)))))

    def testStarmap(self):
        """Test starmap method"""
        def gen(a, x, y):
            x = iter(x)
            y = iter(y)
            try:
                while True:
                    yield (a, x.next(), y.next())
            except StopIteration:
                pass

        axpy = lambda a, x, y: a * x + y
        it = gen(10, xrange(100), cycle(xrange(10)))
        self.assertEqual(list(self.s.starmap(axpy, it)),
                         list(imap(axpy, repeat(10), xrange(100), cycle(xrange(10)))))

# general purpose, but useless, accumulator class
class Accum_1:
    def __init__(self):
        self.results = {}

    def setresults(self, results):
        assert len(results) == 1
        tag = results[0][0]
        val = results[0][1]
        assert not self.results.has_key(tag)
        self.results[tag] = val

    def getresults(self):
        return [self.results[i] for i in xrange(len(self.results))]

# a "new-style" accumulator class that does result reduction
class Accum_2:
    def __init__(self):
        self._sum = 0

    def result(self, results):
        self._sum += sum([r[1] for r in results])

    def value(self):
        return self._sum

if __name__ == '__main__':
    import sys, os

    slargs = {}
    try: slargs['nwsHost'] = os.environ['NWS_HOST']
    except: pass
    try: slargs['nwsPort'] = int(os.environ['NWS_PORT'])
    except: pass
    slargs['verbose'] = verbose = os.environ.get('NWS_VERBOSE', False)

    try:
        slargs['nodeList'] = os.environ['NWS_NODES'].split()
        slargs['launch'] = sshcmd
        print "using ssh to launch workers on machines:", slargs['nodeList']
    except:
        print "using default local workers"

    basicConfig()
    log = getLogger('SleighTest')
    if verbose:
        log.setLevel(DEBUG)

    unittest.main()
