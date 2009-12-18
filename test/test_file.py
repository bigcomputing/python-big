#!/usr/bin/env python

import unittest
from itertools import izip, count
from StringIO import StringIO
from nws.client import *

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

class FileTest(unittest.TestCase):
    log = getLogger('FileTest')
    wsname = 'FileTest'

    def setUp(self):
        self.log.info('Setting up')
        self.ws = NetWorkSpace(self.wsname, **nwsargs)

    def tearDown(self):
        self.log.info('Tearing down')
        try: self.ws.server.close()
        except: pass

    def testReadFile(self):
        """Test reading the values of a variable using a read mode file."""
        for i in range(10):
            self.ws.store('TestReadFile', "%d\n" % i)

        for i, x in izip(count(), self.ws.makefile('TestReadFile')):
            self.assertEqual("%d\n" % i, x)

    def testWriteAndRead(self):
        """Test the line splitting of the write method."""
        n = 10
        s = StringIO()
        for i in xrange(n):
            s.write("%d\n" % i)

        w = self.ws.makefile('TestWriteAndRead', 'w')
        exp = s.getvalue()
        w.write(exp)
        w.close()

        for i, x in izip(count(), self.ws.makefile('TestWriteAndRead', 'r')):
            self.assertEqual("%d\n" % i, x)

        act = self.ws.makefile('TestWriteAndRead', 'r').read()
        self.assertEqual(act, exp)

    def testReadline(self):
        """Test the readline method."""
        n = 10
        s = StringIO()
        for i in xrange(n):
            s.write("%d\n" % i)

        w = self.ws.makefile('TestReadline', 'w')
        exp = s.getvalue()
        w.write(exp)
        w.close()

        r = self.ws.makefile('TestReadline', 'r')
        for i in xrange(n):
            x = r.readline()
            self.assertEqual("%d\n" % i, x)
        r.close()

        act = self.ws.makefile('TestReadline', 'r').read()
        self.assertEqual(act, exp)

    def testReadlines(self):
        """Test the readlines method."""
        w = self.ws.makefile('TestReadlines', 'w')
        s = "This is a test"
        for c in s:
            w.write(c)
        # test that nothing is there because no newline and we didn't flush or close
        r1 = self.ws.makefile('TestReadlines')
        self.assertEqual(len(r1.readlines()), 0)
        w.close()
        # test that we still read nothing because we hit EOF
        self.assertEqual(len(r1.readlines()), 0)
        r2 = self.ws.makefile('TestReadlines')
        x = r2.readlines()
        self.assert_(len(x) == 1 and x[0] == s)

        w = self.ws.makefile('TestReadlines', 'w')
        exp = ['This is a test\n', 'that we get three\n', 'lines when we call readline\n']
        s = ''.join(exp) + 'some extra junk that is buffered but not written'
        for c in s:
            w.write(c)
        act = self.ws.makefile('TestReadlines').readlines()
        self.assertEqual(act, exp)

    def testExceptions(self):
        """Test various exceptions."""
        w = self.ws.makefile('TestExceptions', 'w')
        self.assertRaises(ValueError, iter, w)

        w.close()
        self.assertRaises(ValueError, w.read)

        self.ws.store('TestExceptions', 1)
        r = self.ws.makefile('TestExceptions')
        r.read(0)  # this doesn't read anything
        self.assertRaises(ValueError, r.read, 1)

    def testFlush(self):
        """Test the flush method."""
        w = self.ws.makefile('TestFlush', 'w')
        w.write("hello,")
        self.assertEqual([], list(self.ws.ifindTry('TestFlush')))
        w.flush()
        self.assertEqual(['hello,'], list(self.ws.ifindTry('TestFlush')))
        w.write(" world")
        self.assertEqual(['hello,'], list(self.ws.ifindTry('TestFlush')))
        w.write('\n')
        self.assertEqual(['hello,', ' world\n'],
                list(self.ws.ifindTry('TestFlush')))
        r = self.ws.makefile('TestFlush')
        self.assertEqual('hello, world\n', r.read())

    def testSmallReads(self):
        """Test reads of one byte at a time."""
        w = self.ws.makefile('TestSmallReads', 'w')
        s = 'abcdefg\n'
        w.write(s)
        r =  self.ws.makefile('TestSmallReads')
        for c in s:
            self.assertEqual(c, r.read(1))

    def testSmallWrites(self):
        """Test writing one byte at a time."""
        w = self.ws.makefile('TestSmallReads', 'w')
        s = 'abcdefg\n'
        for c in s:
            w.write(c)

        r =  self.ws.makefile('TestSmallReads')
        self.assertEqual(s, r.read())

    def testAttributes(self):
        """Test that various attributes are set correctly."""
        w = self.ws.makefile('TestAttributes', 'r')
        self.assertEqual(w.mode, 'r')
        self.assert_(not w.isatty())
        self.assertEqual(w.name, 'TestAttributes')
        self.assert_(not w.closed)
        w.close()
        self.assert_(w.closed)

        w = self.ws.makefile('TestAttributes', 'w')
        self.assertEqual(w.mode, 'w')
        self.assert_(not w.isatty())
        self.assertEqual(w.name, 'TestAttributes')
        self.assert_(not w.closed)
        w.close()
        self.assert_(w.closed)

if __name__ == '__main__':
    import sys, os

    nwsargs = {}
    try: nwsargs['serverHost'] = os.environ['NWS_HOST']
    except: pass
    try: nwsargs['serverPort'] = int(os.environ['NWS_PORT'])
    except: pass
    verbose = os.environ.get('NWS_VERBOSE', False)

    basicConfig()
    log = getLogger('LongTest')
    if verbose:
        log.setLevel(DEBUG)

    unittest.main()
