#!/usr/bin/env python

import unittest
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

class LongTest(unittest.TestCase):
    log = getLogger('NWSTest')
    wsname = 'unit test ws'

    def setUp(self):
        self.log.info('Setting up')
        self.ws = NetWorkSpace(self.wsname, **nwsargs)

    def tearDown(self):
        self.log.info('Tearing down')
        try: self.ws.server.close()
        except: pass

    def testLongValues(self):
        """Test sending lots of long values of the same name to the server."""
        self.log.info('Storing lots of long values (this will take awhile)')
        for i in xrange(10):
            for j in xrange(1000):
                self.ws.store('Long', range(10000))
            for j in xrange(1000):
                self.ws.fetch('Long')

    def testDifferentLongValues(self):
        """Test sending lots of long values of different names to the server."""
        self.log.info('Storing lots of different long values (this will take awhile)')
        for i in xrange(10):
            for j in xrange(1000):
                self.ws.store('Long %d' % j, range(10000))
            for j in xrange(1000):
                self.ws.fetch('Long %d' % j)

    def testDeleteVarLongValues(self):
        """Test if long values are cleaned up when you delete a variable"""
        for i in xrange(100):
            # consume a lot of file descriptors
            for j in xrange(1000):
                self.ws.store('Long', range(10000))
            self.ws.deleteVar('Long')

    def testDeleteWsLongValues(self):
        """Test if long values are cleaned up when you delete the workspace"""
        for i in xrange(100):
            twsname = self.ws.server.mktempWs("delws_%d")
            tws = self.ws.server.openWs(twsname)
            # consume a lot of file descriptors
            for j in xrange(1000):
                tws.store('Long', range(10000))
            tws.server.deleteWs(twsname)

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
