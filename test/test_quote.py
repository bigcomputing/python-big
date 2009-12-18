#!/usr/bin/env python

import unittest
from nws.util import msc_quote

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

class QuoteTest(unittest.TestCase):
    log = getLogger('QuoteTest')

    def testQuote(self):
        """Test Windows style quoting."""
        examples = [('a', 'a'),
                    (' ', '" "'),
                    ('\\', '\\'),
                    ('a"', '"a\\""'),
                    (' \\', '" \\\\"'),
                    (' \\"', '" \\\\\\""'),
                    ('', '')]
        for r, q in examples:
            self.assertEqual(msc_quote(r), q)

if __name__ == '__main__':
    import sys, os

    verbose = os.environ.get('NWS_VERBOSE', False)

    basicConfig()
    log = getLogger('QuoteTest')
    if verbose:
        log.setLevel(DEBUG)

    unittest.main()
