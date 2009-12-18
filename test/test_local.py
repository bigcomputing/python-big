#!/usr/bin/env python

from nwss.local import NwsLocalServer
from nws.client import NetWorkSpace
from nws.sleigh import Sleigh
from math import sqrt

# start the local nws server
srv = NwsLocalServer(daemon=False)
srv.wait_until_started()

# try an nws example
ws = NetWorkSpace('foo', serverPort=srv.port)
print ws
for i in range(10):
    ws.store('x', 'Value %d' % i)
print list(ws.ifetchTry('x'))

# try a sleigh example
s = Sleigh(nwsPort=srv.port)
print s
for i in xrange(10):
    print s.eachElem(sqrt, range(10))

# shutdown the local nws server
print 'shutting it down'
srv.shutdown()

assert not srv.isAlive()

try:
    ws.store('y', 1)
    print 'did not get expected exception'
except:
    print 'got expected exception'

# This hangs on shutdown
if False:
    # start the local nws server
    print 'starting another server'
    srv = NwsLocalServer(daemon=False)
    print 'waiting for it to start'
    srv.waitUntilStarted()

    # a quick test
    ws = NetWorkSpace('bar', serverPort=srv.port)
    ws.store('x', 'hello')
    print ws.listVars()

    # shutdown the local nws server
    print 'shutting it down, too'
    srv.shutdown()
    print 'finished'
