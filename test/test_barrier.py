#!/usr/bin/env python

import os
from nws.sleigh import Sleigh, sshcmd
from socket import getfqdn

slargs = {}
try: slargs['nwsHost'] = os.environ['NWS_HOST']
except: pass
try: slargs['nwsPort'] = int(os.environ['NWS_PORT'])
except: pass

try:
    slargs['nodeList'] = os.environ['NWS_NODES'].split()
    slargs['launch'] = sshcmd
    print "using ssh to launch workers on machines:", slargs['nodeList']
except:
    print "using default local workers"

s = Sleigh(**slargs)

# use os.getloadavg if they have it (it's not in Python 2.2 or on Windows)
# because socket.getfqdn runs terribly slowly on Mac OS X
if hasattr(os, 'getloadavg'):
    for i in xrange(1000): print i, s.eachWorker(os.getloadavg)
else:
    for i in xrange(1000): print i, s.eachWorker(getfqdn)

s.stop()
