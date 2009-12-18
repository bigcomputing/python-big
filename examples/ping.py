#!/usr/bin/env python

import sys, socket, time

host = "localhost"
port = 8765
wsname = "ping-pong"

if len(sys.argv) == 2:
    wsname = sys.argv[1]
elif len(sys.argv) != 1:
    print >> sys.stderr, "Usage: %s [<wsname>]" % sys.argv[0]
    sys.exit(1)

try:
    from nws.client import NetWorkSpace
    nws = NetWorkSpace(wsname, host, port)
    nws.store("game", 0)

    print "Ping-pong server '%s' starting" % wsname
    while 1:
        request = nws.fetch("ping")
        pong = request['replyto']
        request['time'] = time.asctime()
        print "Got a ping from", pong
        nws.store(pong, request)

except KeyboardInterrupt:
    print "Ping-pong server '%s' exiting" % wsname
except ImportError:
    print "Unable to import the nwsClient module"
    print "You may need to set or correct your PYTHONPATH"
except socket.error:
    print "NWS server not running on %s, port %d" % (host, port)
except:
    print "An unexpected error occurred:", sys.exc_info()[1]
