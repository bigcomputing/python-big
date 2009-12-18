#!/usr/bin/env python

import sys, socket

host = "localhost"
port = 8765
wsname = "ping-pong"
loops = 10

if len(sys.argv) in [2, 3]:
    try:
        loops = int(sys.argv[1])
    except:
        print >> sys.stderr, "Usage: %s [<loops> [<wsname>]]" % sys.argv[0]
        sys.exit(1)

    if len(sys.argv) == 3:
        wsname = sys.argv[2]
elif len(sys.argv) != 1:
    print >> sys.stderr, "Usage: %s [<loops> [<wsname>]]" % sys.argv[0]
    sys.exit(1)

try:
    from nws.client import NetWorkSpace
    nws = NetWorkSpace(wsname, host, port, useUse=True)
    game = nws.fetch("game")
    nws.store("game", game + 1)
    pong = "pong_%d" % game
    request = {'replyto': pong}

    print "Starting a ping-pong game", game
    for i in xrange(loops):
        request['i'] = i
        nws.store("ping", request)
        reply = nws.fetch(pong)
        print reply
        if i != reply['i']:
            print "Error: expected %d, received %d" % (i, reply['i'])

    nws.deleteVar(pong)

except KeyboardInterrupt:
    print "pong exiting"
except ImportError:
    print "Unable to import the nwsClient module"
    print "You may need to set or correct your PYTHONPATH"
except socket.error:
    print "NWS server not running on %s, port %d" % (host, port)
except:
    print "An unexpected error occurred:", sys.exc_info()[1]
