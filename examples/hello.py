#!/usr/bin/env python

import sys, socket

host = "localhost"
port = 8765
wsname = "hello"

try:
    from nws.client import NetWorkSpace
    nws = NetWorkSpace(wsname, host, port)

    count = int(raw_input("Number of times to loop? "))
    print "hello: iterations:", count
    nws.store("hello example", count)

    for i in range(count):
        nws.store("hello", i)
        j = nws.fetch("hello")

    nws.fetch("hello example")
    print "Success"

except KeyboardInterrupt:
    print "hello exiting"
except ImportError:
    print "Unable to import the nwsClient module"
    print "You may need to set or correct your PYTHONPATH"
except socket.error:
    print "NWS server not running on %s, port %d" % (host, port)
except:
    print "An unexpected error occurred:", sys.exc_info()[1]
