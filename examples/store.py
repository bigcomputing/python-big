#!/usr/bin/env python

import sys, getopt, time
from nws.sleigh import Sleigh, sshcmd

# this is the worker function
def store(numtasks):
    for i in xrange(numtasks):
        SleighUserNws.store('store', str(SleighRank) + ':' + str(i))

if __name__ == '__main__':
    numtasks = 1000
    nkw = {}
    skw = {}

    try:
        opts, args = getopt.getopt(sys.argv[1:], 'h:p:n:c:v')

        for opt, arg in opts:
            if opt == '-h':
                nkw['host'] = skw['nwsHost'] = arg
            elif opt == '-p':
                nkw['port'] = skw['nwsPort'] = int(arg)
            elif opt == '-n':
                numtasks = int(arg)
            elif opt == '-c':
                skw['workerCount'] = int(arg)
            elif opt == '-v':
                skw['verbose'] = 1
            else:
                raise 'internal error: out-of-sync with getopt'

        if len(args) > 0:
            skw['launch'] = sshcmd
            skw['nodeList'] = args
            if skw.has_key('workerCount'):
                print >> sys.stderr, 'warning: ignoring -c option since workers are specified'

        # create a Sleigh, and compute the number of workers.
        # this is necessary because of web launch.
        s = Sleigh(**skw)
	s.userNws.declare('store', 'single')

	print "Running with %d workers and %d tasks" % \
	        (s.workerCount, numtasks)

        # tell the workers to execute the store function defined in this file
        s.eachWorker(store, numtasks)

        print "Finished"
    except getopt.GetoptError, e:
        print >> sys.stderr, e.msg
        sys.exit(1)
    except ValueError, e:
        print >> sys.stderr, "option %s requires an integer argument" % opt
        sys.exit(1)
    except (KeyboardInterrupt, SystemExit):
        pass
    except:
        ex = sys.exc_info()
        print >> sys.stderr, ex[0], ex[1]
