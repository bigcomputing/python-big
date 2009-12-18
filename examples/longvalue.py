#!/usr/bin/env python

import sys, os, getopt, time, traceback
from nws.sleigh import Sleigh, sshcmd

# this is the worker function
def longvalue(numtasks, fname):
    status = []
    print >> sys.stderr, "longvalue function starting"

    try:
        bname = os.path.basename(fname) + '_' + str(SleighRank)
        for i in xrange(numtasks):
            fobj = open(bname, 'wb')
            SleighUserNws.findFile('longvalue', fobj)
            fobj.close()
            s = os.system("cmp -s %s %s" % (fname, bname)) # XXX Unix specific
            if s: status.append(s)
            os.remove(bname)
    except:
        status = str(sys.exc_info()[1])
        traceback.print_exc()

    return status

if __name__ == '__main__':
    fname = None
    numtasks = 10
    nkw = {}
    skw = {}

    try:
        opts, args = getopt.getopt(sys.argv[1:], 'h:p:n:c:f:v')

        for opt, arg in opts:
            if opt == '-h':
                nkw['host'] = skw['nwsHost'] = arg
            elif opt == '-p':
                nkw['port'] = skw['nwsPort'] = int(arg)
            elif opt == '-n':
                numtasks = int(arg)
            elif opt == '-c':
                skw['workerCount'] = int(arg)
            elif opt == '-f':
                fname = arg
            elif opt == '-v':
                skw['verbose'] = 1
            else:
                raise 'internal error: out-of-sync with getopt'

        if not fname:
            print >> sys.stderr, 'error: must use -f option to specify a file'
            sys.exit(1)

        if len(args) > 0:
            skw['launch'] = sshcmd
            skw['nodeList'] = args
            if skw.has_key('workerCount'):
                print >> sys.stderr, 'warning: ignoring -c option since workers are specified'

        # create a Sleigh, and compute the number of workers.
        # this is necessary because of web launch.
        s = Sleigh(**skw)

        fobj = open(fname, 'rb')
        print "Storing %s to server..." % fname
        s.userNws.declare('longvalue', 'single')
        s.userNws.storeFile('longvalue', fobj)
        fobj.close()

        print "Running with %d workers and %d tasks" % \
                (s.workerCount, numtasks)

        # tell the workers to execute the longvalue function
        # defined in this file
        print s.eachWorker(longvalue, numtasks, fname)

        raw_input('Hit return to finish ')
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
