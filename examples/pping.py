#!/usr/bin/env python

import sys, getopt, time, socket
from nws.sleigh import sshcmd, rshcmd
from nws.sleigh import Sleigh

defaultNodeList = [socket.getfqdn()] * 3
try:
    import win32api
    defaultNodeList = [win32api.GetUserName() + '@' + socket.gethostname()] * 3
except:
    pass

# this is the master function
def ping(ws, totaltasks):
    for i in xrange(totaltasks):
        r = ws.fetch('ping')
        assert len(r) == 2
        ws.store(r[0], r[1])

# this is the worker function
def pong(numtasks, size):
    pong = "pong_%d" % SleighRank
    payload = "#" * size

    for i in xrange(numtasks):
        SleighUserNws.store('ping', [pong, payload])
        j = SleighUserNws.fetch(pong)

if __name__ == '__main__':
    numtasks = 10
    timeout = 10
    size = 10
    nkw = {}
    skw = {}

    try:
        opts, args = getopt.getopt(sys.argv[1:], 'h:p:n:m:r:s:t:c:u:v')

        for opt, arg in opts:
            if opt == '-h':
                nkw['host'] = skw['nwsHost'] = arg
            elif opt == '-p':
                nkw['port'] = skw['nwsPort'] = int(arg)
            elif opt == '-m':
                skw['modulePath'] = arg
            elif opt == '-n':
                numtasks = int(arg)
            elif opt == '-r':
                if arg == 'ssh':
                    skw['launch'] = sshcmd
                elif arg == 'rsh':
                    skw['launch'] = rshcmd
                elif arg in ['service']:
                    skw['launch'] = arg
                else:
                    print >> sys.stderr, \
                            "option -r takes a value of 'ssh', 'rsh' or 'service'"
                    sys.exit(1)
            elif opt == '-s':
                size = int(arg)
            elif opt == '-t':
                timeout = int(arg)
            elif opt == '-v':
                skw['verbose'] = 1
            elif opt == '-c':
                skw['workerCount'] = int(arg)
            elif opt == '-u':
                skw['user'] = arg
            else:
                raise 'internal error: out-of-sync with getopt'

        if skw.get('launch') and skw.get('workerCount'):
            print >> sys.stderr, 'warning: workerCount is ignored when not using local launch'

        if len(args) > 0:
            if skw.get('launch'):
                skw['nodeList'] = args
            else:
                print >> sys.stderr, \
                        'warning: ignoring unused arguments:', " ".join(args)
        elif skw.get('launch') == 'service':
            skw['nodeList'] = defaultNodeList
            print "warning: using default nodeList:", defaultNodeList

        # create a Sleigh
        s = Sleigh(**skw)

        numworkers, complete = s.status(True, timeout)

        if numworkers == 0:
            print >> sys.stderr, "no workers successfully started " \
                    "within %d seconds" % timeout
            sys.exit(1)

        # tell the workers to execute the ring function defined in this file
        s.eachWorker(pong, numtasks, size, blocking=False)

        totaltasks = numworkers * numtasks
        start = time.time()
        ping(s.userNws, totaltasks)
        totaltime = time.time() - start
        print "Seconds per operation:", totaltime / (4 * totaltasks)
        print "Payload size is approximately %d bytes" % size

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
