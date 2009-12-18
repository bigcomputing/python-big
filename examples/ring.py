#!/usr/bin/env python

import sys, getopt, time
from nws.sleigh import sshcmd
from nws.sleigh import Sleigh

# This is the "worker" function.  Each worker waits for the token to
# arrive in his variable.  When it does, he increments it by one, and
# puts it in the next workers variable.  After he's done that "numtask"
# times, he quit.
def ring(numworkers, numtasks):
    mine = "worker_%d" % SleighRank
    next = (SleighRank + 1) % numworkers
    his = "worker_%d" % next

    for i in xrange(numtasks):
        j = SleighUserNws.fetch(mine)
        SleighUserNws.store(his, j + 1)

if __name__ == '__main__':
    numtasks = 10
    timeout = 10
    nkw = {}
    skw = {}

    try:
        opts, args = getopt.getopt(sys.argv[1:], 'h:p:n:t:c:wv')

        for opt, arg in opts:
            if opt == '-h':
                nkw['host'] = skw['nwsHost'] = arg
            elif opt == '-p':
                nkw['port'] = skw['nwsPort'] = int(arg)
            elif opt == '-n':
                numtasks = int(arg)
            elif opt == '-t':
                timeout = int(arg)
            elif opt == '-c':
                skw['workerCount'] = int(arg)
            elif opt == '-w':
                skw['launch'] = 'web'
            elif opt == '-v':
                skw['verbose'] = 1
            else:
                raise 'internal error: out-of-sync with getopt'

        if not skw.get('launch'):
            if len(args) > 0:
                skw['launch'] = sshcmd
                skw['nodeList'] = args
                if skw.has_key('workerCount'):
                    print >> sys.stderr, 'warning: ignoring -c option since workers are specified'
        else:
            if len(args) > 0:
                print >> sys.stderr, \
                        'warning: ignoring unused arguments:', " ".join(args)
            print >> sys.stderr, 'starting sleigh in web launch mode'

        # create a Sleigh, and compute the number of workers.
        # this is necessary because of web launch.
        s = Sleigh(**skw)
        numworkers, complete = s.status(True, timeout)

        # include the master as one of the workers (which works even
        # when no workers join)
        numworkers += 1

        # tell the workers to execute the ring function defined in this file
        s.eachWorker(ring, numworkers, numtasks, blocking=False)

        # the master becomes the last worker
        global SleighRank, SleighUserNws
        SleighRank = numworkers - 1
        SleighUserNws = s.userNws

        print "Master assigned rank %d" % SleighRank

        # time how long it takes the token to go all the way around the
        # ring numtask times.
        start = time.time()
        s.userNws.store('worker_0', 0)
        ring(numworkers, numtasks)
        finish = time.time()
        token = s.userNws.fetch('worker_0')
        assert token == numtasks * numworkers

        print "The token was passed %d times in %f" % (token, finish - start)
        print "Seconds per operation:", (finish - start) / (2 * token)

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
