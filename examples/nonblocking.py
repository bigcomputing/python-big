#!/usr/bin/env python

import sys, getopt, traceback
from nws.sleigh import Sleigh, sshcmd

def worker():
    while 1:
        task = SleighUserNws.fetch('task')
        result = 2 * task
        SleighUserNws.store('result', (task, result))

if __name__ == '__main__':
    numtasks = 10
    nkw = {}
    skw = {}

    try:
        opts, args = getopt.getopt(sys.argv[1:], 'h:p:n:c:wv')

        for opt, arg in opts:
            if opt == '-h':
                nkw['host'] = skw['nwsHost'] = arg
            elif opt == '-p':
                nkw['port'] = skw['nwsPort'] = int(arg)
            elif opt == '-n':
                numtasks = int(arg)
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

        s = Sleigh(**skw)

        s.eachWorker(worker, blocking=False)

        for i in range(numtasks):
            s.userNws.store('task', i)

        for i in range(numtasks):
            result = s.userNws.fetch('result')
            print result[0], 'times 2 is', result[1]

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
