#!/usr/bin/env python

import sys, getopt, time, math, socket, random
from nws.sleigh import Sleigh, sshcmd, rshcmd, scriptcmd
from nws.client import SINGLE

defaultNodeList = [socket.gethostname] * 3
try:
    import win32api
    defaultNodeList = [win32api.GetUserName() + '@' + socket.gethostname] * 3
except:
    pass

# define the function to execute
def worker(x):
    time.sleep(abs(random.gauss(mu, 2.0)))
    return math.exp(x)

# used by worker function
mu = abs(random.gauss(10.0, 4.0))

if __name__ == '__main__':
    numtasks = 10
    description = 'Simple Sleigh Demonstration'
    enableNwsUtility = False
    skw = {}
    eo = {}

    try:
        opts, args = getopt.getopt(sys.argv[1:], 'h:p:n:d:f:m:r:c:sve')

        for opt, arg in opts:
            if opt == '-h':
                skw['nwsHost'] = arg
            elif opt == '-p':
                skw['nwsPort'] = int(arg)
            elif opt == '-m':
                skw['modulePath'] = arg
            elif opt == '-n':
                numtasks = int(arg)
            elif opt == '-d':
                description = arg
            elif opt == '-f':
                eo['loadFactor'] = int(arg)
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
                skw['scriptExec'] = scriptcmd
                skw['scriptName'] = 'PythonNWSSleighWorker.py'
            elif opt == '-v':
                skw['verbose'] = 1
            elif opt == '-e':
                enableNwsUtility = True
            elif opt == '-c':
                skw['workerCount'] = int(arg)
            else:
                raise 'internal error: out-of-sync with getopt'

        # if no nodes are specified, start three on the local machine
        if len(args) > 0:
            skw['nodeList'] = args
        elif skw.get('launch') == 'service':
            skw['nodeList'] = defaultNodeList
            print "warning: using default nodeList:", defaultNodeList

        # create a sleigh on the specified nodes
        s = Sleigh(**skw)

        if enableNwsUtility:
            s.nws.declare('enableNwsUtility', SINGLE)
        s.nws.declare('MainTitle', SINGLE)
        s.nws.store('MainTitle', description)
        s.nws.declare('SubTitle', SINGLE)

        submitted = s.workerCount

        # do the work
        for i in xrange(numtasks):
            submitted += numtasks
            s.nws.store('SubTitle', '%d tasks submitted' % submitted)
            s.eachElem(worker, range(numtasks), **eo)

        raw_input('Hit return to exit sleigh: ')
        s.stop()
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
