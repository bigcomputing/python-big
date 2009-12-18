#!/usr/bin/env python
#
# For LSF, I think you can execute this script directly,
# using bsub command:
#
#     % bsub -n 10 batchqueueing.py -v -h `hostname`
#
# For PBS, I believe you need to write a shell script that
# calls this script, using the proper annotations, and
# execute that script using the qsub command.
#

import sys, os, getopt
from nws.sleigh import Sleigh, sshcmd

def getNodeList():
    if os.environ.get('LSB_HOSTS'):
        print "Submitted via LSF"
        nodeList = os.environ['LSB_HOSTS'].split()
    elif os.environ.get('PBS_NODEFILE'):
        print "Submitted via PBS"
        nodeList = [n.strip() for n in open(os.environ['PBS_NODEFILE'])]
    else:
        print >> sys.stderr, "Can't figure out what nodes to use"
        print >> sys.stderr, "Has this been submitted using LSF or PBS?"
        sys.exit(1)

    return nodeList

if __name__ == '__main__':
    host = None  # no good default since this is executed on the cluster
    port = 8765
    verbose = 0

    try:
        opts, args = getopt.getopt(sys.argv[1:], 'h:p:v')

        for opt, arg in opts:
            if opt == '-h':
                host = arg
            elif opt == '-p':
                port = int(arg)
            elif opt == '-v':
                verbose = 1
            else:
                raise 'internal error: out-of-sync with getopt'

        if not host:
            print >> sys.stderr, 'the -h option is required'
            sys.exit(1)

        if len(args) > 0:
            print >> sys.stderr, 'ignoring unused arguments:', ' '.join(args)

        nodeList = getNodeList()
        print "Executing on nodes:", " ".join(nodeList)
        s = Sleigh(launch=sshcmd, nodeList=nodeList, nwsHost=host, nwsPort=port, verbose=verbose)

        # here's a personal favorite of mine...
        from math import exp
        r = s.eachElem(exp, range(100))
        print "The results are:", r

    except getopt.GetoptError, e:
        print >> sys.stderr, e.msg
        sys.exit(1)
    except ValueError, e:
        print >> sys.stderr, "option %s requires an integer argument" % opt
        sys.exit(1)
    except SystemExit, e:
        sys.exit(e.code)
    except:
        ex = sys.exc_info()
        print >> sys.stderr, ex[0], ex[1]
