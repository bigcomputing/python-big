#!/usr/bin/env python

import sys, getopt, traceback
from types import FunctionType, BuiltinFunctionType
from nws.sleigh import Sleigh, sshcmd

if __name__ == '__main__':
    host = 'localhost'
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

        if len(args) == 0:
            print >> sys.stderr, 'no workers specified'
            sys.exit(1)

        s = Sleigh(launch=sshcmd, nodeList=args, nwsHost=host, nwsPort=port, verbose=verbose)

        while 1:
            print '>',
            cmd = raw_input()
            cmd = cmd.strip()
            if cmd:
                try:
                    results = s.eachWorker(cmd)
                    for i in results:
                        if i:
                            print i
                except SyntaxError, e:
                    print >> sys.stderr, str(e)

    except getopt.GetoptError, e:
        print >> sys.stderr, e.msg
        sys.exit(1)
    except ValueError, e:
        print >> sys.stderr, "option %s requires an integer argument" % opt
        sys.exit(1)
    except (KeyboardInterrupt, SystemExit, EOFError):
        print
    except:
        ex = sys.exc_info()
        print >> sys.stderr, ex[0], ex[1]
