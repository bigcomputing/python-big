#!/usr/bin/env python

import sys, os, getopt
from nws.client import NetWorkSpace, DICT, V_VARIABLE, V_MODE, SINGLE

def exists(ws, varName):
    return varName in listVars(ws)

def listVars(ws):
    wsList = ws.listVars(format=DICT).values()
    return [t[V_VARIABLE] for t in wsList if t[V_MODE] == SINGLE]

def usage(msg=''):
    print >> sys.stderr, "usage: %s [-h nwshost] [-p nwsport] [ls | get | put | rm] file ..." % sys.argv[0]
    if msg:
        print >> sys.stderr, msg
    sys.exit(1)

if __name__ == '__main__':
    nkw = {'persistent': True}

    try:
        opts, args = getopt.getopt(sys.argv[1:], 'h:p:')

        for opt, arg in opts:
            if opt == '-h':
                nkw['serverHost'] = arg
            elif opt == '-p':
                nkw['serverPort'] = int(arg)
            else:
                raise 'internal error: out-of-sync with getopt'

        if len(args) < 1:
            op = 'ls'  # default operation
        elif args[0] not in ['list', 'ls', 'dir',
                             'put', 'store', 'write',
                             'get', 'find', 'read',
                             'delete', 'del', 'remove', 'rm']:
            usage()
        else:
            op = args.pop(0)

        ws = NetWorkSpace('NWS Copy', **nkw)

        if op in ['list', 'ls', 'dir']:
            vars = listVars(ws)
            vars.sort()
            for var in vars:
                print var
        elif op in ['put', 'store', 'write']:
            if not args: usage()
            for n in args:
                try:
                    f = open(n, 'rb')
                except IOError:
                    print >> sys.stderr, "error reading file", n
                    continue

                varName = os.path.basename(n)
                if exists(ws, varName):
                    a = raw_input("overwrite existing variable %s? (y/n) " % varName)
                    if not a.strip().lower().startswith('y'):
                        print >> sys.stderr, "skipping", n
                        continue
                    else:
                        print >> sys.stderr, "overwriting", n 

                try:
                    ws.declare(varName, 'single')
                    ws.storeFile(varName, f)
                except:
                    print >> sys.stderr, "error putting file", n

                try: f.close()
                except: pass
        elif op in ['get', 'find', 'read']:
            vars = args or listVars(ws)
            for n in vars:
                varName = os.path.basename(n)
                if not exists(ws, varName):
                    print >> sys.stderr, "no variable named", varName
                    continue

                # only confirm when wildcarding
                if not args:
                    a = raw_input("get variable %s? (y/n) " % varName)
                    if not a.strip().lower().startswith('y'):
                        print >> sys.stderr, "skipping", varName
                        continue
                    else:
                        print >> sys.stderr, "getting", varName

                if os.path.exists(n):
                    a = raw_input("overwrite existing file %s? (y/n) " % n)
                    if not a.strip().lower().startswith('y'):
                        print >> sys.stderr, "skipping", n
                        continue
                    else:
                        print >> sys.stderr, "overwriting", n

                try:
                    f = open(n, 'wb')
                except IOError:
                    print >> sys.stderr, "error writing file", n
                    continue

                try:
                    ws.findFile(varName, f)
                except:
                    print >> sys.stderr, "error getting file", n

                try: f.close()
                except: pass
        elif op in ['delete', 'del', 'remove', 'rm']:
            vars = args or listVars(ws)
            for n in vars:
                varName = os.path.basename(n)
                if not exists(ws, varName):
                    print >> sys.stderr, "no variable named", varName
                    continue

                # always confirm when deleting
                a = raw_input("delete variable %s? (y/n) " % varName)
                if not a.strip().lower().startswith('y'):
                    print >> sys.stderr, "skipping", varName
                    continue
                else:
                    print >> sys.stderr, "deleting", varName

                try:
                    ws.deleteVar(varName)
                except:
                    print >> sys.stderr, "error deleting", varName
        else:
            print >> sys.stderr, "internal error: unhandled operation:", op

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
