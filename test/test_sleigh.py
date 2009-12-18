#!/usr/bin/env python

import os, sys, glob
from math import exp
from nws.sleigh import Sleigh, defaultSleighOptions
from nws.sleigh import envcmd, scriptcmd, sshcmd
from nws.sleigh import _findScript

# this is like scriptcmd, but uses 'python' explicitly,
# and therefore requires a specified scriptDir
def pythoncmd(machine, *env, **opts):
    if opts['scriptDir']:
        script = os.path.join(opts['scriptDir'], opts['scriptName'])
    else:
        script = opts['scriptName']
    return [sys.executable, script] + list(env)

# this is like envcmd, but uses '/bin/sh' explicitly,
# and therefore requires a specified scriptDir
def shellcmd(machine, *env, **opts):
    if opts['scriptDir']:
        script = os.path.join(opts['scriptDir'], opts['scriptName'])
    else:
        script = opts['scriptName']
    return ['/usr/bin/env'] + list(env) + ['/bin/sh', script]

# XXX this needs some thought
BINDIR = os.path.dirname(_findScript('PythonNWSSleighWorker'))

def test(verbose=0):
    s = Sleigh(verbose=verbose, **slargs)
    r = s.eachElem(exp, range(100))
    q = [exp(i) for i in range(100)]
    s.stop()
    if r == q:
        sys.stderr.write('.')
        sys.stderr.flush()
        return 0
    else:
        sys.stderr.write('E')
        sys.stderr.flush()
        return 1

slargs = {}
try: slargs['nwsHost'] = os.environ['NWS_HOST']
except: pass
try: slargs['nwsPort'] = int(os.environ['NWS_PORT'])
except: pass

orig = dict(defaultSleighOptions)

pyopts = [
    { 'scriptExec': pythoncmd,           'scriptDir': BINDIR,      'scriptName': 'PythonNWSSleighWorker.py', },
    { 'scriptExec': scriptcmd,           'scriptDir': '',          'scriptName': 'PythonNWSSleighWorker.py', },
    { 'scriptExec': scriptcmd,           'scriptDir': BINDIR,      'scriptName': 'PythonNWSSleighWorker.py', },
    { 'scriptExec': envcmd,              'scriptDir': '',          'scriptName': 'PythonNWSSleighWorker.py', 'interp': sys.executable, },
    { 'scriptExec': envcmd,              'scriptDir': BINDIR,      'scriptName': 'PythonNWSSleighWorker.py', 'interp': sys.executable, },
]

shopts = [
    { 'scriptExec': shellcmd,            'scriptDir': BINDIR,      'scriptName': 'PythonNWSSleighWorker', },
    { 'scriptExec': envcmd,              'scriptDir': '',          'scriptName': 'PythonNWSSleighWorker', },
    { 'scriptExec': envcmd,              'scriptDir': BINDIR,      'scriptName': 'PythonNWSSleighWorker', },
]

failures = 0
successes = 0

if test(0) == 0:
    successes += 1
else:
    failures += 1

for o in pyopts:
    for wd in ('/tmp', os.getcwd()):
        for ssh in [sshcmd]:
            defaultSleighOptions.clear()
            defaultSleighOptions.update(orig)
            defaultSleighOptions.update(o)
            defaultSleighOptions['launch'] = ssh
            defaultSleighOptions['workingDir'] = wd

            # print 'testing:', defaultSleighOptions
            if test(0) == 0:
                successes += 1
            else:
                failures += 1

print 'successes: %d; failures: %d' % (successes, failures)

def rmglobs(*globs):
    for g in globs:
        for f in glob.glob(g):
            os.remove(f)

globs = [
    "PythonSleighSentinelLog_*",
    "PythonNWSSleighWorkerLog_*",
    "sleigh_ride_*",
    "/tmp/PythonSleighSentinelLog_*",
    "/tmp/PythonNWSSleighWorkerLog_*",
    "/tmp/sleigh_ride_*",
]

rmglobs(*globs)

print 'finished'
