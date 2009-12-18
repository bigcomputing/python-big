#!/usr/bin/env python

import sys, os, time
import nws.client, nws.sleigh
import worker

wsname, host, port = 'foo', 'localhost', 8765
ws = nws.client.NetWorkSpace(wsname, host, port)

verbose = 1
scriptDir = None
if len(sys.argv) > 1:
    scriptDir = sys.argv[1]

print "using scriptDir =", scriptDir
s = nws.sleigh.Sleigh(scriptDir=scriptDir, verbose=verbose)

p = s.eachWorker(worker.worker, wsname, host, port, blocking=False)

r = p.wait()
print "Results from execution 1:", r

r = s.eachWorker(worker.worker, wsname, host, port)
print "Results from execution 2:", r

r = s.eachWorker(worker.worker, wsname, host, port)
print "Results from execution 3:", r

r = s.eachWorker(worker.worker, wsname, host, port)
print "Results from execution 4:", r

r = s.eachWorker(worker.worker, wsname, host, port)
print "Results from execution 5:", r

print "Loop over about 1 minute of work"
for i in xrange(60):
    r = s.eachWorker(time.sleep, 1)

print "Variable listing:"
print ws.listVars(),

s.stop()
