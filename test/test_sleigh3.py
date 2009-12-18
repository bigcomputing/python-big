#!/usr/bin/env python

import sys, os, math, operator, getopt
from nws.sleigh import Sleigh, SleighIllegalValueException, sshcmd
import worker

verbose = 0
launch = sshcmd
scriptDir = None

try:
    opts, args = getopt.getopt(sys.argv[1:], 'vws:')
except getopt.GetoptError, e:
    print >> sys.stderr, e.msg
    sys.exit(1)

for opt, arg in opts:
    if opt == '-s':
        scriptDir = arg
    elif opt == '-v':
        verbose = 1
    elif opt == '-w':
        launch = 'web'
    else:
        print >> sys.stderr, 'error: getopt should have prevented this'
        sys.exit(1)

print "scriptDir =", scriptDir
print "verbose =", verbose
print "launch =", launch
if launch == 'web':
    print "waiting for web launch workers..."

s = Sleigh(scriptDir=scriptDir, launch=launch, verbose=verbose)

t = map(math.exp, range(100))

print "testing a simple list:"
r = s.eachElem(math.exp, range(100))
if r != t:
    print "error:", r
else:
    print "success"

print "testing list of one list:"
r = s.eachElem(math.exp, [range(100)])
if r != t:
    print "error:", r
else:
    print "success"

print "testing a list of two lists:"
r = s.eachElem(operator.add, [range(100), range(100)])
if r != range(0, 200, 2):
    print "error:", r
else:
    print "success"

print "testing a simple list and a fixed arg:"
r = s.eachElem(operator.add, range(100), 100)
if r != range(100, 200):
    print "error:", r
else:
    print "success"

print "testing a list of one list and a fixed arg:"
r = s.eachElem(operator.add, [range(100)], 100)
if r != range(100, 200):
    print "error:", r
else:
    print "success"

print "testing a list of one list, a fixed arg, and a permute vector:"
r = s.eachElem(operator.add, [range(100)], 100, argPermute=[1, 0])
if r != range(100, 200):
    print "error:", r
else:
    print "success"

print "testing a zero length permutation vector:"
r = s.eachElem(worker.answer, range(100), argPermute=[])
if r != [42] * 100:
    print "error:", r
else:
    print "success"

print "testing a short permutation vector:"
r = s.eachElem(math.exp, [range(100), range(100,200), range(200,300)], argPermute=[0])
if r != t:
    print "error:", r
else:
    print "success"

print "testing a short permutation vector non-blocking:"
p = s.eachElem(math.exp, [range(100), range(100,200), range(200,300)],
        argPermute=[0], blocking=False)
print "check:", p.check()
r = p.wait()
if r != t:
    print "error:", r
else:
    print "success"

print "testing exception"
try:
    s.eachElem(math.exp, 0)
    print "error: expected illegal value exception"
except SleighIllegalValueException, e:
    print "success:", e

raw_input("Hit return to stop sleigh: ")
s.stop()
