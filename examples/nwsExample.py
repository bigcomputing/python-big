#!/usr/bin/env python

import sys

try:
    from nws.client import NetWorkSpace
except ImportError, e:
    print >> sys.stderr, "unable to import the nws.client python module"
    print >> sys.stderr, "make sure you have nwsclient installed on this machine"
    sys.exit(1)

try:
    ws = NetWorkSpace('snake pit')
except ImportError, e:
    print >> sys.stderr, "make sure you're running the NWS server on this machine"
    print >> sys.stderr, str(e)
    sys.exit(1)

print 'connected, listing contents of netWorkSpace (should be nothing there).'
print ws.listVars()

ws.store('x', 1)
print 'should now see x.'
print ws.listVars()

print 'find (but don\'t consume) x.'
print ws.find('x')
print 'check that it is still there.'
print ws.listVars()

print 'associate another value with x.'
ws.store('x', 2)
print ws.listVars()

print 'consume values for x, should see them in order saved.'
print ws.fetch('x')
print ws.fetch('x')
print 'no more values for x... .'
print ws.listVars()

print 'so try to fetch and see what happens... .'
print ws.fetchTry('x', 'no go')

print 'create a single-value variable.'
ws.declare('pi', 'single')
print ws.listVars()

print 'get rid of x.'
ws.deleteVar('x')
print ws.listVars()

print 'try to store two values to pi.'
ws.store('pi', 2.171828182)
ws.store('pi', 3.141592654)
print ws.listVars()

print 'check that the right one was kept.'
print ws.find('pi')

print "store a dictionary."
ws.store('dict', {'foo': 'spam', 'bar': 'eggs'})
d = ws.find('dict')
print d

print "store a list."
ws.store('list', ['foo', 1, 'spam', 3.14159, 'bar', 42, 'eggs', 32764])
l = ws.find('list')
print l

print 'what about the rest of the world?'
print ws.server.listWss()
