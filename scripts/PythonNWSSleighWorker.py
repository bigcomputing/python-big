#!/usr/bin/env python
#
# Copyright (c) 2005-2008, REvolution Computing, Inc.
#
# NetWorkSpaces is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License as published
# by the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307
# USA
#

import sys, os, tempfile, traceback, socket, time, subprocess
from os import environ as Env
from nws.client import NetWorkSpace

try:
    import win32api
    _winuser = win32api.GetUserName()
except ImportError:
    _winuser = None

if sys.platform.startswith('win'):
    _TMPDIR = tempfile.gettempdir() or '\\TEMP'
    _NULFILE = 'NUL'
else:
    _TMPDIR = tempfile.gettempdir() or '/tmp'
    _NULFILE = '/dev/null'

class Logger:
    def __init__(self, ws=None, name=socket.getfqdn(), role='unknown'):
        self._ws = ws
        self._name = name
        self._role = role

    def setWs(self, ws):
        self._ws = ws

    def setName(self, name):
        self._name = name

    def setRole(self, role):
        self._role = role

    def logError(self, *s):
        msg = ' '.join([str(m) for m in s])
        print >> sys.stderr, msg
        if self._ws:
            self._ws.store('logError', '[%s] %s %s -- %s' % (time.asctime(), self._name, self._role, msg))

    def logDebug(self, *s):
        msg = ' '.join([str(m) for m in s])
        print >> sys.stderr, msg
        if self._ws:
            self._ws.store('logDebug', '[%s] %s %s -- %s' % (time.asctime(), self._name, self._role, msg))

def main():
    # initialize variables from environment
    modulePath = Env.get('PythonSleighModulePath', '')
    logDir = Env.get('PythonSleighLogDir')
    if not logDir or not os.path.isdir(logDir):
        logDir = _TMPDIR
    outfile = Env.get('PythonSleighWorkerOut')
    if outfile:
        outfile = os.path.join(logDir, os.path.split(outfile)[1])
    else:
        outfile = _NULFILE
    Env['PythonSleighLogFile'] = outfile
    verbose = Env.has_key('PythonSleighWorkerOut') and 1 or 0
    nwsName = Env['PythonSleighNwsName']
    nwsHost = Env.get('PythonSleighNwsHost', 'localhost')
    nwsPort = int(Env.get('PythonSleighNwsPort', '8765'))
    if Env.has_key('PythonSleighName'):
        name = Env['PythonSleighName']
        if Env.has_key('PythonSleighID'):
            name = '%s@%s' % (name, Env['PythonSleighID'])
        logger.setName(name)
    logger.logDebug(nwsName, nwsHost, nwsPort)

    nws = NetWorkSpace(nwsName, nwsHost, nwsPort, useUse=True, create=False)
    newProtocol = nws.findTry('version') is not None
    if newProtocol:
        worker_id = nws.fetch('worker_ids')
        Env['PythonSleighRank'] = worker_id

    # create the script file for the worker to execute
    script = '''\
import sys, os
try: os.setpgid(0, 0)
except: pass
try: sys.stdout = sys.stderr = open(%s, 'w', 0)
except: pass
sys.path[1:1] = %s.split(os.pathsep)
from nws.sleigh import cmdLaunch
print "entering worker loop"
cmdLaunch(%d)
''' % (repr(outfile), repr(modulePath), verbose)
    fd, tmpname = tempfile.mkstemp(suffix='.py', prefix='__nws', text=True)
    tmpfile = os.fdopen(fd, 'w')
    tmpfile.write(script)
    tmpfile.close()

    logger.logDebug("executing Python worker")
    argv = [sys.executable, tmpname]
    out = open(outfile, 'w')

    try:
        p = subprocess.Popen(argv, stdin=open(tmpname), stdout=out,
                stderr=subprocess.STDOUT)
    except OSError, e:
        logger.logError("error executing command:", argv)
        raise e

    if hasattr(p, '_handle'):
        wpid = int(p._handle)  # Windows
    else:
        wpid = p.pid  # Unix
        try: os.setpgid(wpid, 0)
        except: pass

    logger.logDebug("waiting for shutdown")
    sys.path[1:1] = modulePath.split(os.pathsep)
    logger.setWs(nws)
    sentinel(nws)

    logger.logDebug("killing Python worker")
    kill(wpid)

    logger.logDebug("all done")

    # XXX might need to wait for child to die on Windows
    try: os.remove(tmpname)
    except: pass

    # just being paranoid
    sys.stdout.flush()
    sys.stderr.flush()

def getenv(args):
    e = {}
    for kv in args:
        try:
            k, v = [x.strip() for x in kv.split('=', 1)]
            if k: e[k] = v
        except:
            logger.logError("warning: bad argument:", kv)
    return e

def setup(f):
    if os.path.isabs(f):
        log = f
    else:
        log = os.path.join(Env.get('PythonSleighLogDir', _TMPDIR), f)

    # open the output file and redirect stdout and stderr
    try:
        # create an unbuffered file for writing
        outfile = open(log, 'w', 0)
        sys.stdout = outfile
        sys.stderr = outfile
    except:
        traceback.print_exc()
        logger.logError("warning: unable to create file:", log)

    # cd to the specified directory
    wd = Env.get('PythonSleighWorkingDir')
    if not wd or not os.path.isdir(wd):
        wd = _TMPDIR

    try:
        os.chdir(wd)
    except:
        traceback.print_exc()
        logger.logError("warning: unable to cd to", wd)

    # this information will normally seem rather obvious
    logger.logDebug("current working directory:", os.getcwd())

def sentinel(nws):
    logger.logDebug("waiting for sleigh to be stopped")

    try:
        nws.find('Sleigh ride over')
        nws.store('bye', 'Sleigh ride over')
    except:
        try: nws.store('bye', str(sys.exc_info()[1]))
        except: pass

    logger.logDebug("sentinel returning")

def kill(pid):
    try:
        try:
            import win32api
            # the "pid" is really a handle on Windows
            win32api.TerminateProcess(pid, -1)
            win32api.CloseHandle(pid)  # XXX not sure about this
        except ImportError:
            try:
                from signal import SIGTERM as sig
            except ImportError:
                logger.logError("couldn't import signal module")
                sig = 15
            try: os.kill(-pid, sig)
            except: os.kill(pid, sig)
    except OSError:
        # process is already dead, so ignore it
        pass
    except:
        traceback.print_exc()

if __name__ == '__main__':
    try:
        logger = Logger(role='sentinel')

        # treat all arguments as environment settings
        Env.update(getenv(sys.argv[1:]))

        # user name is used in log file name to avoid permission problems
        user = Env.get('USER') or Env.get('USERNAME') or \
                _winuser or 'nwsuser'
        f = 'PythonSleighSentinelLog_' + user + '_' + \
                Env.get('PythonSleighID', 'X') + '.txt'
        setup(f)

        if not Env.has_key('PythonSleighNwsName'):
            logger.logError("PythonSleighNwsName variable is not set")
            print >> sys.__stderr__, "PythonSleighNwsName variable is not set"
        else:
            main()
    except:
        traceback.print_exc()
