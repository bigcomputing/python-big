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

import sys, os, threading, time, tempfile
import win32serviceutil, win32service, win32event, win32process, win32api
import win32file, pywintypes, win32security, win32con
from nws.client import NetWorkSpace, NwsServerException, NwsOperationException, FIFO
from nws.util import msc_argv2str, which

_DEBUG = 1

# try to find the Python installation directory
# XXX should be a better way than this: maybe the registry?
_PYTHONDIR = os.path.dirname(inspect.getfile(win32api))
while not os.path.isfile(os.path.join(_PYTHONDIR, 'python.exe')) and \
        os.path.basename(_PYTHONDIR):
    _PYTHONDIR = os.path.dirname(_PYTHONDIR)

_PYTHONNAME = 'python.exe'  # XXX should this be pythonw.exe?
_DEFAULTINTERP = os.path.join(_PYTHONDIR, _PYTHONNAME)
_NWSSERVERS = [('localhost', 8765)]
_WSNAME = 'PySleighService'
_TMPDIR = tempfile.gettempdir() or '\\TEMP'
_NULFILE = 'NUL'
_PRIVILEGED_ACCOUNTS = ['SYSTEM', 'ADMINISTRATOR']
_onlyForTesting = {}

def _isPrivileged(n):
    return n.upper() in _PRIVILEGED_ACCOUNTS

def _getPassword(n):
    return _onlyForTesting.get(n)

def _getPythonProg():
    m = which(_PYTHONNAME)
    if m:
        return m[0]
    return _DEFAULTINTERP

def logon(user, password, domain='.'):
    try:
        h = win32security.LogonUser(user, domain, password,
                win32con.LOGON32_LOGON_INTERACTIVE,  # XXX could also be BATCH or SERVICE
                win32con.LOGON32_PROVIDER_DEFAULT)
    except:
        h = None

    return h

class PySleighService(win32serviceutil.ServiceFramework):
    _svc_name_ = 'PySleighService'
    _svc_display_name_ = 'Sleigh Service for Python NetWorkSpaces'
    _svc_description_ = 'Allows tasks submitted from Python Sleigh objects ' \
            'to be executed on this machine. ' \
            'To cause your Sleigh to use this service, you must set the ' \
            'launch option to "service" when constructing your Sleigh object. ' \
            'Note that this service can be configured to register its ' \
            'services with multiple NWS servers, but defaults ' \
            'to registering only with the NWS server on the local machine.'

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self._args = args
        self.user = win32api.GetUserName()
        self._shutdownRequested = win32event.CreateEvent(None, 0, 0, None)
        self._requestArrived = win32event.CreateEvent(None, 1, 0, None)  # manual reset
        self._requests = []
        self._lock = threading.Condition()
        if _isPrivileged(self.user):
            self._varName = win32api.GetComputerName()
        else:
            self._varName = self.user + '@' + win32api.GetComputerName()
        self.applicationName = _getPythonProg()
        self._userHandles = {}
        self._nwsservers = []

        # process the service arguments
        self._parseArgs(self._args)

        for nwsHost, nwsPort in self._nwsservers:
            w = RequestWatcher(nwsHost, nwsPort, _WSNAME, self._varName, self.GotRequest)
            w.setDaemon(True)
            w.start()

    def _parseArgs(self, args):
        i = 1  # skip name of service
        while i < len(args):
            a = str(args[i])
            if a == '-P':
                try:
                    self.applicationName = str(args[i + 1])
                    i += 1
                except IndexError:
                    # strange, but no real harm done
                    pass
            else:
                try:
                    port = int(args[i + 1])
                    i += 1
                except (IndexError, ValueError):
                    port = 8765
                self._nwsservers.append((a, port))

            i += 1

        # if no servers were specified, use the default servers
        if not self._nwsservers:
            self._nwsservers = _NWSSERVERS

    # called by the RequestWatcher
    def GotRequest(self, request):
        self._lock.acquire()
        try:
            self._requests.append(request)
            win32event.SetEvent(self._requestArrived)
        finally:
            self._lock.release()

    # called in the service loop to get requests from the RequestWatcher
    def _getRequest(self):
        self._lock.acquire()
        try:
            try:
                r = self._requests.pop(0)
            except IndexError:
                r = None

                # we don't reset the event until we know the
                # queue is empty
                win32event.ResetEvent(self._requestArrived)
        finally:
            self._lock.release()

        return r

    # called by the Sentinel
    def GotShutdown(self, sentinel):
        # the main service thread gave the sentinel this attribute.
        # setting this event indicates that the corresponding worker
        # should be killed if he hasn't died.
        win32event.SetEvent(sentinel._event)

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self._shutdownRequested)

    def SvcDoRun(self):
        import servicemanager
        self.sm = servicemanager
        self.sm.LogMsg(self.sm.EVENTLOG_INFORMATION_TYPE,
                self.sm.PYS_SERVICE_STARTED, (self._svc_name_, ''))

        handles = [self._shutdownRequested, self._requestArrived]
        timeout = win32event.INFINITE
        sentinels = []

        self._info('monitoring nws servers: ' + str(self._nwsservers))

        # sanity checking
        sane = True
        if not self.applicationName:
            self._error("unable to locate a copy of " + _PYTHONNAME)
            sane = False
        elif not os.path.exists(self.applicationName):
            self._error("Python interpreter not found: " + self.applicationName)
            sane = False

        while sane:
            # wait for a stop request or a sleigh worker death
            s = win32event.WaitForMultipleObjects(handles, 0, timeout)

            if s == win32event.WAIT_OBJECT_0:
                # a shutdown was requested
                if len(handles) > 2:
                    # XXX this is currently a dangerous situation since tasks
                    # XXX aren't retreated
                    self._info("shutdown requested: terminating sleigh workers")
                    i = 2
                    while i < len(handles):
                        try:
                            win32process.TerminateProcess(handles[i], 0)
                            win32api.CloseHandle(handles[i])
                        except:
                            e = sys.exc_info()[1]
                            self._info("caught exception terminating sleigh workers: %s" % str(e))
                        i += 2
                else:
                    self._info("shutdown requested while no sleigh workers running")
                break
            elif s == win32event.WAIT_OBJECT_0 + 1:
                # got a request to start a sleigh worker
                request = self._getRequest()
                if request:
                    modulePath = request['PythonSleighModulePath']
                    logDir = request['PythonSleighLogDir']
                    if not logDir or not os.path.isdir(logDir):
                        logDir = _TMPDIR
                    outfile = request['PythonSleighWorkerOut']
                    if outfile:
                        outfile = os.path.join(logDir, os.path.split(outfile)[1])
                    else:
                        outfile = _NULFILE
                    verbose = request.has_key('PythonSleighWorkerOut') and 1 or 0
                    nwsName = request['PythonSleighNwsName']
                    nwsHost = request.get('PythonSleighNwsHost', 'localhost')
                    nwsPort = int(request.get('PythonSleighNwsPort', '8765'))
                    user = request.get('PythonSleighUserName')

                    if not user:
                        self._error("user name not specified")
                        continue
                    elif _isPrivileged(user):
                        self._error("can't run jobs as privileged user: " + user)
                        continue
                    else:
                        if _isPrivileged(self.user):
                            if not self._userHandles.has_key(user):
                                p = _getPassword(user)
                                if not p:
                                    self._error("not configured to run jobs for user: " + user)
                                    continue
                                h = logon(user, p)
                                if not h:
                                    self._error("unable to logon user: " + user)
                                    continue
                                self._userHandles[user] = h
                        else:
                            if user != self.user:
                                self._error("not able to run jobs for user: " + user)
                                self._info("PySleighService is running as user: " + self.user)
                                continue

                    # create the script file for the worker to execute
                    script = '''\
import sys, os, nws.sleigh
sys.path[1:1] = %s.split(os.pathsep)
nws.sleigh.cmdLaunch(%d)
''' % (repr(modulePath), verbose,)
                    fd, tmpname = tempfile.mkstemp(suffix='.py', prefix='__nws', text=True)
                    tmpfile = os.fdopen(fd, 'w')
                    tmpfile.write(script)
                    tmpfile.close()

                    try:
                        # create the Sentinel thread
                        sentinel = Sentinel(nwsHost, nwsPort, nwsName, self.GotShutdown)
                        sentinel._event = win32event.CreateEvent(None, 0, 0, None)
                        sentinel._tmpname = tmpname
                        sentinel.setDaemon(True)
                        sentinel.start()
                    except:
                        # if we can't create the sentinel, don't start the worker
                        self._error("error creating Sentinel thread")
                        os.remove(tmpname)
                    else:
                        # start the worker process
                        commandLine = self._buildCmd()
                        environment = self._buildEnv(request)
                        if _DEBUG:
                            self._info(commandLine)
                            self._info(str(environment))
                        currentDirectory = request.get('PythonSleighWorkingDir')
                        if not currentDirectory or not os.path.isdir(currentDirectory):
                            currentDirectory = _TMPDIR

                        h = self._startWorkerLoop(user, tmpname, outfile, commandLine, environment, currentDirectory)

                        sentinels.append(sentinel)
                        handles.append(h)
                        handles.append(sentinel._event)
                else:
                    if _DEBUG: self._info("_getRequest returned false value")
            elif s >= win32event.WAIT_OBJECT_0 + len(handles):
                # internal error
                self._error("internal error 42")
                break
            else:
                # a worker died or a sentinel signaled a shutdown
                h = s - win32event.WAIT_OBJECT_0
                assert h >= 2
                if h & 1 == 0:
                    # a worker died.
                    # find the Sentinel object so we can delete the tmp file.
                    i = 0
                    while i < len(sentinels):
                        if sentinels[i]._event.handle == handles[h + 1].handle:
                            self._info("removing tmp file " + sentinels[i]._tmpname)
                            os.remove(sentinels[i]._tmpname)
                            del sentinels[i]
                            break
                        i += 1
                    else:
                        self._error("could not find sentinel in list")

                    win32api.CloseHandle(handles[h])
                    win32api.CloseHandle(handles[h + 1])

                    del handles[h]
                    del handles[h]
                else:
                    # a sentinel signaled: kill the worker
                    self._info("sentinel signaled shutdown")
                    try: win32process.TerminateProcess(handles[h - 1], 0)
                    except: pass

        win32api.CloseHandle(self._shutdownRequested)
        win32api.CloseHandle(self._requestArrived)

        self.sm.LogMsg(self.sm.EVENTLOG_INFORMATION_TYPE,
                self.sm.PYS_SERVICE_STOPPED, (self._svc_name_, ''))

    def _info(self, msg):
        self.sm.LogMsg(self.sm.EVENTLOG_INFORMATION_TYPE, 1, (msg,))

    def _error(self, msg):
        self.sm.LogMsg(self.sm.EVENTLOG_ERROR_TYPE, 1, (msg,))

    def _buildCmd(self):
        return msc_argv2str([self.applicationName, '-u'])

    def _buildEnv(self, request):
        env = dict(os.environ)
        env.update(request)
        env['PythonSleighName'] = self._varName

        return env

    def _startWorkerLoop(self, user, script, outfile, commandLine, environment, currentDirectory):
        sa = pywintypes.SECURITY_ATTRIBUTES()
        sa.bInheritHandle = 1
        try:
            outh = win32file.CreateFile(outfile,
                    win32file.GENERIC_WRITE, 0, sa, win32file.CREATE_ALWAYS,
                    win32file.FILE_ATTRIBUTE_NORMAL, None)
        except pywintypes.error:
            self._error("error creating " + outfile)
            raise

        try:
            inh = win32file.CreateFile(script,
                    win32file.GENERIC_READ, 0, sa, win32file.OPEN_EXISTING,
                    win32file.FILE_ATTRIBUTE_NORMAL, None)
        except pywintypes.error:
            self._error("error opening " + script)
            win32api.CloseHandle(outh)
            raise

        processSecurityAttributes = None
        threadSecurityAttributes = None
        fInheritHandles = 1
        creationFlags = win32process.CREATE_NO_WINDOW
        startupInfo = win32process.STARTUPINFO()
        startupInfo.dwFlags = win32process.STARTF_USESTDHANDLES
        startupInfo.hStdInput = inh
        startupInfo.hStdOutput = outh
        startupInfo.hStdError = outh

        try:
            if _isPrivileged(self.user):
                procHandle, threadHandle, procId, threadId = win32process.CreateProcessAsUser(
                        self._userHandles[user], self.applicationName, commandLine,
                        processSecurityAttributes, threadSecurityAttributes,
                        fInheritHandles, creationFlags,
                        environment, currentDirectory,
                        startupInfo)
            else:
                procHandle, threadHandle, procId, threadId = win32process.CreateProcess(
                        self.applicationName, commandLine,
                        processSecurityAttributes, threadSecurityAttributes,
                        fInheritHandles, creationFlags,
                        environment, currentDirectory,
                        startupInfo)

            win32api.CloseHandle(threadHandle)
        except pywintypes.error:
            self._error("error executing " + self.applicationName)
            win32api.CloseHandle(outh)
            win32api.CloseHandle(inh)
            raise

        win32api.CloseHandle(outh)
        win32api.CloseHandle(inh)

        return procHandle

class RequestWatcher(threading.Thread):
    def __init__(self, nwsHost, nwsPort, wsName, varName, gotRequest):
        threading.Thread.__init__(self, name='RequestWatcherThread')
        self.nwsHost = nwsHost
        self.nwsPort = nwsPort
        self.wsName = wsName
        self.varName = varName
        self.gotRequest = gotRequest

    def run(self):
        while True:
            try:
                try:
                    ws = NetWorkSpace(self.wsName, self.nwsHost, self.nwsPort,
                            persistent=True)
                    ws.declare(self.varName, FIFO)

                    while True:
                        s = ws.fetch(self.varName)
                        try:
                            # split the request using the first character
                            x = s[1:].split(s[0])
                            r = {'PythonSleighNwsHost': self.nwsHost,
                                 'PythonSleighNwsPort': str(self.nwsPort)}
                            r['PythonSleighNwsName'], r['PythonSleighWorkerCount'], \
                                r['PythonSleighID'], r['PythonSleighWorkingDir'], \
                                r['PythonSleighWorkerOut'], r['PythonSleighLogDir'], \
                                r['PythonSleighUserName'], r['PythonSleighModulePath'] = x
                            self.gotRequest(r)
                        except:
                            # bad request: ignore it
                            pass
                except NwsServerException:
                    # server is probably down.  sleep a bit, and try again
                    time.sleep(10)
                except NwsOperationException:
                    # maybe someone deleted my variable to signal a shutdown?
                    return
            finally:
                # ws may not be defined
                try:ws.server.close()
                except: pass
                ws = None

class Sentinel(threading.Thread):
    def __init__(self, nwsHost, nwsPort, nwsName, gotShutdown):
        threading.Thread.__init__(self, name='Sentinel:' + nwsName)
        self.nwsHost = nwsHost
        self.nwsPort = nwsPort
        self.nwsName = nwsName
        self.gotShutdown = gotShutdown
        self.ws = NetWorkSpace(self.nwsName, self.nwsHost, self.nwsPort,
                useUse=True, create=False)

    def __str__(self):
        return "%s@%s:%d" % (self.nwsName, self.nwsHost, self.nwsPort)

    def run(self):
        try:
            try:
                self.ws.find('Sleigh ride over')
                self.ws.store('bye', 'Sleigh ride over')
            except Exception, e:
                try: self.ws.store('bye', str(sys.exc_info()[1]))
                except: pass
        finally:
            try: self.gotShutdown(self)
            except: pass
            try: self.ws.server.close()
            except: pass

if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(PySleighService)
