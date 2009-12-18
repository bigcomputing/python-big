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

import threading
import win32serviceutil, win32service, win32event, win32api
from nws.client import NetWorkSpace, NwsException

# babelfish states
_BABELFISH_RUNNING = 100
_BABELFISH_DIED = 101
_BABELFISH_RESTARTED = 102

# timeouts
_DIED_TIMEOUT = 10 * 1000       # milliseconds to wait before restarting
                                # a dead babelfish
_RESTARTED_TIMEOUT = 10 * 1000  # milliseconds to wait before considering a
                                # restarted babelfish to be running

class PyBabelfishService(win32serviceutil.ServiceFramework):
    _svc_name_ = 'PyBabelfishService'
    _svc_display_name_ = 'Babelfish Service for Python NetWorkSpaces'
    _svc_description_ = 'Translates Python objects in NWS variables into ' \
            'human readable form so that the values of variables in a ' \
            'workspace can be displayed in the NWS server\'s web interface.'

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)

    def BabelfishDied(self):
        win32event.SetEvent(self.babelfishDied)

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)

    def SvcDoRun(self):
        import servicemanager
        self.sm = servicemanager
        self.sm.LogMsg(self.sm.EVENTLOG_INFORMATION_TYPE,
                self.sm.PYS_SERVICE_STARTED, (self._svc_name_, ''))

        self._startBabelfish('localhost', 8765)
        handles = (self.hWaitStop, self.babelfishDied)
        timeout = win32event.INFINITE
        state = _BABELFISH_RUNNING

        while True:            
            # wait for a stop request, a babelfish death, or possibly a timeout
            s = win32event.WaitForMultipleObjects(handles, 0, timeout)

            if s == win32event.WAIT_TIMEOUT:
                if state == _BABELFISH_RESTARTED:
                    self._info("babelfish restarted successfully")
                    timeout = win32event.INFINITE
                    state = _BABELFISH_RUNNING
                elif state == _BABELFISH_DIED:
                    self._startBabelfish('localhost', 8765)
                    handles = (self.hWaitStop, self.babelfishDied)
                    timeout = _RESTARTED_TIMEOUT
                    state = _BABELFISH_RESTARTED
                else:
                    self._error("got an unexpected timeout while in state %d" % state)
                    break
            elif s == win32event.WAIT_OBJECT_0:
                # a shutdown was requested, so break out of the while loop
                self._info("shutdown requested")
                break
            elif s == win32event.WAIT_OBJECT_0 + 1:
                # the babelfish exited by itself, which probably means
                # that the NWS server shutdown.  we want to reconnect
                # when it comes back up, so sleep awhile, and then
                # start another babelfish.  this will probably happen
                # over and over again, so don't do it too frequently.
                if state == _BABELFISH_RUNNING:
                    self._info("babelfish died: restarting in a bit")

                win32api.CloseHandle(self.babelfishDied)
                self.babelfishDied = None
                handles = (self.hWaitStop,)
                timeout = _DIED_TIMEOUT
                state = _BABELFISH_DIED
            else:
                self._error("illegal status from WaitForMultipleObjects: stopping")
                break

        self.sm.LogMsg(self.sm.EVENTLOG_INFORMATION_TYPE,
                self.sm.PYS_SERVICE_STOPPED, (self._svc_name_, ''))

    def _info(self, msg):
        self.sm.LogMsg(self.sm.EVENTLOG_INFORMATION_TYPE, 1, (msg,))

    def _error(self, msg):
        self.sm.LogMsg(self.sm.EVENTLOG_ERROR_TYPE, 1, (msg,))

    def _startBabelfish(self, nwsHost, nwsPort):
        self.babelfish = Babelfish(nwsHost, nwsPort, self.BabelfishDied)
        self.babelfishDied = win32event.CreateEvent(None, 0, 0, None)
        self.babelfish.setDaemon(True)
        self.babelfish.start()

class Babelfish(threading.Thread):
    def __init__(self, nwsHost, nwsPort, died):
        threading.Thread.__init__(self, name='babelfish')
        self.nwsHost = nwsHost
        self.nwsPort = nwsPort
        self.died = died

    def __str__(self):
        return "Babelfish[%s:%d]" % (self.nwsHost, self.nwsPort)

    def run(self):
        try:
            try:
                ws = NetWorkSpace('Python babelfish', self.nwsHost, self.nwsPort)
                while True:
                    try:
                        v = ws.fetch('food')
                        t = str(v) or repr(v)
                    except ImportError, e:
                        t = 'Import Error: ' + str(e)
                    ws.store('doof', t)
            except NwsException:
                # probably the server went down
                pass
        finally:
            try: ws.server.close()
            except: pass
            self.died()

if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(PyBabelfishService)
