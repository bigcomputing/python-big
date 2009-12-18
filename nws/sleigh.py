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

"""Python API for parallel programming using NetWorkSpaces.

The sleigh module, built on top of NetWorkSpaces (NWS), makes it very
easy to write simple parallel programs.  It contains the Sleigh class,
which provides two basic methods for executing tasks in parallel:
eachElem and eachWorker.

eachElem is used to execute a specified function multiple times in
parallel with a varying set of arguments.  eachWorker is used to execute
a function exactly once on every worker in the sleigh with a fixed set
of arguments.

Example:

First start up the NWS server, using the twistd command:

    % twistd -y /etc/nws.tac

Now you can create a sleigh to execute python code in parallel:

    % python
    >>> from nws.sleigh import Sleigh
    >>> s = Sleigh()
    >>> import math
    >>> result = s.eachElem(math.exp, range(10))
    >>> print "The answer is", result

"""

from __future__ import generators

import sys, os, string, time, socket, random
import cPickle, pickle, new, copy_reg, traceback

try: import platform
except: platform = None

try: import subprocess
except: pass

try:
    import win32api
    _winuser = win32api.GetUserName()
except ImportError:
    _winuser = None

try:
    from itertools import izip, count
except ImportError:
    # this is compatibility code for Python 2.2 support
    def izip(*iterables):
        iterables = map(iter, iterables)
        while iterables:
            result = [it.next() for it in iterables]
            yield tuple(result)

    def count(n=0):
        while True:
            yield n
            n += 1

try:
    import webbrowser
except ImportError:
    webbrowser = None

# We use this a lot, so provide convenience name.
from os import environ as _Env
from types import CodeType, FunctionType, BuiltinFunctionType, MethodType
from cStringIO import StringIO
from warnings import warn, filterwarnings
from urllib import quote_plus

from nws.client import NetWorkSpace, NwsServer, NwsNoWorkSpaceException
from nws.client import FIFO, DICT, V_VARIABLE, V_VALUES, V_FETCHERS, V_FINDERS, V_MODE
from nws.client import PROTOCOL_VERSION_OLD, PROTOCOL_VERSION_NEW_0
from nws.util import which, msc_quote
from nws import __version__

# Change the default warning filtering for this module
filterwarnings('always', module=__name__)

__all__ = [
    'Sleigh', 'SleighPending', 'SleighResultIterator',
    'SleighException', 'SleighNwsException',
    'SleighGatheredException', 'SleighStoppedException',
    'SleighOccupiedException', 'SleighIllegalValueException',
    'SleighScriptException', 'SleighTaskException',
    'SleighJoinException', 'SleighNotAllowedException',
    'SleighUnsupportedMethodException'
]

# We use alternating barriers to synchronize eachWorker
# invocations. Their names are common to workers and sleighs.
_barrierNames = ['barrier0', 'barrier1']

_lambdaName = (lambda: 0).__name__

# These are directories to look for the sleigh worker scripts
# on the local machine.  These are only searched if scriptDir
# hasn't been set, and the worker script wasn't found using
# the PATH environment variable.
if sys.platform.startswith('win'):
    _pred = lambda p: True
    _rule = lambda p, t: os.path.join(*(p + t))
    _exeRules = [
            (_pred, (), _rule, (['scripts'],)),
            ]
    _scriptDirs = [
            r'C:\python25\scripts',
            r'C:\python24\scripts',
            ]
else:
    _pred = lambda p, prefix: _startsWith(p, prefix)
    _rule = lambda p, n, t: os.path.join(*(p[1:n] + t))
    _exeRules = [
            (_pred, (['', '/', 'Library', 'Frameworks', 'Python.framework', 'Versions'],), _rule, (7, ['bin'])),
            (_pred, (['', '/', 'System', 'Library', 'Frameworks', 'Python.framework', 'Versions'],), _rule, (8, ['bin'])),
            (_pred, (['', '/', 'usr', 'bin'],), _rule, (4, [])),
            (_pred, (['', '/', 'usr', 'local', 'bin'],), _rule, (5, [])),
            ]
    _scriptDirs = [
            '/Library/Frameworks/Python.framework/Versions/Current/bin',
            '/System/Library/Frameworks/Python.framework/Versions/Current/bin',
            ]

class _Result(object):
    tag = None
    value = None
    error = False

    def __str__(self):
        if self.error:
            return "Result Error %s: %s" % (repr(self.tag), repr(self.value))
        else:
            return "Result %s: %s" % (repr(self.tag), repr(self.value))

class _SleighState(object):
    bx = 0
    occupied = None
    stopped = False
    totalTasks = 0

class _Task(object):
    args = None
    barrier = None
    code = None
    module = None
    funcName = None
    methName = None
    tag = None
    type = None

    def __str__(self):
        if self.funcName:
            return "Function Task %s: %s(%s)" % \
                    (repr(self.tag), self.funcName, repr(self.args))
        elif self.methName:
            return "Method Task %s: %s(%s)" % \
                    (repr(self.tag), self.methName, repr(self.args))
        else:
            # XXX improve this
            return "Other Task %s: (%s)" % \
                    (repr(self.tag), repr(self.args))

class SleighException(Exception):
    """Base class for all exceptions raised by this module."""
    def __str__(self):
        return '%s[%s]' % (self.__class__.__name__,
                ' '.join(map(str, self.args)))

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__,
                ', '.join(map(repr, self.args)))

class SleighNwsException(SleighException):
    """Error performing NWS operation."""

class SleighGatheredException(SleighException):
    """Results already gathered."""

class SleighStoppedException(SleighException):
    """Sleigh is stopped."""

class SleighOccupiedException(SleighException):
    """Sleigh is occupied."""

class SleighIllegalValueException(SleighException):
    """Illegal value specified."""

class SleighScriptException(SleighException):
    """Unable to find sleigh worker script."""

class SleighTaskException(SleighException):
    """Error executing a task."""

class SleighJoinException(SleighException):
    """Too late to join worker group."""

class SleighNotAllowedException(SleighException):
    """Sleigh construction not allowed."""

class SleighUnsupportedMethodException(SleighException):
    """Unsupported method."""

# functions needed for pickling and unpickling code objects
def _code_ctor(*args):
    return new.code(*args)

def _reduce_code(co):
    if co.co_freevars or co.co_cellvars:
        raise ValueError("cannot pickle code objects from closures")
    return _code_ctor, (co.co_argcount, co.co_nlocals, co.co_stacksize,
            co.co_flags, co.co_code, co.co_consts, co.co_names,
            co.co_varnames, co.co_filename, co.co_name, co.co_firstlineno,
            co.co_lnotab)

def _tb_info(tb):
    while tb.tb_next is not None:
        tb = tb.tb_next
    return tb.tb_frame.f_code.co_filename, tb.tb_frame.f_lineno

def _tb_stack(tb):
    sio = StringIO()
    traceback.print_tb(tb, None, sio)
    stack = sio.getvalue()
    sio.close()
    return stack

# register the reductor to be used for pickling objects of type 'CodeType'
copy_reg.pickle(CodeType, _reduce_code, _code_ctor)

# test if an object is an iterable
def _isiterable(x):
    try:
        iter(x)
        return True
    except TypeError:
        return False

############################################################################
# Worker code.
#
def cmdLaunch(verbose=0):
    """Execute sleigh tasks on behalf of the sleigh owner.

    cmdLaunch([verbose])

    This function is intended for internal use only.

    Arguments:

    verbose -- Boolean flag used for displaying debug messages.  The
            default value is 0.

    Environment Variables:

    PythonSleighNwsName -- Name of the sleigh workspace.

    PythonSleighUserNwsName -- Name of the user workspace.

    PythonSleighNwsHost -- Host name of the NWS server used by the
            sleigh master.  The default value is 'localhost'.

    PythonSleighNwsPort -- Port number of the NWS server used by the
            sleigh master.  The default value is 8765.

    PythonSleighName -- Name to use for monitoring/display purposes.
            The default value is the fully qualified domain name of
            the local machine.

    PythonSleighWorkerCount -- The total number of workers.

    PythonSleighRedirectOutput -- Specifies whether stdout and stderr
            should be redirected to a workspace variable.
            The default value is "True".

    """

    # note that xbool works quite differently than bool.
    # xbool('False') -> False, whereas bool('False') -> True
    xbool = lambda s: s.lower().strip() == 'true'
    args = {
        'PythonSleighNwsName': ('nwsName', str),
        'PythonSleighUserNwsName': ('userNwsName', str),
        'PythonSleighNwsHost': ('nwsHost', str),
        'PythonSleighNwsPort': ('nwsPort', int),
        'PythonSleighWorkerCount': ('maxWorkerCount', int),
        'PythonSleighName': ('name', str),
        'PythonSleighRank': ('rank', int),
        'PythonSleighRedirectOutput': ('redirectOutput', xbool),
    }
    kw = {'verbose': verbose}
    for e, a in args.items():
        try: kw[a[0]] = a[1](_Env[e])
        except KeyError: pass

    print >> sys.stderr, "Calling launch:", kw
    launch(**kw)

def launch(nwsName, nwsHost='localhost', nwsPort=8765,
        maxWorkerCount=-1, name=socket.getfqdn(), verbose=0,
        userNwsName='__default', rank=None, redirectOutput=True):
    """Execute sleigh tasks on behalf of the sleigh owner.

    launch(nwsName[, nwsHost, nwsPort, maxWorkerCount, name, verbose, userNwsName, redirectOutput])

    This function is executed from the sleigh worker script, via
    cmdLaunch, but it can also be executed manually, using a command
    that was copied from the value of the 'runMe' variable in the sleigh
    workspace (this is refered to as 'web launch').

    Arguments:

    nwsName -- Name of the sleigh workspace.

    nwsHost -- Host name of the NWS server used by the sleigh master.
            The default value is 'localhost'.

    nwsPort -- Port number of the NWS server used by the sleigh master.
            The default value is 8765.

    maxWorkerCount -- The maximum number of workers that should register
            themselves.  The default value is -1, which means there is
            no limit, but that primarily intended for support 'web
            launch'.

    name -- Name to use for monitoring/display purposes.  The default
            value is the fully qualified domain name of the local
            machine.

    rank -- worker rank, if we're using the new protocol

    verbose -- Boolean flag used for displaying debug messages.  The
            default value is 0.

    userNwsName -- Name of the user workspace.  The default value is
            '__default'.

    redirectOutput -- Boolean flag that indicates if stdout and stderr
            should be redirected to a workspace variable.

    """
    nws = NetWorkSpace(nwsName, nwsHost, nwsPort, useUse=True, create=False)
    userNws = nws.server.useWs(userNwsName, create=False)

    if platform:
        worker_info = {
                'host': platform.node(),
                'os': platform.system(),
                'pid': os.getpid(),
                'python': platform.python_version(),
                'nws': __version__,
                'rank': rank,
                'logfile': _Env.get('PythonSleighLogFile', '[unknown]')}
    else:
        worker_info = {
                'host': socket.getfqdn(),
                'os': sys.platform,
                'pid': os.getpid(),
                'python': sys.version,
                'nws': __version__,
                'rank': rank,
                'logfile': _Env.get('PythonSleighLogFile', '[unknown]')}

    if rank is not None:
        new_protocol = True
        nws.store('worker_ids', str(rank), metadata=worker_info)
    else:
        new_protocol = False
        rank = nws.fetch('rankCount')
        worker_info['rank'] = rank
        nws.store('worker_info', worker_info)
        if rank < 0:
            nws.store('rankCount', rank)
            raise SleighJoinException('too late to join worker group')
        elif maxWorkerCount >= 0 and rank + 1 >= maxWorkerCount:
            # I think this handles a maxWorkerCount of zero, but it
            # shouldn't happen unless the user calls launch manually.
            nws.store('workerCount', maxWorkerCount)
            nws.store('rankCount', -1)
        else:
            nws.store('rankCount', rank + 1)

    # initialize for monitoring
    displayName = '%s@%d' % (name, rank)
    nws.declare(displayName, 'single')
    nws.store(displayName, '0')
    nodeList = nws.fetch('nodeList')
    if not nodeList:
        nodeList = displayName
    else:
        nodeList += ' ' + displayName
    nws.store('nodeList', nodeList)

    nws.store('workerDebug', displayName + ': Updated node list.')

    # wait for all workers to join (plugin does not wait)
    # XXX: Should I still do this with the plugin?  I will answer your question
    #      with another question.  Do you feel like making fglob fetch
    #      workerCount lazily?
    workerCount = nws.find('workerCount')

    nws.store('workerDebug', displayName + ': Getting ready to prepare to begin calling the worker loop.')

    # enter the main worker loop
    try:
        _workerLoop(nws, displayName, rank, workerCount, verbose, userNws, redirectOutput)
    except:
        nws.store('workerDebug', displayName + ': Something is rotten, my friends: ' + str(sys.exc_info()))

class Logger(object):
    def __init__(self, ws, name, role):
        self._ws = ws
        self._name = name
        self._role = role

    def _logMsg(self, *s, **opt):
        try:
            msg = ' '.join([str(m) for m in s])
        except Exception, e:
            msg = 'Exception while logging: %s' % str(e)

        print >> sys.__stderr__, msg
        if self._ws:
            try:
                logmsg = '[%s] %s %s -- %s' % \
                        (time.asctime(), self._name, self._role, msg)
            except Exception, e:
                logmsg = '[%s] %s %s -- Exception while logging: %s' % \
                        (time.asctime(), self._name, str(e))
            self._ws.store(opt['var'], logmsg)

    def logError(self, *s):
        self._logMsg(*s, **{'var': 'logError'})

    def logDebug(self, *s):
        self._logMsg(*s, **{'var': 'logDebug'})

def _workerLoop(nws, displayName, rank, workerCount, verbose, userNws, redirectOutput):
    """Repeatedly execute tasks on behalf of the sleigh.

    _workerLoop(nws, displayName, rank, workerCount, verbose, userNws)

    Arguments:

    nws -- NetWorkSpace object for the sleigh workspace.

    displayName -- Name to use for monitoring/display purposes.

    rank -- The integer rank of this worker.  It acts as a unique ID
            within the sleigh.

    workerCount -- The total number of workers.  A negative value means
            the number is unknown.

    verbose -- Boolean flag used for displaying debug messages.

    userNws -- NetWorkSpace object for the user workspace.

    redirectOutput -- Boolean flag that indicates if stdout and stderr
            should be redirected to a workspace variable.

    """
    nws.store('workerDebug', displayName + ': Entering worker loop.')
    bx = 0

    # monitoring stuff here
    tasks = 0

    r = _Result()

    # create a global namespace
    fglob = {
        '__builtins__': globals()['__builtins__'],
        'SleighRank': rank,
        'SleighNws': nws,
        'SleighUserNws': userNws,
        'SleighWorkerCount': workerCount,
    }

    nws.store('workerDebug', displayName + ': Creating worker logger.')
    logger = Logger(nws, displayName, 'worker')

    if redirectOutput:
        f = nws.makefile('output@%d' % rank, 'w')
        sys.stdout = f
        sys.stderr = f

    # try to prevent Sleigh objects from being accidentally
    # created when modules are imported
    if not _Env.has_key('PythonSleighAllowed'):
        _Env['PythonSleighAllowed'] = 'FALSE'

    nws.store('workerDebug', displayName + ': Beginning task loop.')
    try:
        while 1:
            # update the number of tasks executed
            nws.store(displayName, str(tasks))

            # get a task
            if verbose: logger.logDebug(nws, 'waiting for a task')
            t = nws.fetch('task')
            if verbose: logger.logDebug('got task %s' % repr(t.tag))

            if not isinstance(t, _Task):
                # probably the sleigh modules are incompatible between
                # the master and this worker.  return the task, because
                # maybe some other workers are compatible.
                logger.logError('found bad task: exiting')
                nws.store('task', t)
                break

            r.tag = t.tag
            r.value = None
            r.error = False

            if t.type == 'define' or t.type == 'invoke':
                # define the function
                if t.code:
                    if t.funcName:
                        try:
                            # unpickle code object and put it into a function
                            obj = cPickle.loads(t.code)
                            f = new.function(obj, fglob)
                        except Exception, e:
                            r.value = 'Task definition from pickle failed: ' + str(e)
                            r.error = True
                            logger.logError(r.value)
                    elif t.methName:
                        try:
                            # unpickle the object and get the method
                            obj = cPickle.loads(t.code)
                            f = getattr(obj, t.methName)
                        except Exception, e:
                            r.value = 'Task definition from pickle failed: ' + str(e)
                            r.error = True
                            logger.logError(r.value)
                    else:
                        logger.logError('found corrupt task: ignoring')
                        continue
                else:
                    try:
                        # import the module
                        __import__(t.module)
                        module = sys.modules[t.module]

                        # add our global variables to the function's global
                        # namespace
                        module.SleighRank = rank
                        module.SleighNws = nws
                        module.SleighUserNws = userNws
                        module.SleighWorkerCount = workerCount

                        # get the function from the module
                        f = getattr(module, t.funcName)
                    except ImportError, e:
                        r.value = 'Unable to import module %s that defines worker ' \
                                  'function %s: %s' % \
                            (repr(t.module), repr(t.funcName), str(e))
                        r.error = True
                        logger.logError(r.value)
                    except AttributeError, e:
                        r.value = 'Unable to find worker function %s at ' \
                                  'top level of module %s: %s' % \
                            (repr(t.funcName), repr(t.module), str(e))
                        r.error = True
                        logger.logError(r.value)
                    except Exception, e:
                        try: exname = e.__class__.__name__
                        except: exname = '[unknown]'
                        r.value = 'Error getting worker function %s from ' \
                                  'module %s - %s: %s' % \
                            (repr(t.funcName), repr(t.module), exname, str(e))
                        r.error = True
                        logger.logError(r.value)

                if not r.error:
                    if t.type == 'define':
                        if t.funcName == _lambdaName:
                            # XXX might want to support a way to specify
                            # XXX an alternative name to use
                            fglob['SleighFunction'] = f
                        elif t.methName:
                            # XXX kind of weird...
                            fglob[t.methName] = f
                        else:
                            fglob[t.funcName] = f
                        r.value = None
                    else:
                        # execute the function (or method)
                        try:
                            r.value = f(*t.args)
                        except Exception, e:
                            tb = sys.exc_info()[2]
                            fname, lineno = _tb_info(tb)
                            try: exname = e.__class__.__name__
                            except: exname = '[unknown]'
                            r.value = 'Task invocation failed - %s: %s [%s:%d]' % \
                                    (repr(exname), str(e), fname, lineno)
                            r.error = True
                            logger.logError(r.value)
                            if verbose:
                                # check if there is an attribute that provides
                                # the "originating" exception
                                if hasattr(e, 'exception'):
                                    logger.logError(str(e.exception))
                                logger.logError(_tb_stack(tb))
            elif t.type == 'eval':
                try:
                    # prepare the global namespace
                    fglob['SleighArgs'] = t.args
                    try: del fglob['SleighValue']
                    except: pass

                    try:
                        # see if we can treat it as an expression
                        co = compile(t.code, '<string>', 'eval')
                    except:
                        # it's not an expression, so compile it for use
                        # with exec
                        co = compile(t.code, '<string>', 'exec')
                        exec co in fglob

                        # see if they want us to return a value
                        r.value = fglob.get('SleighValue')
                    else:
                        # it compiled for 'eval', so now execute it
                        r.value = eval(co, fglob)
                except Exception, e:
                    tb = sys.exc_info()[2]
                    fname, lineno = _tb_info(tb)
                    r.value = 'Task evaluation failed: %s [%s:%d]' % \
                            (str(e), fname, lineno)
                    r.error = True
                    logger.logError(r.value)
                    if verbose:
                        logger.logError(_tb_stack(tb))
            else:
                r.value = 'Task garbled: ' + t.type
                r.error = True
                logger.logError(r.value)

            # return the result
            try:
                if verbose: logger.logDebug('task %s completed' % repr(t.tag))
                nws.store('result', r)
            except Exception, e:
                # maybe a pickle error: try to return an error message
                r.value = 'Error returning result: ' + str(e)
                r.error = True
                logger.logError(r.value)
                nws.store('result', r)

            tasks += 1

            if t.barrier:
                if verbose: logger.logDebug('waiting at barrier')
                nws.find(_barrierNames[bx])
                bx ^= 1
    except Exception, e:
        # connection may corrupt, so just write to stderr
        sys.__stderr__.write('Worker exiting: %s\n' % str(e))
        # traceback.print_exc()

def getparam(name, opts, key, defkey='default', defval=None, coerce=str):
    try:
        p = opts[name]
        if isinstance(p, dict):
            try: p = p[key]
            except KeyError: p = p[defkey]
    except KeyError, e:
        if defval is None: raise e
        p = defval

    if coerce: p = coerce(p)

    return p

def sshcmd(machine, **kw):
    socket.gethostbyname(machine)
    if not machine:
        raise SleighIllegalValueException('empty string specified in nodeList')
    try: ssh = which('ssh')[0]
    except IndexError: ssh = '/usr/bin/ssh'
    user = getparam('user', kw, machine, coerce=None)
    if user:
        return [ssh, '-n', '-x', '-l', str(user), machine]
    else:
        return [ssh, '-n', '-x', machine]

def rshcmd(machine, **kw):
    socket.gethostbyname(machine)
    if not machine:
        raise SleighIllegalValueException('empty string specified in nodeList')
    try: rsh = which('rsh')[0]
    except IndexError: rsh = '/usr/bin/rsh'
    user = getparam('user', kw, machine, coerce=None)
    if user:
        return [rsh, machine, '-l', str(user), '-n']
    else:
        return [rsh, machine, '-n']

def sshforwardcmd(machine, **kw):
    # this only seems to make sense if:
    #   - we're ssh'ing to a different machine
    #   - nwsHostRemote is one of remote machines's interfaces
    #     (where localhost always is)
    socket.gethostbyname(machine)
    if not machine:
        raise SleighIllegalValueException('empty string specified in nodeList')
    r = '%s:%d:%s:%d' % \
            (kw['nwsHostRemote'], kw['nwsPortRemote'],
             kw['nwsHost'], kw['nwsPort'])
    try: ssh = which('ssh')[0]
    except IndexError: ssh = '/usr/bin/ssh'
    user = getparam('user', kw, machine, coerce=None)
    if user:
        return [ssh, '-n', '-x', '-R', r, '-l', str(user), machine]
    else:
        return [ssh, '-n', '-x', '-R', r, machine]

def rwincmd(machine, **kw):
    socket.gethostbyname(machine)
    if not machine:
        raise SleighIllegalValueException('empty string specified in nodeList')
    scriptDir = getparam('scriptDir', kw, machine, coerce=None)
    if scriptDir:
        vbscript = os.path.join(scriptDir, 'rwin.vbs')
    else:
        # scriptDir isn't set, so we'll try to guess where rwin.vbs is
        vbscript = _findScript('rwin.vbs')
    user = getparam('user', kw, machine, coerce=None)
    if not user:
        user = _Env.get('USER') or _Env.get('USERNAME') or _winuser or 'nwsuser'
    passwd = getparam('passwd', kw, machine, coerce=None)
    if not passwd:
        return ['cscript', '//nologo', vbscript, machine, '--']
    else:
        return ['cscript', '//nologo', vbscript, machine, '-l', user,
                '-p', passwd, '--']

def _pathSplit(p):
    s = []
    while True:
        p, b = os.path.split(p)
        if not b:
            if p:
                s.insert(0, p)
            break
        s.insert(0, b)
    return s

def _startsWith(x, y):
    for a, b in izip(x, y):
        if a != b:
            return False
    return True

# use heuristics to find the sleigh worker script
def _findScript(scriptName):
    # first try to find scriptName by looking at sys.executable
    exedrive, exepath = os.path.splitdrive(sys.executable)
    exelist = [exedrive] + _pathSplit(exepath)[:-1]  # remove file name, but include drive
    plist = [rule(exelist, *y) for pred, x, rule, y in _exeRules if pred(exelist, *x)]
    scriptPaths = which(scriptName, path=plist)
    if len(scriptPaths) > 0:
        scriptPath = scriptPaths[0]
    else:
        # next, try to find scriptName in your PATH
        scriptPaths = which(scriptName)
        if len(scriptPaths) > 0:
            scriptPath = scriptPaths[0]
            if len(scriptPaths) > 1:
                # don't give the warning if they're all the same file
                for p in scriptPaths[1:]:
                    if not os.path.samefile(scriptPath, p):
                        warn("found multiple copies of %s in PATH: using %s" % \
                                (scriptName, scriptPath))
                        break
        else:
            # finally, try to find scriptName in _scriptDirs
            scriptPaths = which(scriptName, path=_scriptDirs)
            if len(scriptPaths) > 0:
                scriptPath = scriptPaths[0]
            else:
                msg = 'unable to find %s in PATH: use Sleigh scriptDir option' % scriptName
                raise SleighScriptException(msg)

    return scriptPath

def envcmd(machine, *envVars, **kw):
    # use the 'env' command so the script won't have to set it's
    # own environment.
    try: envcmd = which('env')[0]
    except IndexError: envcmd = '/usr/bin/env'
    envcmd = getparam('env', kw, machine, defval=envcmd)

    interp = getparam('interp', kw, machine, defval='')
    scriptDir = getparam('scriptDir', kw, machine, coerce=None)
    scriptName = getparam('scriptName', kw, machine)

    if interp:
        interp = [interp]
    else:
        interp = []

    if scriptDir:
        scriptPath = os.path.join(scriptDir, scriptName)
    else:
        # they didn't tell us where to find the sleigh worker script
        # so we'll try to guess
        scriptPath = _findScript(scriptName)

    return [envcmd] + list(envVars) + interp + [scriptPath]

def scriptcmd(machine, *envVars, **kw):
    # execute the script directly, passing environment
    # settings as arguments.

    # this is an example of how a script can define its own optional
    # parameters that can be machine specific.
    interp = getparam('interp', kw, machine, defval=sys.executable)
    scriptDir = getparam('scriptDir', kw, machine, coerce=None)
    scriptName = getparam('scriptName', kw, machine)

    if scriptDir:
        scriptPath = os.path.join(scriptDir, scriptName)
    else:
        # they didn't tell us where to find the sleigh worker script
        # so we'll try to guess
        scriptPath = _findScript(scriptName)

    return [interp, scriptPath] + list(envVars)

def _getFuncInfo(fun):
    stacklevel = 4 # called from _newtask, eachElem
    funname = fun.__name__

    try:
        modname = fun.__module__
    except AttributeError:
        # should only be necessary in pre-python 2.3
        modname = pickle.whichmodule(fun, funname)

    try:
        module = sys.modules[modname]
    except KeyError:
        warn('%s is not defined in an imported module' % funname,
                stacklevel=stacklevel)
        module = None

    if modname == '__main__':
        try:
            # try to get the name of the main program
            modpath, ext = os.path.splitext(sys.modules['__main__'].__file__)
            if ext == '.py':
                modname = os.path.basename(modpath)
            else:
                # can't import it if it doesn't have the '.py' extension
                warn('worker function defined in file without .py extension',
                        stacklevel=stacklevel)
                warn('attempting to pickle the function code',
                        stacklevel=stacklevel)
                modname = None
        except AttributeError:
            # __file__ attribute isn't defined when running interactively
            modname = None
            warn('cannot determine module from which to import %s' % funname,
                    stacklevel=stacklevel)
            warn('attempting to pickle the function code',
                    stacklevel=stacklevel)

    if modname and module and not hasattr(module, funname):
        # don't issue warning for lambdas
        if funname != _lambdaName:
            warn('module %s has no attribute named %s' % (modname, funname),
                    stacklevel=stacklevel)
            warn('attempting to pickle the function code',
                    stacklevel=stacklevel)
        modname = None

    return modname, funname

# the following three functions are used for operating on
# the accumulator objects
def _accumresult(accum, results):
    try:
        try:
            accum.result(results)
        except AttributeError:
            accum(results)
    except Exception:
        traceback.print_exc()
        tags = ', '.join([str(r[0]) for r in results])
        warn('error calling accumulator: result %s is lost' % tags,
                stacklevel=3)

def _accumerror(accum, results):
    try:
        try:
            accum.error(results)
        except AttributeError:
            try:
                accum.result(results)
            except AttributeError:
                accum(results)
    except Exception:
        traceback.print_exc()
        tags = ', '.join([str(r[0]) for r in results])
        warn('error calling accumulator: result %s is lost' % tags,
                stacklevel=3)

def _accumvalue(accum):
    try:
        try:
            return accum.value()
        except AttributeError:
            return None
    except Exception:
        traceback.print_exc()
        warn('error calling value method of accumulator: returning None',
                stacklevel=3)
        return None

# this is used by eachWorker, eachElem, etc, to create a new _Task object
def _newtask(fun, taskType, barrier):
    if not isinstance(taskType, str):
        raise SleighIllegalValueException('"type" parameter must be a string')

    taskType = taskType.strip().lower()

    if taskType not in ['invoke', 'eval', 'define']:
        raise SleighIllegalValueException(
                '"type" parameter must be "invoke", "eval", or "define"')

    if isinstance(fun, str):
        if taskType != 'eval':
            raise SleighIllegalValueException(
                    '"type" parameter must be "eval" for a string')
        else:
            # this may raise a SyntaxError
            fun = fun.lstrip()
            co = compile(fun, '<imap_function_string>', 'exec')
    elif isinstance(fun, (FunctionType, BuiltinFunctionType, MethodType)):
        if taskType == 'eval':
            raise SleighIllegalValueException(
                    '"fun" argument must be a string if the task type is "eval"')
        if isinstance(fun, MethodType):
            # I think this is necessary
            if not hasattr(fun, '__module__'):
                raise SleighIllegalValueException(
                        '"fun" argument is method without __module__ defined')
            if fun.__module__ == '__main__':
                raise SleighIllegalValueException(
                        '"fun" argument is defined in module "__main__"')
    else:
        raise SleighIllegalValueException(
                '"fun" argument must be a function or a string')

    t = _Task()
    t.barrier = barrier
    t.type = taskType

    if taskType == 'eval':
        t.code = fun
        t.funcName = '<eval_string>'
    elif hasattr(fun, 'im_self') and hasattr(fun, 'im_func'):
        t.module = fun.__module__
        if fun.im_self is None:
            raise SleighIllegalValueException(
                    '"fun" argument cannot be an unbound method')
        t.code = cPickle.dumps(fun.im_self)
        t.methName = fun.im_func.__name__
    else:
        t.module, t.funcName = _getFuncInfo(fun)
        # XXX we should require permission to do this
        if not t.module or t.funcName == _lambdaName:
            # try to pickle it
            t.code = cPickle.dumps(fun.func_code)

    return t

defaultSleighOptions = {
    'nwsHost':          socket.getfqdn(),
    'nwsHostRemote':    None,
    'nwsPort':          8765,
    'nwsPortRemote':    None,
    'outfile':          None,
    'launch':           'local',
    'workerCount':      3,
    'nodeList':         ['localhost', 'localhost', 'localhost'],
    'scriptExec':       (os.name == 'posix') and envcmd or scriptcmd,
    'scriptDir':        None,
    'scriptName':       'PythonNWSSleighWorker.py',
    'modulePath':       None,
    'workingDir':       os.getcwd(),
    'logDir':           None,
    'user':             None,
    'passwd':           None,
    'wsNameTemplate':   'sleigh_ride_%04d',
    'userWsNameTemplate': 'sleigh_user_%04d',
    'verbose':          False,
    'redirectOutput':   True,
}

class Barrier(object):
    def __init__(self, ws, name):
        self.ws   = ws
        self.name = name

    def leave(self, block=False):
        if block:
            self.ws.fetch(self.name)
        else:
            self.ws.fetchTry(self.name)

    def enter(self):
        self.ws.store(self.name, '1')

    def wait(self):
        self.ws.find(self.name)

class NullBarrier(object):
    def __init__(self):
        pass

    def leave(self, block=False):
        pass

    def enter(self):
        pass

    def wait(self):
        pass

class Accumulator(object):
    def __init__(self):
        self._val = {}

    def error(self, results):
        for tag, val in results:
            if self._val.has_key(tag):
                warn('got repeat result for task %d' % tag)
            else:
                self._val[tag] = SleighTaskException(val)

    def result(self, results):
        for tag, val in results:
            if self._val.has_key(tag):
                warn('got repeat result for task %d' % tag)
            else:
                self._val[tag] = val

    def value(self):
        return [self._val[i] for i in xrange(len(self._val))]

class SleighPending(object):

    """Represents a sleigh eachWorker/eachElem invocation in progress.

    This is returned from the eachElem and eachWorker operations
    when executed asynchronously.  It allows you to check for
    the number of tasks left to be executed, and to wait for
    the results of the operation to be returned.
    """

    def __init__(self, nws, id, numTasks, barrier, my_sleigh, accumulator, bcastTask=False):
        """Create an object that represents the pending sleigh operation.

        This constructor is intended for internal use only.

        Arguments:

        nws -- Sleigh NetWorkSpace object.

        numTasks -- Number of tasks that were submitted.

        barrierName -- Name of the barrier to wait at when complete.

        sleighState -- Object representing the current state of the
                sleigh.

        accumulator -- Function to call with results as they arrive.
        
        bcastTask -- True if this is a broadcast task, false otherwise

        """
        self.nws = nws
        self.id = id
        self.numTasks = numTasks
        self.barrier = barrier
        self.my_sleigh = my_sleigh
        self.accumulator = accumulator
        self.done = False
        self.bcastTask = bcastTask

    def __str__(self):
        return "SleighPending [%s]" % str(self.nws)

    def check(self):
        """Return the number of tasks still outstanding.

        p.check() -> integer

        """
        if self.done: return 0 # could argue either way here... .

        # look for the count of result values.
        for l in self.nws.listVars().split('\n'):
            if l.startswith('result\t'):
                return self.numTasks - int(l.split('\t')[1])

        # didn't find the variable 'result' --- assume no results have
        # yet been generated.
        return self.numTasks

    def wait(self):
        """Wait for and return the list of results.

        p.wait() -> list

        """
        # note: a lot of code is duplicated here and in the
        # non-blocking sections of eachWorker and eachElem. refactor?

        if self.done:
            raise SleighGatheredException('results already gathered')

        self.my_sleigh._Sleigh__retrieveResults(self.id, self.numTasks, self.accumulator, self.bcastTask)

        if self.barrier is not None: self.barrier.enter()

        self.my_sleigh.state.occupied = False
        self.done = True

        return _accumvalue(self.accumulator)

class Sleigh(object):

    """Represents a collection of python processes used to execute tasks.

    The sleigh allows python functions, methods, and expressions to be
    executed in parallel using the eachElem and eachWorker methods.

    The sleigh workers are started when the Sleigh object is
    constructed.  When tasks are submitted to the sleigh, using the
    eachWorker and eachElem methods, the workers will execute the tasks,
    and return the results.  When the stop method is called, the workers
    are stopped.

    Note that a given python program can create multiple Sleigh objects,
    which will each have it's own set of workers.  This could be useful
    if tasks have different requirements.  For example, you could create
    a Linux sleigh and a Windows sleigh, and submit Excel tasks only to
    your Windows sleigh.
    """

    def __init__(self, *deprecated, **kw):
        """Start the remote python processes used to execute tasks.

        Keyword arguments:

        launch -- Specifies the method of starting workers.  If this
                argument is set to the string 'local', then the workers
                are executed on the local machine.  If it is set to the
                string 'web', then web launching is used.  Otherwise,
                the launch argument must specify a function (such as
                nws.sleigh.sshcmd) that returns a list of command
                arguments use to execute the workers on the machines
                specified by the nodeList argument.  The default value
                is 'local'.

        workerCount -- Number of workers to start if the launch argument
                is set to 'local' (which is the default value of launch).
                This argument is ignored if launch is not set to
                'local'.  The default value is 3.

        nodeList -- List of hosts on which to execute workers, if the
                launch argument is set to a function.  This argument is
                ignored if launch is set to 'local' or 'web'.  The
                default value is ['localhost', 'localhost',
                'localhost'].

        nwsHost -- Host name of the machine where the NWS server is
                executing.

        nwsPort -- Port to connect to the NWS server.

        nwsHostRemote -- Host name of the machine that workers should
                use to connect to the NWS server.  This is useful in
                conjunction with the sshforwardcmd function (see the
                description of scriptExec).  The default is the value of
                the nwsHost argument.

        nwsPortRemote -- Port that workers should use to connect to the
                NWS server.  This is useful in conjunction with the
                sshforwardcmd function (see the description of
                scriptExec).  The default is the value of the nwsPort
                argument.

        scriptExec -- Python function returning a list of command
                arguments to execute the worker script.  This list of
                command arguments is appended to the list returned by
                the launch function.  The default value is the envcmd
                function (defined in this module), which uses the
                standard 'env' command to execute the script with the
                appropriate environment for the worker.

        scriptDir -- Directory on the worker that contains the execution
                script.

        scriptName -- Name of the script that executes the worker on the
                remote machines.  This defaults to
                PythonNWSSleighWorker.sh on Unix, and
                PythonNWSSleighWorker.py on Windows.

        modulePath -- Directory path to add to sys.path on workers.
                This is often useful for giving the workers access to
                python modules that define the python function to be
                executed by eachWorker or eachElem.  The default value
                is None.

        workingDir -- Directory path to use as the current working
                directory for the workers.  The default value is the
                current working directory of the sleigh master.

        logDir -- Directory in which to create the worker log files.
                The default value is None, which leaves the decision
                to the sleigh worker scripts, which generally uses the
                remote system's temporary directory.

        user -- User name to use for remote execution of worker.  This
                argument may be ignored, depending the specified
                launch function.  The default is None, which allows the
                remote execution mechanism decide.

        wsNameTemplate -- Template for the sleigh workspace name.  This
                must be a legal 'format' string, containing only an
                integer format specifier.  The default is
                'sleigh_ride_%04d'.

        userWsNameTemplate -- Template for the user's workspace name.
                This must be a legal 'format' string, containing only
                an integer format specifier.  The default is
                'sleigh_user_%04d'.

        verbose -- Boolean flag used for displaying debug messages.
                Debug messages will be sent to stderr.  This will also
                cause the worker processes to write debug messages to
                files prefixed with 'sleigh_ride' in their current
                working directory (as controled by the workerDir
                argument).  The default value is False.

        redirectOutput -- Boolean flag used to control whether messages
                written by the worker functions to stdout and stderr
                should be redirected to a workspace variable.  If False,
                they will go to the output file if verbose is True,
                otherwise to the null device.  The default value is True.

        """
        # sanity check the keyword arguments
        unrecog = [a for a in kw.keys() if a not in defaultSleighOptions.keys()]
        if unrecog:
            raise TypeError(
                    "__init__() got an unexpected keyword argument '%s'" % \
                    unrecog[0])

        # check for a recursive sleigh construction
        if _Env.get('PythonSleighAllowed', 'TRUE') == 'FALSE':
            raise SleighNotAllowedException(
                    'Sleigh construction not allowed '
                    '(probably accidentally called from sleigh worker)')

        # join related
        self.rankCount = 0

        # make a copy of the default sleigh options
        self.options = dict(defaultSleighOptions)

        # environment variables higher precedence than defaults
        try: self.options['nwsHost'] = _Env['PythonSleighNwsHost']
        except: pass
        try: self.options['nwsPort'] = int(_Env['PythonSleighNwsPort'])
        except: pass

        # keyword arguments to constructor have highest precedence
        self.options.update(kw)

        # now that everyone has been heard from, if nwsHostRemote and
        # nwsPortRemote still aren't set, set them to the values of
        # nwsHost and nwsPort respectively
        if not self.options['nwsHostRemote']:
            self.options['nwsHostRemote'] = self.options['nwsHost']

        if not self.options['nwsPortRemote']:
            self.options['nwsPortRemote'] = self.options['nwsPort']

        opts = self.options

        # error check the options so we can throw any exceptions before
        # we've created the sleigh workspace, or caused any other side
        # effects.
        if not isinstance(opts['launch'], FunctionType) and \
                opts['launch'] not in ('web', 'local', 'service'):
            raise SleighIllegalValueException('unknown launch protocol: ' +
                                              str(opts['launch']))

        # check if a node list has been specified in the old way
        if len(deprecated) > 1:
            raise TypeError('__init__() takes exactly 1 argument (%d given)' % len(deprecated))
        elif len(deprecated) == 1:
            if type(deprecated[0]) == type([]):
                warn('nodeList should be passed using the nodeList keyword', stacklevel=2)
                opts['nodeList'] = deprecated[0]
                if not isinstance(opts['launch'], FunctionType):
                    warn('nodeList not used when launch = ' + opts['launch'], stacklevel=2)
            else:
                raise TypeError('__init__() takes exactly 1 argument (2 given)')

        try:
            socket.gethostbyname(opts['nwsHost'])
        except socket.gaierror, e:
            raise SleighIllegalValueException('bad nwsHost value "%s": %s'
                    % (opts['nwsHost'], e.args[1]))

        # set up the sleigh's netWorkSpace.
        self.nwss = NwsServer(host=opts['nwsHost'], port=opts['nwsPort'], connopts={'MetadataToServer': '1', 'MetadataFromServer': '1'})

        # last minute sanity checking of nodeList when using service launch
        if opts['launch'] == 'service':
            self.service = self.nwss.useWs('PySleighService', create=False)
            workers = self.service.listVars(format=DICT)
            for node in opts['nodeList']:
                try:
                    w = workers[node]
                    if w[V_MODE] != FIFO:
                        self.nwss.close()
                        raise SleighIllegalValueException('variable for node %s has illegal mode: %s'
                             % (node, w[V_MODE]))
                    if w[V_VALUES] > 0:
                        warn('pending requests for node %s' % node, stacklevel=2)
                    if w[V_FETCHERS] == 0:
                        warn('worker may not be running on node %s' % node, stacklevel=2)
                    elif w[V_FETCHERS] > 1:
                        warn('multiple workers for node %s' % node, stacklevel=2)
                    if w[V_FINDERS] > 0:
                        warn('unknown finders present for node %s' % node, stacklevel=2)
                except KeyError:
                    self.nwss.close()
                    raise SleighIllegalValueException('bad node %s in nodeList: %s'
                            % (node, 'no such worker registered'))

        self.nwsName = self.nwss.mktempWs(opts['wsNameTemplate'], metadata={'wstype': 'sleigh'})
        if not self.nwsName:
            self.nwss.close()
            raise SleighIllegalValueException('bad wsNameTemplate value "%s": %s'
                    % (opts['nwsHost'], e.args[1]))
        self.nws = self.nwss.openWs(self.nwsName)

        self.userNwsName = self.nwss.mktempWs(opts['userWsNameTemplate'])
        if not self.userNwsName:
            self.nwss.close()
            raise SleighIllegalValueException('bad userWsNameTemplate value "%s": %s'
                    % (opts['nwsHost'], e.args[1]))
        self.userNws = self.nwss.openWs(self.userNwsName)

        self.__initialize(self.nwss.get_protocol_version())
        self.__launch_workers(opts)

        self.state =  _SleighState()
        self.state.bx = 0
        self.state.occupied = False
        self.state.stopped = False
        self.state.totalTasks = 0

    def __launch_workers(self, opts):
        if isinstance(opts['launch'], FunctionType):
            # remote launch
            self.nodeList = opts['nodeList']
            self.workerCount = len(self.nodeList)

            for i in xrange(self.workerCount):
                if opts['verbose']:
                    opts['outfile'] = '%s_%04d.txt' % (self.nwsName, i)
                self._addWorker(self.nodeList[i], i)
        elif opts['launch'] == 'service':
            # remote launch using the "Python Sleigh Service"
            self.nodeList = opts['nodeList']
            self.workerCount = len(self.nodeList)

            b = lambda x: x and str(x) or ''  # turn None into ''
            for i in xrange(self.workerCount):
                if opts['verbose']:
                    opts['outfile'] = '%s_%04d.txt' % (self.nwsName, i)
                # XXX is '@' the best delimiter?
                # XXX this should include userNwsName
                request = "@%s@%d@%d@%s@%s@%s@%s@%s" % \
                        (self.nwsName,
                         self.workerCount,
                         i,
                         b(opts['workingDir']),
                         b(opts['outfile']),
                         b(opts['logDir']),
                         b(opts['user']),
                         b(opts['modulePath']))
                if opts['verbose']:
                    print 'command:', request

                # XXX error check to make sure the worker is listed in workspace?
                # XXX or issue a warning message?
                self.service.store(self.nodeList[i], request)
        elif opts['launch'] == 'local':
            # local launch
            self.workerCount = int(opts['workerCount'])
            self.nodeList = self.workerCount * ['localhost']

            for i in xrange(self.workerCount):
                if opts['verbose']:
                    opts['outfile'] = '%s_%04d.txt' % (self.nwsName, i)
                self._addWorker(self.nodeList[i], i)
        elif opts['launch'] == 'web':
            # web launch
            self.nws.store('runMe',
                    'import nws.sleigh; nws.sleigh.launch(%s, %s, %s, userNwsName=%s)' % \
                    (repr(self.nwsName), repr(opts['nwsHost']),
                        repr(opts['nwsPort']), repr(self.userNwsName)))

            # store some more values (probably only one) using
            # dotted-decimal ip addresses in case there are host name
            # resolution problems on the worker.
            try:
                ipaddrs = socket.gethostbyname_ex(opts['nwsHost'])[2]
                for ipaddr in ipaddrs:
                    self.nws.store('runMe',
                            'import nws.sleigh; nws.sleigh.launch(%s, %s, %s, userNwsName=%s)' % \
                            (repr(self.nwsName), repr(ipaddr),
                                repr(opts['nwsPort']), repr(self.userNwsName)))
            except:
                # this seems highly unlikely to happen, but it seems
                # wrong to allow something optional to blow us up
                pass

            try: self.nws.fetch('deleteMeWhenAllWorkersStarted')
            except: pass

            self.nws.deleteVar('runMe')
            self.workerCount = self.nws.fetch('rankCount')
            if not self.new_protocol:
                self.nws.store('workerCount', self.workerCount)
                self.nws.store('rankCount', -1)
                self.rankCount = -1
                self.nodeList = self.nws.fetchTry('nodeList', '')
        else:
            # the previous error checking should prevent this from
            # ever happening
            warn("internal error: unknown launch protocol: " + str(opts['launch']))
            self.workerCount = 0
            self.rankCount = -1
            self.nodeList = ''

    def __initialize(self, proto_version):
        v = self.nws.findTry('version')
        if v is None:
            proto_version = PROTOCOL_VERSION_OLD

        if proto_version == PROTOCOL_VERSION_OLD:
            self.__initialize_old()
        elif proto_version == PROTOCOL_VERSION_NEW_0:
            self.__initialize_new_0()
        else:
            raise Exception('Unknown protocol version')

    def __initialize_old(self):
        self.new_protocol = False

        # initialize for monitoring
        self.nws.declare('nodeList', 'single')
        self.nws.store('nodeList', '')
        self.nws.declare('totalTasks', 'single')
        self.nws.store('totalTasks', '0')

        self.nws.declare('rankCount', 'single')
        self.nws.store('rankCount', 0)
        self.nws.declare('workerCount', 'single')

        self.__statusImpl = self.__status_old
        self._syncWorkerCount = self.__syncWorkerCount_old
        self.__storeBroadcastTask = self.__storeBroadcastTask_old
        self.__newBatch = lambda: ''
        self.__barriers = [Barrier(self.nws, _barrierNames[i]) for i in range(2)]

    def __initialize_new_0(self):
        self.nws.declare('nodeList', 'single')
        self.nws.store('nodeList', '')
        self.__nextBatch = 0
        self.new_protocol = True
        if self.options['launch'] != 'web':
            self.nws.store('workerCount', '%d' % len(self.options['nodeList']))
        self.__statusImpl = self.__status_new_0
        self._syncWorkerCount = self.__syncWorkerCount_new_0
        self.__storeBroadcastTask = self.__storeBroadcastTask_new_0
        self.__newBatch = self.__newBatch_new
        self.__barriers = [NullBarrier() for i in range(2)]

    def __newBatch_new(self):
        id = '%d' % self.__nextBatch
        self.__nextBatch += 1
        return id

    def __storeBroadcastTask_old(self, wc, task):
        # update the total number of submitted tasks
        self.state.totalTasks += wc
        self.nws.store('totalTasks', str(self.state.totalTasks))

        for i in xrange(wc):
            task.tag = i
            self.nws.store('task', task)

        # no batch ids in old versions
        return ''

    def __storeBroadcastTask_new_0(self, wc, task):
        id = self.__newBatch()
        self.nws.store('broadcast', task, metadata={'batchId': id})
        return id

    def __retrieveResults(self, id, ntasks, accum, ignoreTag = False):
        # Ignore tag is to allow for eachWorker calls, where a broadcast task
        # is translated into multiple tasks with identical tags.
        for i in xrange(ntasks):
            r = self.nws.fetch('result', metadata={'batchId': id})
            if ignoreTag:
                r.tag = i
            if r.error:
                _accumerror(accum, [(r.tag, r.value)])
            else:
                _accumresult(accum, [(r.tag, r.value)])

    def __str__(self):
        return "Sleigh [%s] on nodes: %s" % \
            (str(self.nws), " ".join(self.nodeList))

    def _addWorker(self, machine, id):
        # basic idea is (or should be): if we can get the appropriate
        # sleighworker script running on the remote node, we
        # just need to give it enough env info to take care of the rest

        # prepare the environment
        envVars = []
        envVars.append('PythonSleighNwsHost=' + self.options['nwsHostRemote'])
        envVars.append('PythonSleighNwsName=' + self.nwsName)
        envVars.append('PythonSleighUserNwsName=' + self.userNwsName)
        envVars.append('PythonSleighNwsPort=' + str(self.options['nwsPortRemote']))
        if self.options['workingDir']:
            envVars.append('PythonSleighWorkingDir=' + self.options['workingDir'])
        if self.options['scriptDir']:
            envVars.append('PythonSleighScriptDir=' + self.options['scriptDir'])
        envVars.append('PythonSleighScriptName=' + self.options['scriptName'])
        if self.options['modulePath']:
            envVars.append('PythonSleighModulePath=' + self.options['modulePath'])
        if self.options['outfile']:
            envVars.append('PythonSleighWorkerOut=' + self.options['outfile'])
        if self.options['logDir']:
            envVars.append('PythonSleighLogDir=' + self.options['logDir'])
        envVars.append('PythonSleighName=' + machine)
        envVars.append('PythonSleighWorkerCount=' + str(self.workerCount))
        envVars.append('PythonSleighID=' + str(id))
        envVars.append('PythonSleighRedirectOutput=' + str(self.options['redirectOutput']))

        # remote execution command
        if self.options['launch'] == 'local':
            argv = []

            # this environment variable is only needed and used by
            # PythonNWSSleighWorker.sh, not the python version
            envVars.append('PythonProg=' + sys.executable)
        else:
            argv = self._getargs('launch',
                    self.options['launch'],
                    machine,
                    **self.options)

        # environment setting command
        argv += self._getargs('scriptExec',
                self.options['scriptExec'],
                machine,
                *envVars,
                **self.options)

        # now actually execute the worker on the remote machine
        if not os.path.isabs(argv[0]):
            try:
                argv[0] = which(argv[0])[0]
            except IndexError:
                warn("unable to convert %s to absolute path" % argv[0], stacklevel=3)

        try:
            if self.options['verbose']:
                print 'command:', argv
            subprocess.Popen(argv)
        except NameError:
            app = argv[0]
            if sys.platform.startswith('win'):
                warn("Python 2.4 is _strongly_ recommended on Windows", stacklevel=3)
                argv = [msc_quote(a) for a in argv]
                if self.options['verbose']:
                    print 'MS C quoted command:', argv
            os.spawnv(os.P_NOWAIT, app, argv)

    def _getargs(self, name, cmd, *args, **kw):
        if not isinstance(cmd, FunctionType):
            raise SleighIllegalValueException(name + ' must be set to a function')
        x = cmd(*args, **kw)
        if not isinstance(x, list):
            raise SleighIllegalValueException(name + ' does not yield a list')
        for i in x:
            if not isinstance(i, str):
                raise SleighIllegalValueException(name + ' does not yield a list of strings')
        return x

    def status(self, closeGroup=False, timeout=0.0):
        """Return the status of the worker group.

        s.status(closeGroup, timeout) -> numworkers, closed

        The status includes the number of workers that have joined the
        group so far, and a flag that indicates whether the group has
        been closed (meaning that no more workers can join).  Normally,
        the group is automatically closed when all the workers that were
        listed in the constructor have joined.  However, this method
        allows you to force the group to close after the timeout
        expires.  This can be particularly useful if you are running
        on a large number of nodes, and some of the nodes are slow or
        unreliable.  If some of the workers are never started, the group
        will never close, and no tasks will ever execute.

        Arguments:

        closeGroup -- Boolean flag indicating whether to close the
                group.  If True, the group will be closed, after the
                specified timeout.  The default value is False.

        timeout -- Number of seconds to wait for the group to close
                before returning.  The default value is 0.0.

        """
        if self.rankCount < 0:
            # join phase completed before
            return self.workerCount, 1
        else:
            return self.__statusImpl(closeGroup, timeout)

    def __status_new_0(self, closeGroup, timeout):
        if closeGroup:
            self.workerCount = self.nws.fetch('status', metadata={'delay': str(timeout)})
            self.rankCount = -1
            return self.workerCount, 1
        else:
            status = self.nws.fetch('join_status')
            if status == 'closed':
                self.workerCount = self.nws.fetch('status', metadata={'delay': str(timeout)})
                self.rankCount = -1
                return self.workerCount, 1
            else:
                return self.nws.fetch('waitForWorkers', metadata={'delay': str(timeout)}), 0

    def __status_old(self, closeGroup, timeout):
        sleepTime = initialSleep = min(2.0, timeout)
        repeatedSleep = 1.0
        startTime = time.time()

        while 1:
            n = self.nws.fetch('rankCount')
            if n < 0:
                # all workers joined
                if self.options['verbose']:
                    print 'all %d workers have started' % self.workerCount

                assert self.workerCount == self.nws.find('workerCount')
                self.rankCount = -1
                self.nws.store('rankCount', self.rankCount)
                return self.workerCount, 1
            else:
                # three choices: close now, return not closed, or
                # sleep and try again
                if time.time() - startTime >= timeout:
                    # time is up, so either close the join, or
                    # return the current status
                    if closeGroup:
                        if self.options['verbose']:
                            print 'closing group: %d workers' % n

                        self.workerCount = n
                        self.nws.store('workerCount', self.workerCount)
                        self.rankCount = -1
                        self.nws.store('rankCount', self.rankCount)
                        return self.workerCount, 1
                    else:
                        if self.options['verbose']:
                            print 'group not formed: %d workers' %n

                        self.rankCount = n
                        self.nws.store('rankCount', self.rankCount)
                        return self.rankCount, 0
                else:
                    if self.options['verbose']:
                        print 'only %d workers: sleeping...' % n

                    self.rankCount = n
                    self.nws.store('rankCount', n)
                    time.sleep(sleepTime)
                    sleepTime = repeatedSleep

    def stop(self):
        """Stop the remote processes and delete the sleigh workspace.

        s.stop()

        """
        if self.state.stopped:
            return

        self.nws.store('Sleigh ride over', 1)

        if self.options['launch'] != 'web':
            time.sleep(3)
            exitCount = 0
            while self.nws.fetchTry('bye', None) != None: exitCount += 1
            if exitCount < self.workerCount:
                warn('only %d of %d workers have exited' % \
                        (exitCount, self.workerCount), stacklevel=2)

        self.nwss.deleteWs(self.nwsName)
        self.nwss.close()
        self.state.stopped = True

    def __syncWorkerCount_old(self):
        if self.rankCount != -1:
            self.rankCount = self.nws.find('rankCount')
            if self.rankCount == -1:
                self.workerCount = int(self.nws.find('workerCount'))
        return self.workerCount

    def __syncWorkerCount_new_0(self):
        if self.rankCount != -1:
            self.workerCount = int(self.nws.find('workerCount'))
        return self.workerCount

    def eachWorker(self, fun, *workerArgs, **kw):
        """Execute a function, method, or expression on each worker of sleigh.

        s.eachWorker(fun[, ...]) -> list or SleighPending

        The results are normally returned as a list, unless
        the blocking arguments is set to False, in which case, a
        SleighPending object is returned.

        Arguments:

        fun -- Function, method, or python expression to execute.  To
                execute a method, you must specify a bound method
                object.  If the function or defining class is defined in
                a module, the workers will attempt to import that
                module.  If that module isn't available to the worker
                (because it's a non-standard module, not in the
                PYTHONPATH), then the worker is not be able to execute
                those tasks.

                To execute a python expression, you must specify it as a
                string.  Leading whitespace is automatically stripped to
                avoid a common source of python syntax errors.

        Optional arguments:

        *workerArgs -- Arguments to pass to the function or method.
                Specify whatever arguments the function requires,
                including no arguments.  The exact same set of arguments
                will be used for each worker (unlike eachElem).  For a
                python expression, these arguments are passed to the
                expression as a global variable named 'SleighArguments'.

        Keyword arguments:

        type -- Indicates the type of function invocation to perform.
                This can be either 'invoke', 'define', or 'eval'.
                If the fun argument is a function or bound method, then
                the default value is 'invoke'.  If the fun argument is
                a string, then the default value is 'eval' (a value of
                'invoke' or 'define' is illegal for python expressions).

        blocking -- A boolean value that indicates whether to wait for
                the results, or to return as soon as the tasks have been
                submitted.  If set to False, eachWorker will return a
                SleighPending object that is used to monitor the status
                of the tasks, and to eventually retrieve the results.
                You must wait for the results to be complete before
                executing any further tasks on the sleigh, or a
                SleighOccupiedException will be raised.

                This argument is important if you want the master to be
                able to interact/communicate with the workers, via NWS
                operations, for example.  This allows you to implement
                more complex parallel or distributed programs.

                The default value is True.

        accumulator -- A function or callable object that will be called
                for each result as they arrive.  The first argument to
                the function is a list of result values, and the second
                argument is a list of indexes, which identifies which task.

                The arguments to the accumulator function are lists since
                in the future, we plan to allow tasks to be "chunked" to
                improve performance of small tasks.

                Note that bound methods can be very useful accumulators.


        """
        if self.state.occupied:
            raise SleighOccupiedException('sleigh is occupied')

        if self.state.stopped:
            raise SleighStoppedException('sleigh is stopped')

        blocking = kw.get('blocking', True)
        accumulator = kw.get('accumulator', Accumulator())
        if isinstance(fun, str):
            taskType = kw.get('type', 'eval')
        else:
            taskType = kw.get('type', 'invoke')

        # verify that accumulator is a callable object or an object with a result method
        if not callable(accumulator) and not hasattr(accumulator, 'result'):
            raise SleighIllegalValueException('accumulator must be callable or have a callable result attribute')

        t = _newtask(fun, taskType, False)
        
        # Give it the standard (see R client) dummy tag number
        t.tag = 99999

        self._syncWorkerCount()

        nws = self.nws
        wc = self.workerCount

        # use alternating barrier to sync eachWorker invocations with the workers.
        cur_barrier = self.__barriers[self.state.bx]
        self.state.bx ^= 1

        t.args = list(workerArgs)
        id = self.__storeBroadcastTask(wc, t)

        if not blocking:
            self.state.occupied = not self.new_protocol
            return SleighPending(nws, id, wc, cur_barrier, self, accumulator, bcastTask=True)

        self.__retrieveResults(id, wc, accumulator, ignoreTag = True)
        return _accumvalue(accumulator)

    # run fun once for each element of a vector.
    def eachElem(self, fun, elementArgs=[[]], fixedArgs=[], **kw):
        """Execute a function, method, or expression for each element in
        the specified list(s).

        s.eachElem(fun, elementArgs[, fixedArgs]) -> list or SleighPending

        The results are normally returned as a list, unless the blocking
        arguments is set to False, in which case, a SleighPending object
        is returned.

        Arguments:

        fun -- Function, method, or python expression to execute.  To
                execute a method, you must specify a bound method
                object.  If the function or defining class is defined in
                a module, the workers will attempt to import that
                module.  If that module isn't available to the worker
                (because it's a non-standard module, not in the
                PYTHONPATH), then the worker is not be able to execute
                those tasks.

                To execute a python expression, you must specify it as a
                string.  Leading whitespace is automatically stripped to
                avoid a common source of python syntax errors.

        elementArgs -- List of arguments to pass to the function or method
                that need to be different for different tasks.  In
                general, this is a list of iterable objects, such as lists,
                each containing the values to use for a given argument of
                the different tasks.

                If your function needs only one varying argument of a
                simple type, you can specify it without the outer list.

                Note that for a python expression, the list of arguments
                is passed to the expression as a global variable named
                'SleighArguments'.

        fixedArgs -- List of additional arguments that are
                fixed/constant for each task.  Normally, they are
                appended to the arguments specified by elementArgs, but
                the order can be altered using the argPermute argument
                described below.

                The default value is an empty list, which means that no
                extra arguments are passed to the function.

                Note that for a python expression, the list of arguments
                is passed to the expression as a global variable named
                'SleighArguments'.

        Keyword arguments:

        type -- Indicates the type of function invocation to perform.
                This can be either 'invoke', 'define', or 'eval'.
                If the fun argument is a function or bound method, then
                the default value is 'invoke'.  If the fun argument is
                a string, then the default value is 'eval' (a value of
                'invoke' or 'define' is illegal for python expressions).

        blocking -- A boolean value that indicates whether to wait for
                the results, or to return as soon as the tasks have been
                submitted.  If set to False, eachElem will return a
                SleighPending object that is used to monitor the status
                of the tasks, and to eventually retrieve the results.
                You must wait for the results to be complete before
                executing any further tasks on the sleigh, or a
                SleighOccupiedException will be raised.

                If blocking is set to False, then the loadFactor
                argument is disabled and ignored.  Note that it's
                unlikely that you'll need to turn off blocking in
                eachElem.  Non-blocking mode is more useful in
                eachWorker.

                The default value is True.

        argPermute -- List that maps the specified arguments to the
                actual arguments of the execution function.  By
                "specified arguments", I mean the items extracted from
                elementArgs, followed by fixedArgs.  (Note that unless
                you are specifying both elementArgs and fixedArgs, you
                probably don't need to use argPermute.)  The items in
                the argPermute list are used as indexes into the
                "specified arguments" list.  The length of argPermute
                determines the number of arguments passed to the
                execution function, which would normally be the length
                of the specified arguments list, but this is not
                required.  For example, setting argPermute to an empty
                list would cause the execution function to be called
                without any arguments (although elementArgs would still
                be required, and would be used to determine the number
                of tasks to execute).

                The default behaviour is to pass the execution function
                the arguments specified by elementArgs, followed by the
                arguments from fixedArgs, which is equivalent to setting
                argPermute to:

                    n = len(elementArgs) + len(fixedArgs)
                    argPermute = range(n)

                If you wished to reverse the order of the arguments, you
                could then modify argPermute:

                    argPermute.reverse()

                But, more realistically, you need to interleave the
                fixed arguments with the varying arguments.  For
                example, your execution function takes on fixed
                argument, followed by two that vary, you would set
                argPermute to:

                    argPermute=[1,2,0]

        loadFactor -- Maximum number of tasks per worker to put into the
                workspace at one time.  This can become important if you
                are executing a very large number of tasks.  Setting
                loadFactor to 3 will probably keep enough tasks
                available in the workspace to keep the workers busy,
                without flooding the workspace and bogging down the NWS
                server.

                The default behaviour is to submit all of the tasks
                to the sleigh workspace immediately.

        accumulator -- A function or callable object that will be called
                for each result as they arrive.  The first argument to
                the function is a list of result values, and the second
                argument is a list of indexes, which identifies which task.

                The arguments to the accumulator function are lists since
                in the future, we plan to allow tasks to be "chunked" to
                improve performance of small tasks.

                Note that bound methods can be very useful accumulators.

        """
        if self.state.occupied:
            raise SleighOccupiedException('sleigh is occupied')

        if self.state.stopped:
            raise SleighStoppedException('sleigh is stopped')

        if isinstance(elementArgs, list):
            # it's a list, but if it doesn't contain an iterable,
            # then wrapped it in a list
            if len(elementArgs) > 0:
                if not _isiterable(elementArgs[0]): elementArgs = [elementArgs]
            else:
                raise SleighIllegalValueException('elementArgs is an empty list')
        else:
            # it's not a list, so wrap it in a list
            elementArgs = [elementArgs]

        # make sure that elementArgs is now a list of iterables
        for xv in elementArgs:
            if not _isiterable(xv):
                raise SleighIllegalValueException('elementArgs must be an iterable or a list of iterables')

        if not isinstance(fixedArgs, list): fixedArgs = [fixedArgs]

        argPermute = kw.get('argPermute', None)
        blocking = kw.get('blocking', True)
        loadFactor = kw.get('loadFactor', 0)
        accumulator = kw.get('accumulator', Accumulator())
        if isinstance(fun, str):
            taskType = kw.get('type', 'eval')
        else:
            taskType = kw.get('type', 'invoke')

        if argPermute is not None and not isinstance(argPermute, list):
            raise SleighIllegalValueException('argPermute must be a list')

        # verify that accumulator is a callable object or an object with a result method
        if not callable(accumulator) and not hasattr(accumulator, 'result'):
            raise SleighIllegalValueException('accumulator must be callable or have a callable result attribute')

        t = _newtask(fun, taskType, False)

        nws = self.nws

        # create an "enumerator" for the task data
        taskIter = izip(count(), *elementArgs)

        # allocate a batch id for the tasks
        id = self.__newBatch()
        md = {'batchId': id}

        # loadFactor is ignored if non-blocking
        if blocking and loadFactor > 0:
            wc = self._syncWorkerCount()
            taskLimit = xrange(max(loadFactor * wc, wc))
        else:
            taskLimit = count()  # no limit

        # task args generator
        argsgen = ((taskData[0], (list(taskData[1:])+fixedArgs)) for taskData in taskIter)
        if argPermute is not None:
            argsgen = ((td[0], [td[1][j] for j in argPermute]) for td in argsgen)

        # submit tasks, possibly limited by loadFactor
        initialSubmission = 0
        for ignore, taskData in izip(taskLimit, argsgen):
            t.tag = taskData[0]
            t.args = taskData[1]
            nws.store('task', t, md)
            initialSubmission += 1

        # update the total number of submitted tasks
        if not self.new_protocol:
            self.state.totalTasks += initialSubmission
            nws.store('totalTasks', str(self.state.totalTasks))

        # return a SleighPending object if we're non-blocking
        if not blocking:
            self.state.occupied = not self.new_protocol
            return SleighPending(nws, id, initialSubmission, None, self, accumulator)

        # start retrieving results and finish submitting tasks
        # which is needed when loadFactor > 0
        numTasks = initialSubmission
        for taskData in argsgen:
            # fetch a result
            r = nws.fetch('result', md)
            if r.error:
                _accumerror(accumulator, [(r.tag, r.value)])
            else:
                _accumresult(accumulator, [(r.tag, r.value)])

            # submit next task
            t.tag = taskData[0]
            t.args = taskData[1]
            nws.store('task', t, md)
            numTasks += 1

            # update the total number of submitted tasks
            if not self.new_protocol:
                self.state.totalTasks += 1
                nws.store('totalTasks', str(self.state.totalTasks))

        # finish retrieving results
        for i in xrange(initialSubmission):
            # fetch a result
            r = nws.fetch('result', md)
            if r.error:
                _accumerror(accumulator, [(r.tag, r.value)])
            else:
                _accumresult(accumulator, [(r.tag, r.value)])

        return _accumvalue(accumulator)

    def imap(self, fun, *iterables, **kw):
        """Return an iterator whose values are returned from the function
        evaluated with a argument tuple taken from the given iterable.
        Stops when the shortest of the iterables is exhausted.

        s.imap(fun, *iterables[, **kw]) -> SleighResultIterator

        This is intended to be very similar to the imap function
        in the itertools module.  Other than being a method, rather than
        a function, this method takes additional, keyword arguments, and
        the iterator that is returned has special methods and properties.
        See the SleighResultIterator documentation for more information.

        Arguments:

        fun -- Function, method, or python expression to execute.  To
                execute a method, you must specify a bound method
                object.  If the function or defining class is defined in
                a module, the workers will attempt to import that
                module.  If that module isn't available to the worker
                (because it's a non-standard module, not in the
                PYTHONPATH), then the worker is not be able to execute
                those tasks.

                To execute a python expression, you must specify it as a
                string.  Leading whitespace is automatically stripped to
                avoid a common source of python syntax errors.


        *iterables -- One or more iterables, one for each argument needed
                by the function.

        Keyword arguments:

        type -- Indicates the type of function invocation to perform.
                This can be either 'invoke', 'define', or 'eval'.
                If the fun argument is a function or bound method, then
                the default value is 'invoke'.  If the fun argument is
                a string, then the default value is 'eval' (a value of
                'invoke' or 'define' is illegal for python expressions).

        loadFactor -- Maximum number of tasks per worker to put into the
                workspace at one time.  This can become important if you
                are executing a very large number of tasks (and essential
                if submitting infinite tasks).  The default value of 10
                will probably keep enough tasks available in the workspace
                to keep the workers busy, without flooding the workspace
                and bogging down the NWS server.

        """
        if self.state.occupied:
            raise SleighOccupiedException('sleigh is occupied')

        if self.state.stopped:
            raise SleighStoppedException('sleigh is stopped')

        # make sure that iterables is a list of iterables
        for it in iterables:
            if not _isiterable(it):
                raise SleighIllegalValueException('arguments must be iterables')

        loadFactor = kw.get('loadFactor', 10)
        if isinstance(fun, str):
            taskType = kw.get('type', 'eval')
        else:
            taskType = kw.get('type', 'invoke')

        t = _newtask(fun, taskType, False)

        nws = self.nws

        # create an "enumerator" for the task data
        taskIter = izip(count(), *iterables)

        # allocate a batch id for the tasks
        id = self.__newBatch()
        md = {'batchId': id}

        # loadFactor is ignored if non-blocking
        if loadFactor > 0:
            wc = self._syncWorkerCount()
            taskLimit = xrange(max(loadFactor * wc, wc))
        else:
            taskLimit = count()  # no limit

        # submit tasks, possibly limited by loadFactor
        initialSubmission = 0
        for ignore, taskData in izip(taskLimit, taskIter):
            t.tag = taskData[0]
            t.args = list(taskData[1:])
            nws.store('task', t, md)
            initialSubmission += 1

        # update the total number of submitted tasks
        self.state.totalTasks += initialSubmission
        nws.store('totalTasks', str(self.state.totalTasks))
        
        self.state.occupied = True
        return SleighResultIterator(t, taskIter, False, nws, initialSubmission, self.state, md)

    def starmap(self, fun, iterable, **kw):
        """Return an iterator whose values are returned from the function
        evaluated with a argument tuple taken from the given iterable.
        Stops when the shortest of the iterables is exhausted.

        s.starmap(fun, iterable[, **kw]) -> SleighResultIterator

        This is intended to be very similar to the starmap function
        in the itertools module.  Other than being a method, rather than
        a function, this method takes additional, optional arguments, and
        the iterator that is returned has special methods and properties.
        See the SleighResultIterator documentation for more information.

        Arguments:

        fun -- Function, method, or python expression to execute.  To
                execute a method, you must specify a bound method
                object.  If the function or defining class is defined in
                a module, the workers will attempt to import that
                module.  If that module isn't available to the worker
                (because it's a non-standard module, not in the
                PYTHONPATH), then the worker is not be able to execute
                those tasks.

                To execute a python expression, you must specify it as a
                string.  Leading whitespace is automatically stripped to
                avoid a common source of python syntax errors.


        iterable -- Returns argument tuples for the function.

        Keyword arguments:

        type -- Indicates the type of function invocation to perform.
                This can be either 'invoke', 'define', or 'eval'.
                If the fun argument is a function or bound method, then
                the default value is 'invoke'.  If the fun argument is
                a string, then the default value is 'eval' (a value of
                'invoke' or 'define' is illegal for python expressions).

        loadFactor -- Maximum number of tasks per worker to put into the
                workspace at one time.  This can become important if you
                are executing a very large number of tasks (and essential
                if submitting infinite tasks).  The default value of 10
                will probably keep enough tasks available in the workspace
                to keep the workers busy, without flooding the workspace
                and bogging down the NWS server.

        """
        if self.state.occupied:
            raise SleighOccupiedException('sleigh is occupied')

        if self.state.stopped:
            raise SleighStoppedException('sleigh is stopped')

        # make sure that iterable is an iterable
        if not _isiterable(iterable):
            raise SleighIllegalValueException('iterable must be an iterable')

        loadFactor = kw.get('loadFactor', 10)
        if isinstance(fun, str):
            taskType = kw.get('type', 'eval')
        else:
            taskType = kw.get('type', 'invoke')

        t = _newtask(fun, taskType, False)

        nws = self.nws

        # create an "enumerator" for the task data
        taskIter = izip(count(), iterable)

        # allocate a batch id for the tasks
        id = self.__newBatch()
        md = {'batchId': id}

        # loadFactor is ignored if non-blocking
        if loadFactor > 0:
            wc = self._syncWorkerCount()
            taskLimit = xrange(max(loadFactor * wc, wc))
        else:
            taskLimit = count()  # no limit

        # submit tasks, possibly limited by loadFactor
        initialSubmission = 0
        for ignore, taskData in izip(taskLimit, taskIter):
            t.tag = taskData[0]
            t.args = list(taskData[1])
            nws.store('task', t, md)
            initialSubmission += 1

        # update the total number of submitted tasks
        self.state.totalTasks += initialSubmission
        nws.store('totalTasks', str(self.state.totalTasks))

        self.state.occupied = True
        return SleighResultIterator(t, taskIter, True, nws, initialSubmission, self.state, md)

    def workerInfo(self):
        """Return information about the sleigh workers.

        This returns a list of dictionaries, one for each worker.
        Note that if this is called before all of the workers have
        "joined", then you may get a partial list.  This can be
        useful in conjunction with the status method.

        The keys in the dictionaries are:
            "nws":     nws version used by the worker
            "python":  Python version used by the worker
            "pid":     process id of the worker process
            "rank":    worker rank (0 to workerCount - 1)
            "host":    name of worker machine (not necessarily the same as 
                       the value in nodeList)
            "logfile": path of the log file on the worker machine
            "os":      operating system of the worker machine

        """
        # get all of the "worker info" values
        info = list(self.nws.ifindTry('worker info'))

        # sort the list by rank to avoid surprising the caller
        aux = [(info[i]['rank'], i) for i in range(len(info))]
        aux.sort()
        return [info[i] for rank, i in aux]

    def view(self, port=8766, ws='system'):
        if webbrowser:
            if ws == 'system':
                nws = self.nws
            elif ws == 'user':
                nws = self.userNws
            else:
                raise SleighIllegalValueException(
                        'illegal value specified for "ws" argument: %s' % ws)

            vstring = "http://%s:%d/doit?op=listVars&wsName=%s" % \
                    (nws.server.serverHost, port, quote_plus(nws.curWs))
            webbrowser.open_new(vstring)
        else:
            raise SleighUnsupportedMethodException('cannot import webbrowser module')

class SleighResultIterator(object):

    """Returns results from tasks submitted to the sleigh.

    Instances of this class are returned from the Sleigh imap, and
    starmap methods.
    """

    def __init__(self, task, taskIter, starIter, nws, numSubmitted, sleighState, md):
        """Create an iterator over the task results.

        This constructor is intended for internal use only.

        Arguments:

        task -- Partially initialized Task object.

        taskIter -- Iterator over the task arguments.

        starIter -- Is this a "star" iteration?

        nws -- Sleigh NetWorkSpace object.

        numSubmitted -- Number of tasks already submitted.

        sleighState -- Part of the Sleigh objects internal state.
        
        md -- Batch ID

        """
        self._task = task
        self._taskIter = taskIter
        self._starIter = starIter
        self._nws = nws
        self._numSubmitted = numSubmitted
        self._sleighState = sleighState
        self._buffer = {}
        self._nextTag = 0
        self._doneSubmitting = False
        self._md = md

    def __iter__(self):
        return self

    def next(self):
        if self._nextTag >= self._numSubmitted:
            raise StopIteration()

        try:
            # see if we already have the next result
            r = self._buffer.pop(self._nextTag)
        except KeyError:
            while True:
                # fetch a result
                r = self._nws.fetch('result', self._md)

                if not self._doneSubmitting:
                    try:
                        # submit next task
                        taskData = self._taskIter.next()
                        self._task.tag = taskData[0]
                        if self._starIter:
                            self._task.args = list(taskData[1])
                        else:
                            self._task.args = list(taskData[1:])
                        self._nws.store('task', self._task, self._md)

                        # update the total number of submitted tasks
                        self._sleighState.totalTasks += 1
                        self._nws.store('totalTasks', str(self._sleighState.totalTasks))
                        self._numSubmitted += 1
                    except StopIteration:
                        self._doneSubmitting = True

                if r.tag == self._nextTag:
                    break

                self._buffer[r.tag] = r

        self._nextTag += 1
        if self._nextTag >= self._numSubmitted:
            self._sleighState.occupied = False

        if r.error:
            v = SleighTaskException(r.value)
        else:
            v = r.value

        return v

    def returned(self):
        return self._nextTag

    returned = property(returned, doc='Number of task results returned.')

    def buffered(self):
        return len(self._buffer)

    buffered = property(buffered, doc='Number of buffered task results.')

    def submitted(self):
        return self._numSubmitted

    submitted = property(submitted, doc='Number of submitted tasks.')

    def stopped(self):
        return self._nextTag >= self._numSubmitted

    stopped = property(stopped, doc='Is the iterator stopped?')

    def shutdown(self):
        """Stop submitting tasks from the iterator.

        This method is a less drastic version of "stop".  It is expected
        that you will keep retrieving results that have already been
        submitted, but no new tasks will be submitted, regardless of
        what tasks were originally specified to imap or starmap.  The
        sleigh object will continue to be "occupied" until all results of
        the pending tasks have been retreived.

        """
        self._doneSubmitting = True

    def stop(self):
        """Stop the iterator, flushing any pending results.

        This method is useful if you're done with the iterator, and
        don't want to retrieve anymore results.  After calling stop,
        you can submit more tasks to the sleigh (that is, it will
        no longer be "occupied".

        """
        if self._nextTag < self._numSubmitted:
            self._nextTag += len(self._buffer)
            for i in xrange(self._numSubmitted - self._nextTag):
                self._nws.fetch('result', self._md)
            self._nextTag = self._numSubmitted
            self._doneSubmitting = True
            self._sleighState.occupied = False
            self._buffer.clear()
