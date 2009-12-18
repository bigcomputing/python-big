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

"""Python API for performing NetWorkSpace operations.

NetWorkSpaces (NWS) is a powerful, open-source software package that
makes it easy to use clusters from within scripting languages like
Python, R, and Matlab.  It uses a Space-based approach, similar to
JavaSpaces (TM) for example, that makes it easier to write distributed
applications.

Example:

First start up the NWS server, using the twistd command:

    % twistd -y /etc/nws.tac

Now you can perform operations against it using this module:

    % python
    >>> from nws.client import NetWorkSpace
    >>> nws = NetWorkSpace("test")
    >>> nws.store("answer", 42)
    >>> count = nws.fetch("answer")
    >>> print "The answer is", count

"""

import string
import cPickle, os, socket, stat
try:
    import fcntl
except ImportError:
    fcntl = None
try:
    import errno
except ImportError:
    errno = None

try:
    import webbrowser
except ImportError:
    webbrowser = None

from types import StringType
from warnings import warn, filterwarnings
from urllib import quote_plus

# Change the default warning filtering for this module
filterwarnings('always', module=__name__)

__all__ = ['NwsServer', 'NetWorkSpace', 'NwsValueIterator',
           'NwsException', 'NwsServerException',
           'NwsConnectException', 'NwsConnectionDroppedException',
           'NwsUnsupportedOperationException', 'NwsOperationException',
           'NwsNoWorkSpaceException', 'NwsDeclarationFailedException',
           'NwsPicklingException', 'NwsUnpicklingException',
           'NwsUnsupportedMethodException',
           'FIFO', 'LIFO', 'MULTI', 'SINGLE',
           'STRING', 'DICT',
           'OPT_SSLKEY', 'OPT_SSLCERT', 'OPT_SSL', 'OPT_DEADMAN', 'OPT_METADATATOSERVER', 'OPT_METADATAFROMSERVER',
           'WS_NAME', 'WS_MINE', 'WS_OWNER', 'WS_PERSISTENT', 'WS_NUMVARS', 'WS_VARLIST',
           'V_VARIABLE', 'V_VALUES', 'V_FETCHERS', 'V_FINDERS', 'V_MODE',
           'PROTOCOL_VERSION_OLD', 'PROTOCOL_VERSION_NEW_0']

_PythonFP =     0x01000000
_DirectString = 0x00000001
_OldProtocol =  '2222'
_NegotiationHandshakeInit      = 'X000'
_NegotiationHandshakeAdvertise = 'P000'
_NegotiationHandshakeRequest   = 'R000'
_NegotiationHandshakeAccept    = 'A000'

PROTOCOL_VERSION_OLD   = 'old'
PROTOCOL_VERSION_NEW_0 = 'new-0000'

NWS_PORT = 8765

FIFO = 'fifo'
LIFO = 'lifo'
MULTI = 'multi'
SINGLE = 'single'

STRING = 'string'
DICT = 'dict'

OPT_SSLKEY             = "SSLKey"
OPT_SSLCERT            = "SSLCertificate"
OPT_SSL                = "SSL"
OPT_DEADMAN            = "KillServerOnClose"
OPT_METADATATOSERVER   = "MetadataToServer"
OPT_METADATAFROMSERVER = "MetadataFromServer"

WS_NAME = 0
WS_MINE = 1
WS_OWNER = 2
WS_PERSISTENT = 3
WS_NUMVARS = 4
WS_VARLIST = 5

V_VARIABLE = 0
V_VALUES = 1
V_FETCHERS = 2
V_FINDERS = 3
V_MODE = 4

_modes = [FIFO, LIFO, MULTI, SINGLE]
_formats = [STRING, DICT]

def _marshaldict(d):
    if d is None:
        return "0000"
    else:
        n = len(d)
        return ("%04d" % n) + ''.join(["%04d%s%04d%s" % (len(k), k, len(v), v) for k, v in d.items()])

def _marshallist(d, extraArgs=0):
    if d is None:
        return "%04d" % extraArgs
    else:
        n = len(d) + extraArgs
        return ("%04d" % n) + ''.join(["%020d%s" % (len(k), k) for k in d])

def _getintattr(obj, attr, defval=0):
    try:
        val = getattr(obj, attr)
        if callable(val):
            val = val()
        val = int(val)
    except:
        val = defval

    return val

def _sanityFileObj(fobj, mode):
    if fobj is None: return

    try:
        # check the mode (but don't require "mode" attribute)
        if hasattr(fobj, 'mode'):
            # these can throw a ValueError
            fobj.mode.index('b')
            fobj.mode.index(mode)

        # must be open (but don't require "closed" attribute)
        if hasattr(fobj, 'closed'):
            assert not fobj.closed

        # must have either a read or write method, depending on mode
        if mode == 'w':
            assert hasattr(fobj, 'write')
        else:
            assert hasattr(fobj, 'read')
    except (AssertionError, ValueError):
        if mode == 'w':
            raise TypeError('fobj must be a binary mode file object opened for writing')
        else:
            raise TypeError('fobj must be a binary mode file object opened for reading')

def _sanityStrObj(s, sname):
    try:
        assert len(s) == len(s.__str__())
    except (AssertionError, AttributeError):
        raise TypeError('%s must be a string' % sname)

class NwsException(Exception):
    """Base class for all exceptions raised by this module."""

class NwsServerException(NwsException):
    """Error communicating with the NWS server."""

class NwsConnectException(NwsServerException):
    """Unable to connect to the NWS server."""

class NwsConnectionDroppedException(NwsServerException):
    """NWS server connection dropped."""

class NwsUnsupportedOperationException(NwsServerException):
    """NWS server does not support this operation."""

class NwsOperationException(NwsException):
    """Error performing an NWS operation."""

class NwsNoWorkSpaceException(NwsOperationException):
    """No such workspace."""

class NwsDeclarationFailedException(NwsOperationException):
    """Variable declaration failed."""

class NwsUnsupportedMethodException(NwsException):
    """Unsupported method."""

class NwsPicklingException(NwsOperationException):
    """Error pickling an object."""

    def __init__(self, exception, wsName, varName):
        Exception.__init__(self)
        self.exception = exception
        self._wsName = wsName
        self._varName = varName
        try: self._exname = exception.__class__.__name__
        except: self._exname = '[unknown]'

    def __str__(self):
        return "caught %s while pickling object to store in '%s @ %s'" % \
                (repr(self._exname), self._wsName, self._varName)

class NwsUnpicklingException(NwsOperationException):
    """Error unpickling an object."""

    def __init__(self, exception, wsName, varName, obj):
        Exception.__init__(self)
        self.exception = exception
        self._wsName = wsName
        self._varName = varName
        self.obj = obj
        try: self._exname = exception.__class__.__name__
        except: self._exname = '[unknown]'

    def __str__(self):
        return "caught %s while unpickling object retrieved from '%s @ %s'" % \
                (repr(self._exname), self._wsName, self._varName)

class SslSocket(object):
    """Simple wrapper class giving an SSL object a minimal socket interface
    allowing seamless switching between plain sockets and SSL.
    """

    def __init__(self, sock, keyfile=None, certfile=None):
        self.sock   = sock
        self.sslobj = socket.ssl(sock, keyfile, certfile)

    def recv(self, len):
        return self.sslobj.read(len)

    def sendall(self, bytes):
        return self.sslobj.write(bytes)

    def shutdown(self, n):
        self.sock.shutdown(n)

    def close(self):
        self.sock.close()

class NwsServer(object):

    """Perform operations against the NWS server.

    Operations against workspaces are performed
    by using the NetWorkSpace class.
    """

    def __init__(self, host='localhost', port=NWS_PORT, connopts=None):
        """Create a connection to the NWS server.

        Arguments:

        host -- Host name of the NWS server.

        port -- Port of the NWS server.

        """
        self.serverHost = host
        self.serverPort = port
        if connopts is not None:
            self.connopts = dict(connopts)
        else:
            self.connopts = None
        self.sslkey  = None
        self.sslcert = None
        self.lastReceivedMetadata = None
        if self.connopts is not None:
            if self.connopts.has_key(OPT_SSLKEY):
                self.sslkey  = self.connopts[OPT_SSLKEY]
                del self.connopts[OPT_SSLKEY]
            if self.connopts.has_key(OPT_SSLCERT):
                self.sslcert = self.connopts[OPT_SSLCERT]
                del self.connopts[OPT_SSLCERT]
        if (self.sslkey is None) != (self.sslcert is None):
            raise NwsConnectException("""invalid SSL configuration while connecting to the NWS server at %s:%d
  Either both or none of SSLKey and SSLCertificate must be specified in the connection options.""" % \
                    (self.serverHost, self.serverPort))
        self._connect()

    def receive_name(self):
        len = int(self._recvN(4))
        return self._recvN(len)

    def receive_dict(self):
        num_opts = int(self._recvN(4))
        advertise_opts = {}
        for i in range(0, num_opts):
            name = self.receive_name()
            value = self.receive_name()
            advertise_opts[name] = value
        return advertise_opts

    def _connect(self):
        if self.sslkey is not None:
            if self.connopts is None:
                self.connopts = dict()
            self.connopts[OPT_SSL] = ""

        self.nwsSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            self.nwsSocket.connect((self.serverHost, self.serverPort))
        except socket.error:
            raise NwsConnectException('unable to connect to the NWS server at %s:%d' % \
                    (self.serverHost, self.serverPort))

        # denagle the socket to improve performance
        self.nwsSocket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        # enable the keepalive option
        self.nwsSocket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

        # try to set the 'close on exec' flag for the socket
        if fcntl and hasattr(fcntl, 'FD_CLOEXEC'):
            fd = self.nwsSocket.fileno()
            f = fcntl.fcntl(fd, fcntl.F_GETFD) | fcntl.FD_CLOEXEC
            fcntl.fcntl(fd, fcntl.F_SETFD, f)

        # tell the server that we're a new client
        self._sendAll(_NegotiationHandshakeInit)
        self._handshake = self._recvN(4)
        if self._handshake == _NegotiationHandshakeAdvertise:
            self._protocolVersion = PROTOCOL_VERSION_NEW_0
            self._cookieProtocol = True
            advertise_opts = self.receive_dict()
            if self.connopts is None and len(advertise_opts) != 0:
                self.connopts = {}
            for k, v in self.connopts.items():
                if not advertise_opts.has_key(k):
                    raise NwsConnectException('requested connection option %s="%s" but this server does not support this option' % (k, v))
                elif advertise_opts[k] != '' and advertise_opts[k] != v:
                    raise NwsConnectException('requested connection option %s="%s" but this server requires %s="%s"' % (k, v, k, advertise_opts[k]))
            self._sendAll(_NegotiationHandshakeRequest + _marshaldict(self.connopts))
            if self._recvN(4) != _NegotiationHandshakeAccept:
                raise NwsConnectException('failed options negotiation with NWS server at %s:%d' % \
                           (self.serverHost, self.serverPort))
            if self.connopts is not None and self.connopts.has_key(OPT_SSL):
                self.nwsSocket = SslSocket(self.nwsSocket, self.sslkey, self.sslcert)
        else:
            self._protocolVersion = PROTOCOL_VERSION_OLD
            self._cookieProtocol = self._handshake != _OldProtocol

    def __getstate__(self):
        return {'serverHost': self.serverHost, 'serverPort': self.serverPort}

    def __setstate__(self, d):
        self.__dict__.update(d)
        self._connect()

    def __str__(self):
        return "NwsServer %s:%d" % (self.serverHost, self.serverPort)

    def get_protocol_version(self):
        return self._protocolVersion

    def _recvN(self, n, fobj=None):
        if n > 0:
            if fobj is not None:
                # determine the buffer size for writing to the file or
                # file-like object
                defval = 16 * 1024
                blen = _getintattr(fobj, "blocksize", defval)
                if blen <= 0:
                    blen = defval

                # read data from the socket, then write it to the file
                while n > 0:
                    d = self.nwsSocket.recv(min(n, blen))
                    if not d:
                        raise NwsConnectionDroppedException('NWS server connection dropped')
                    fobj.write(d)
                    n -= len(d)

                return ''
            else:
                b = self.nwsSocket.recv(n)
                m = len(b)
                n -= m
                if n <= 0: return b
                if m == 0:
                    raise NwsConnectionDroppedException('NWS server connection dropped')

                r = [b]
                while 1:
                    b = self.nwsSocket.recv(n)
                    r.append(b)
                    m = len(b)
                    n -= m
                    if n <= 0: break
                    if m == 0:
                        raise NwsConnectionDroppedException('NWS server connection dropped')

                return ''.join(r)
        else:
            return ''

    def _sendAll(self, b):
        return self.nwsSocket.sendall(b)

    def sendOp(self, *args, **metadata):
        if self.connopts.has_key(OPT_METADATATOSERVER):
            self._sendAll(_marshaldict(metadata) + _marshallist(args))
        else:
            self._sendAll(_marshallist(args))

    def sendOpStreaming(self, *args, **metadata):
        if self.connopts.has_key(OPT_METADATATOSERVER):
            self._sendAll(_marshaldict(metadata) + _marshallist(args, 1))
        else:
            self._sendAll(_marshallist(args, 1))

    def receiveMetadata(self):
        if self.connopts.has_key(OPT_METADATAFROMSERVER):
            return self.receive_dict()
        else:
            return None

    def _receiveMetadata(self):
        self.lastReceivedMetadata = self.receiveMetadata()

    def close(self):
        """Close the connection to the NWS server.

        s.close()

        This will indirectly cause the NWS server to purge all
        non-persistent workspaces owned by this client.  Purging may
        not happen immediately, though, but will depend on the load on
        the server.

        """
        try:
            self.nwsSocket.shutdown(2)
            self.nwsSocket.close()
        except: pass

    def deleteWs(self, wsName, metadata={}):
        """Delete the specfied workspace on the NWS server.

        s.deleteWs(wsName)

        Arguments:

        wsName -- Name of the workspace to delete.

        """
        _sanityStrObj(wsName, 'workspace name')
        self.sendOp('delete ws', wsName, **metadata)
        self._receiveMetadata()
        status = int(self._recvN(4))
        if status: raise NwsOperationException('deleteWs failed')

    def listWss(self, format=STRING, metadata={}):
        """Return a listing of all of the workspaces on the NWS server.

        s.listWss([format]) -> string or dictionary

        The listing is a string, consisting of lines, each ending with a
        newline.  Each line consists of tab separated fields.  The first
        field is the workspace name, prefixed with either a '>' or a
        space, indicating whether the client owns that workspace or
        not.

        """
        _sanityStrObj(format, 'format')

        if format not in _formats:
            raise ValueError('illegal format: ' + format)

        self.sendOp('list wss', **metadata)
        self._receiveMetadata()
        status = int(self._recvN(4))
        desc = self._recvN(20) # unused at the moment.
        if self._cookieProtocol:
            cookie = self._recvN(40) # unused at the moment.
        n = int(self._recvN(20))
        listing = self._recvN(n)
        if status: raise NwsOperationException('listWss failed')

        if format == DICT:
            wss = {}
            if listing:
                for ws in listing[:-1].split('\n'):
                    wsRec = ws.split('\t')
                    mine = wsRec[0][0] == '>'
                    wsRec[WS_NAME] = wsRec[WS_NAME][1:]
                    wsRec.insert(WS_MINE, mine)
                    wsRec[WS_PERSISTENT] = wsRec[WS_PERSISTENT].lower().startswith('t')
                    wsRec[WS_NUMVARS] = int(wsRec[WS_NUMVARS])
                    wsRec[WS_VARLIST] = wsRec[WS_VARLIST] and wsRec[WS_VARLIST].split(',') or []
                    wss[wsRec[WS_NAME]] = tuple(wsRec)
        else:
            wss = listing

        return wss

    def mktempWs(self, wsName='__pyws__%d', metadata={}):
        """Make a temporary workspace on the NWS server.

        s.mktempWs([wsName]) -> string

        The workspace is named using the template and a number
        generated by the server that makes the workspace name unique.

        Note that the workspace will be created, but it will not be
        owned until some client that is willing to take ownership of it
        opens it.

        The return value is the actual name of workspace that was
        created.

        Arguments:

        wsName -- Template for the workspace name.  This must be a legal
                'format' string, containing only an integer format
                specifier.  The default is '__pyws__%d'.

        Examples:

            Let's create an NwsServer, call mktempWs to make a
            workspace, and then use openWs to create a NetWorkSpace
            object for that workspace:

                >>> from nws.client import NwsServer
                >>> server = NwsServer()
                >>> name = server.mktempWs('example_%d')
                >>> workspace = server.openWs(name)

        """
        _sanityStrObj(wsName, 'workspace name')

        self.sendOp('mktemp ws', wsName, **metadata)
        self._receiveMetadata()
        status = int(self._recvN(4))
        desc = self._recvN(20) # unused at the moment.
        if self._cookieProtocol:
            cookie = self._recvN(40) # unused at the moment.
        n = int(self._recvN(20))
        name = self._recvN(n)
        if status: raise NwsOperationException('mktempWs failed')
        return name

    def openWs(self, wsName, space=None, metadata={}, **opt):
        """Open a workspace.

        s.openWs(wsName[, space]) -> space

        If called without a space argument, this method will construct a
        NetWorkSpace object that will be associated with this NwsServer
        object, and then perform an open operation with it against the NWS
        server.  The open operation tells the NWS server that this
        client wants to use that workspace, and is willing to take
        ownership of it, if it doesn't already exist.

        The space argument is only intended to be used from the
        NetWorkSpace constructor.

        The return value is the constructed NetWorkSpace object.

        Arguments:

        wsName -- Name of the workspace to open.  If the space argument
                is not None, this value must match the space's name.

        space -- NetWorkSpace object to use for the open operation.
                If the value is None, then openWs will construct a
                NetWorkSpace object, specifying this NwsServer object as
                the space's server.  Note that this argument is only
                intended to be used from the NetWorkSpace constructor.
                The default value is None.

        Keyword Arguments:

        persistent -- Boolean value indicating whether the workspace
                should be persistent or not.  See the description of the
                persistent argument in the __init__ method of the
                NetWorkSpace class for more information.

        create -- Boolean value indicating whether the workspace should
                be created if it doesn't already exist.  The default
                value is true.

        Examples:

            Let's create an NwsServer, and then use openWs to create an
            NetWorkSpace object for a workspace called 'foo':

                >>> from nws.client import NwsServer
                >>> server = NwsServer()
                >>> workspace = server.openWs('foo')

            Note that this is (nearly) equivalent to:

                >>> from nws.client import NetWorkSpace
                >>> workspace = NetWorkSpace('foo')

        """
        # sanity check the keyword arguments
        unrecog = [a for a in opt.keys() if a not in ['create', 'persistent']]
        if unrecog:
            raise TypeError(
                    "__init__() got an unexpected keyword argument '%s'" % \
                    unrecog[0])

        _sanityStrObj(wsName, 'workspace name')

        # if invoked directly by user, we need to create a space
        # instance. if invoked via NetWorkSpace constructor, use the
        # space passed in.
        if not space:
            space = NetWorkSpace(wsName, server=self)
        elif space.currentWs() != wsName:
            raise ValueError('name of the specified workspace is incorrect')
        else:
            owner = '%d' % os.getpid()

            p = 'no'
            if opt.get('persistent', False): p = 'yes'

            if opt.get('create', True):
                self.sendOp('open ws', wsName, owner, p, **metadata)
            else:
                self.sendOp('open ws', wsName, owner, p, 'no', **metadata)

            self._receiveMetadata()
            status = int(self._recvN(4))
            if status: raise NwsNoWorkSpaceException("workspace %s doesn't exist" % repr(wsName))
        return space

    def useWs(self, wsName, space=None, metadata={}, **opt):
        """Use a NetWorkSpace object.

        s.useWs(wsName[, space]) -> space

        If called without a space argument, this method will construct a
        NetWorkSpace object that will be associated with this NwsServer
        object, and then perform a use operation with it against the NWS
        server.  The use operation tells the NWS server that this client
        wants to use that workspace, but is not willing to take
        ownership of it.

        The space argument is only intended to be used from the
        NetWorkSpace constructor.

        The return value is the constructed NetWorkSpace object.

        Arguments:

        wsName -- Name of the workspace to use.  If the space argument
                is not None, this value must match the space's name.

        space -- NetWorkSpace object to use for the use operation.
                If the value is None, then useWs will construct a
                NetWorkSpace object, specifying this NwsServer object as
                the space's server.  Note that this argument is only
                intended to be used from the NetWorkSpace constructor.
                The default value is None.

        Keyword Arguments:

        create -- Boolean value indicating whether the workspace should
                be created if it doesn't already exist.  The default
                value is true.

        Examples:

            Let's create an NwsServer, and then use useWs to create an
            NetWorkSpace object for a workspace called 'foo':

                >>> from nws.client import NwsServer
                >>> server = NwsServer()
                >>> workspace = server.useWs('foo')

            Note that this is (nearly) equivalent to:

                >>> from nws.client import NetWorkSpace
                >>> workspace = NetWorkSpace('foo', useUse=True)

        """
        # sanity check the keyword arguments
        unrecog = [a for a in opt.keys() if a not in ['create', 'persistent']]
        if unrecog:
            raise TypeError(
                    "__init__() got an unexpected keyword argument '%s'" % \
                    unrecog[0])

        _sanityStrObj(wsName, 'workspace name')

        # if invoked directly by user, we need to create a space
        # instance. if invoked via networkspace constructor, use the
        # space passed in.
        if not space:
            space = NetWorkSpace(wsName, server=self)
        elif space.currentWs() != wsName:
            raise ValueError('name of the specified workspace is incorrect')
        else:
            owner = ''

            p = 'no'

            if opt.get('create', True):
                self.sendOp('use ws', wsName, owner, p, **metadata)
            else:
                self.sendOp('use ws', wsName, owner, p, 'no', **metadata)

            self._receiveMetadata()
            status = int(self._recvN(4))
            if status: raise NwsNoWorkSpaceException("workspace %s doesn't exist" % repr(wsName))
        return space

    def view(self, port=8766):
        if webbrowser:
            vstring = "http://%s:%d/doit?op=listWss" % \
                    (self.serverHost, port)
            webbrowser.open_new(vstring)
        else:
            raise NwsUnsupportedMethodException('cannot import webbrowser module')

class NetWorkSpace(object):

    """Perform operations against workspaces on NWS servers.

    The NetWorkSpace object is the basic object used to perform
    operatations on workspaces.  Variables can be declared, created,
    deleted, and the values of those variables can be manipulated.  You
    can think of a workspace as a network accessible python dictionary,
    where the variable names are keys in the dictionary, and the
    associated values are lists of pickled python objects.  The store
    method puts a value into the list associated with the specified
    variable.  The find method returns a single value from a list.
    Which value it returns depends on the "mode" of the variable (see
    the declare method for more information on the variable mode).  If
    the list is empty, the find method will not return until a value is
    stored in that list.  The findTry method works like the find method,
    but doesn't wait, returning a default value instead (somewhat like
    the dictionary's get method).  The fetch method works like the find
    method, but it will also remove the value from the list.  If
    multiple clients are all blocked on a fetch operation, and a value
    is stored into that variable, the server guarantees that only one
    client will be able to fetch that value.  The fetchTry method, not
    surprisingly, works like the fetch method, but doesn't wait,
    returning a default value instead.
    """

    def __init__(self, wsName='__default', serverHost='localhost', serverPort=NWS_PORT, useUse=False, server=None, connopts=None, wsmetadata={}, **opt):
        """Construct a NetWorkSpace object for the specified NwsServer.

        Arguments:

        wsName -- Name of the workspace.  There can only be one
                workspace on the server with a given name, so two
                clients can easily communicate with each other by both
                creating a NetWorkSpace object with the same name on the
                same server.  The first client that creates a workspace
                that is willing to take ownership of it, will become the
                owner (see the description of the useUse argument below
                for more information on workspace ownership).

        serverHost -- Host name of the NWS server.  This argument is
                ignored if the server argument is not None.  The default
                value is 'localhost'.

        serverPort -- Port of the NWS server.  This argument is ignored
                if the server argument is not None.  The default value
                is 8765.

        useUse -- Boolean value indicating whether you only want to use
                this workspace, or if you want to open it (which means
                you're willing to take ownership of it, if it's not
                already owned).

                The default value is False, meaning you are willing to
                take ownership of this workspace.

        server -- This argument is only intended for internal use.
                Specifying a value other than None will result in a
                NetWorkSpace object that isn't properly initialized.

        connopts -- connection options for options negotiation with the
                server.

        Keyword Arguments:

        persistent -- Boolean value indicating whether the workspace
                should be persistent or not.  If a workspace is
                persistent, it won't be purged when the owner
                disconnects from the NWS server.  Note that only the
                client who actually takes ownership of the workspace
                can make the workspace persistent.  The persistent
                argument is effectively ignored if useUse is True, since
                that client never becomes the owner of the workspace.
                If useUse is False, it is the client who actually
                becomes the owner that determines whether it is
                persistent or not.  That is, after the workspace is
                owned, the persistent argument is ignored.  The default
                value is false.

        create -- Boolean value indicating whether the workspace should
                be created if it doesn't already exist.  The default
                value is true.

        """
        # sanity check the keyword arguments
        unrecog = [a for a in opt.keys() if a not in ['create', 'persistent']]
        if unrecog:
            raise TypeError(
                    "__init__() got an unexpected keyword argument '%s'" % \
                    unrecog[0])

        _sanityStrObj(wsName, 'workspace name')

        self.curWs = wsName
        self._useUse = useUse
        self._opt = opt

        self.lastReceivedMetadata = None
        if len(wsmetadata) != 0:
            if connopts is None:
                connopts = {}
            connopts[OPT_METADATATOSERVER] = '1'

        # if invoked (indirectly) via a NwsServer openWs or useWs method,
        # the server will be passed in and used. if invoked directly,
        # need to create a new NwsServer instance.
        if not server:
            self.server = NwsServer(serverHost, serverPort, connopts=connopts)
        else:
            self.server = server

        # now give the server a chance to do its thing.
        try:
            if self._useUse:
                self.server.useWs(self.curWs, self, metadata=wsmetadata, **self._opt)
            else:
                self.server.openWs(self.curWs, self, metadata=wsmetadata, **self._opt)
        except Exception, e:
            # close the server and re-raise the exception
            try: self.server.close()
            except: pass
            raise e

        self.send = self.server._sendAll
        self.recv = self.server._recvN
        self._handshake = self.server._handshake
        self._cookieProtocol = self.server._cookieProtocol

    def __getstate__(self):
        return {'curWs': self.curWs, '_useUse': self._useUse, '_opt': self._opt,
                'server': self.server}

    def __setstate__(self, d):
        self.__dict__.update(d)

        try:
            if self._useUse:
                self.server.useWs(self.curWs, self, **self._opt)
            else:
                self.server.openWs(self.curWs, self, **self._opt)
        except Exception, e:
            # close the server and re-raise the exception
            try: self.server.close()
            except: pass
            raise e

        self.send = self.server._sendAll
        self.recv = self.server._recvN
        self._handshake = self.server._handshake
        self._cookieProtocol = self.server._cookieProtocol

    def __str__(self):
        return "NetWorkspace '%s' [%s]" % (self.curWs, str(self.server))

    def receiveMetadata(self):
        self.lastReceivedMetadata = self.server.receiveMetadata()

    def currentWs(self):
        """Return the name of the current workspace.

        ws.currentWs() -> string

        """
        return self.curWs

    def declare(self, varName, mode, metadata={}):
        """Declare a variable in a workspace with the specified mode.

        ws.declare(varName, mode)

        This method is used to specify a mode other than the default
        mode of 'fifo'.  Legal values for the mode are:

            'fifo', 'lifo', 'multi', and 'single'

        In the first three modes, multiple value can be stored in
        a variable.  If the mode is 'fifo', then values are retrieved
        in a "first-in, first-out" fashion.  That is, the first value
        stored, will be the first value fetched.  If the mode is 'lifo',
        then values are retrieved in a "last-in, first-out" fashion,
        as in a stack.  If the mode is 'multi', then the order of
        retrieval is pseudorandom.

        The 'single' mode means that only a single value can be
        stored in the variable.  Each new store operation will overwrite
        the current value of the variable.

        If a variable is created using a store operation, then the
        mode defaults to 'fifo'.  The mode cannot be changed with
        subsequent calls to declare, regardless of whether the variable
        was originally created using store or declare.

        Arguments:

        varName -- Name of the workspace variable to declare.

        mode -- Mode of the variable.

        """
        _sanityStrObj(varName, 'variable name')
        _sanityStrObj(mode, 'variable mode')

        # note that workspace variable extensions start with '__'
        if mode not in _modes and not mode.startswith('__'):
            raise ValueError('illegal mode: ' + str(mode))

        self.server.sendOp('declare var', self.curWs, varName, mode, **metadata)
        self.receiveMetadata()
        status = int(self.recv(4))
        if status: raise NwsDeclarationFailedException('variable declaration failed')

    def deleteVar(self, varName, metadata={}):
        """Delete a variable from a workspace.

        ws.deleteVar(varName)

        All values of the variable are destroyed, and all currently
        blocking fetch and find operations will be aborted.

        Arguments:

        varName -- Name of the workspace variable to delete.

        """
        _sanityStrObj(varName, 'variable name')

        self.server.sendOp('delete var', self.curWs, varName, **metadata)

        self.receiveMetadata()
        status = int(self.recv(4))
        if status: raise NwsOperationException('deleteVar failed')

    def __retrieve(self, varName, op, missing=None, fobj=None, metadata={}):
        _sanityStrObj(varName, 'variable name')
        _sanityFileObj(fobj, 'w')

        self.server.sendOp(op, self.curWs, varName, **metadata)
        self.receiveMetadata()
        status = int(self.recv(4)) # barely used at the moment.

        # even if failure status, read the rest of the bytes.
        desc = int(self.recv(20))
        if self.server._cookieProtocol:
            cookie = self.recv(40) # unused at the moment.
        n = int(self.recv(20))
        pVal = self.recv(n, fobj)

        if status: raise NwsOperationException('retrieval failed')

        if fobj is None:
            if desc & _DirectString:
                return pVal
            elif pVal:
                try:
                    val = cPickle.loads(pVal)
                except Exception, e:
                    raise NwsUnpicklingException(e, self.curWs, varName, pVal)
                return val
            else:
                return missing
        else:
            return n

    def fetch(self, varName, metadata={}):
        """Return and remove a value of a variable from a workspace.

        ws.fetch(varName) -> object

        If the variable has no values, the operation will not return
        until it does.  In other words, this is a "blocking" operation.
        fetchTry is the "non-blocking" version of this method.

        Note that if many clients are all trying to fetch from the same
        variable, only one client can fetch a given value.  Other
        clients may have previously seen that value using the find or
        findTry method, but only one client can ever fetch or fetchTry a
        given value.

        Arguments:

        varName -- Name of the workspace variable to fetch.

        """
        return self.__retrieve(varName, 'fetch', None, metadata=metadata)

    def fetchTry(self, varName, missing=None, metadata={}):
        """Try to return and remove a value of a variable from a workspace.

        ws.fetchTry(varName[, missing]) -> object

        If the variable has no values, the operation will return
        the value specified by "missing", which defaults to None.

        Note that if many clients are all trying to fetchTry from the
        same variable, only one client can fetchTry a given value.
        Other clients may have previously seen that value using the find
        or findTry method, but only one client can ever fetch or
        fetchTry a given value.

        Arguments:

        varName -- Name of the workspace variable to fetch.

        missing -- Value to return if the variable has no values.

        """
        try:
            return self.__retrieve(varName, 'fetchTry', missing, metadata=metadata)
        except NwsUnpicklingException:
            raise
        except NwsOperationException:
            return missing

    def find(self, varName, metadata={}):
        """Return a value of a variable from a workspace.

        ws.find(varName) -> object

        If the variable has no values, the operation will not return
        until it does.  In other words, this is a "blocking" operation.
        findTry is the "non-blocking" version of this method.

        Note that (unlike fetch) find does not remove the value.  The
        value remains in the variable.

        Arguments:

        varName -- Name of the workspace variable to find.

        """
        return self.__retrieve(varName, 'find', None, metadata=metadata)

    def findTry(self, varName, missing=None, metadata={}):
        """Try to return a value of a variable in the workspace.

        ws.findTry(varName[, missing]) -> object

        If the variable has no values, the operation will return
        the value specified by "missing", which defaults to the value
        "None".

        Note that (unlike fetchTry) findTry does not remove the value.
        The value remains in the variable.

        Arguments:

        varName -- Name of the workspace variable to use.

        missing -- Value to return if the variable has no values.  The
                default is None.

        """
        try:
            return self.__retrieve(varName, 'findTry', missing, metadata=metadata)
        except NwsUnpicklingException:
            raise
        except NwsOperationException:
            return missing

    def listVars(self, wsName=None, format=STRING, metadata={}):
        """Return a listing of the variables in the workspace.

        ws.listVars([wsName[, format]]) -> string or dictionary

        Arguments:

        wsName -- Name of the workspace to list.  The default is
                None, which means to use the current workspace.

        format -- Output format to return.  Legal values include
                'string' and 'dict'.  The 'string' format returns
                a string which is suitable for printing.
                The 'dict' format returns a dictionary of tuples, where
                the first field is the variable name, the second is
                the number of values, the third is the number of
                fetchers, the fourth is the number of finders, and
                the fifth is the variables mode.  The default value
                is 'string'.

        """
        if wsName is not None:
            _sanityStrObj(wsName, 'workspace name')

        _sanityStrObj(format, 'format')

        if format not in _formats:
            raise ValueError('illegal format: ' + format)

        if not wsName: wsName = self.curWs
        self.server.sendOp('list vars', wsName, **metadata)
        self.receiveMetadata()
        status = int(self.recv(4))
        desc = self.recv(20) # unused at the moment.
        if self.server._cookieProtocol:
            cookie = self.recv(40) # unused at the moment.
        n = int(self.recv(20))
        listing = self.recv(n)
        if status != 0: raise NwsOperationException('listVars failed')

        if format == DICT:
            vars = {}
            if listing:
                for var in listing.split('\n'):
                    varRec = var.split('\t')
                    varRec[V_VALUES] = int(varRec[V_VALUES])
                    varRec[V_FETCHERS] = int(varRec[V_FETCHERS])
                    varRec[V_FINDERS] = int(varRec[V_FINDERS])
                    vars[varRec[V_VARIABLE]] = tuple(varRec)
        else:
            vars = listing

        return vars

    def store(self, varName, val, metadata={}):
        """Store a new value into a variable in the workspace.

        ws.store(varName, val)

        Arguments:

        varName -- Name of the workspace variable.

        val -- Value to store in the variable.

        """
        _sanityStrObj(varName, 'variable name')

        desc = _PythonFP
        if isinstance(val, str):
            desc |= _DirectString
        descTxt = '%020u' % desc

        if desc & _DirectString:
            pVal = val
        else:
            try:
                pVal = cPickle.dumps(val, 1)
            except Exception, e:
                raise NwsPicklingException(e, self.curWs, varName)

        self.server.sendOpStreaming('store', self.curWs, varName, descTxt, **metadata)
        self.send('%020d' % len(pVal))
        self.send(pVal)
        self.receiveMetadata()
        status = int(self.recv(4))
        if status: raise NwsOperationException('store failed')

    def fetchFile(self, varName, fobj, metadata={}):
        """Return and remove a value of a variable from a workspace.

        ws.fetchFile(varName, fobj) -> number of bytes written to file

        Arguments:

        varName -- Name of the workspace variable to fetch.

        fobj -- File to write data to.

        """
        return self.__retrieve(varName, 'fetch', fobj=fobj, metadata=metadata)

    def fetchTryFile(self, varName, fobj, metadata={}):
        """Try to return and remove a value of a variable from a workspace.

        ws.fetchTryFile(varName, fobj) -> number of bytes written to file

        Arguments:

        varName -- Name of the workspace variable to fetch.

        fobj -- File to write data to.

        """
        try:
            return self.__retrieve(varName, 'fetchTry', fobj=fobj, metadata=metadata)
        except NwsUnpicklingException:
            raise
        except NwsOperationException:
            return False

    def findFile(self, varName, fobj, metadata={}):
        """Return a value of a variable from a workspace.

        ws.findFile(varName, fobj) -> number of bytes written to file

        Arguments:

        varName -- Name of the workspace variable to find.

        fobj -- File to write data to.

        """
        return self.__retrieve(varName, 'find', fobj=fobj, metadata=metadata)

    def findTryFile(self, varName, fobj, metadata={}):
        """Try to return a value of a variable in the workspace.

        ws.findTryFile(varName, fobj) -> number of bytes written to file

        Arguments:

        varName -- Name of the workspace variable to use.

        fobj -- File to write data to.

        """
        try:
            return self.__retrieve(varName, 'findTry', fobj=fobj, metadata=metadata)
        except NwsUnpicklingException:
            raise
        except NwsOperationException:
            return False

    def storeFile(self, varName, fobj, n=0, metadata={}):
        """Store a new value into a variable in the workspace from a file.

        ws.storeFile(varName, fobj[, n]) -> number of bytes read from file

        Note that if there is no more data to read from the file,
        storeFile returns 0, and does not store a value into the
        workspace variable.

        Arguments:

        varName -- Name of the workspace variable.

        fobj -- File to read data from.

        n -- Maximum number of bytes to read from the file.  A value of
                zero means to read and store all of the data in the
                file.  The default value is zero.

        """
        _sanityStrObj(varName, 'variable name')
        _sanityFileObj(fobj, 'r')

        desc = _PythonFP | _DirectString
        descTxt = '%020u' % desc

        if hasattr(fobj, 'len'):
            # it's a file-like object.  get the size from the
            # "len" attribute, which should be either an integer
            # (as in StringIO.StringIO) or a method
            fsize = _getintattr(fobj, 'len')
        else:
            # determine the length of the file using stat
            # if length not specified
            fsize = os.fstat(fobj.fileno())[stat.ST_SIZE]

        fpos = _getintattr(fobj, 'tell')
        fbytes = fsize - fpos

        if n <= 0:
            n = fbytes
        else:
            n = min(n, fbytes)

        if n <= 0: return 0
        nread = n  # save this for the return value

        self.server.sendOpStreaming('store', self.curWs, varName, descTxt, **metadata)
        self.send('%020d' % n)

        # allow file-like objects to tell us how much data to ask
        # for at a time.  however, we also allow them return either
        # more or less data.  this mechanism is more important when
        # receiving data.
        defval = 16 * 1024
        blen = _getintattr(fobj, "blocksize", defval)
        if blen <= 0:
            blen = defval

        while n > 0:
            d = fobj.read(min(blen, n))
            dlen = len(d)
            if dlen <= 0:
                break
            elif dlen > n:
                # we allow this for file-like objects
                dlen = n
                d = d[:n]
            self.send(d)
            n -= dlen

        if n > 0:
            # this can happen if a file-like object gives us
            # less data than the reported size
            warn('premature end-of-file reached: padding value with zeroes',
                    stacklevel=2)
            blen = 1024
            buffer = blen * "\0"
            while n > 0:
                if blen <= n:
                    self.send(buffer)
                    n -= dlen
                else:
                    self.send(n * "\0")
                    break

        self.receiveMetadata()
        status = int(self.recv(4))
        if status: raise NwsOperationException('store file failed')

        return nread

    def _iretrieve(self, varName, op, varId, valIndex, fobj=None):
        _sanityFileObj(fobj, 'w')

        self.server.sendOp(op, self.curWs, varName, "%-20.20s" % varId, "%020d" % valIndex)
        self.receiveMetadata()
        status = int(self.recv(4)) # barely used at the moment.

        # even if failure status, read the rest of the bytes.
        desc = int(self.recv(20))
        if self.server._cookieProtocol:
            varId = self.recv(20)
            valIndex = int(self.recv(20))
        n = int(self.recv(20))
        pVal = self.recv(n, fobj)

        if fobj is None:
            if desc & _DirectString:
                return (status, pVal, varId, valIndex, n)
            elif pVal:
                try:
                    val = cPickle.loads(pVal)
                except Exception, e:
                    raise NwsUnpicklingException(e, self.curWs, varName, pVal)
                return (status, val, varId, valIndex, n)
            else:
                raise StopIteration
        else:
            if desc & _DirectString or pVal:
                return (status, pVal, varId, valIndex, n)
            else:
                raise StopIteration

    def ifetch(self, varName):
        """Return a fetch iterator for a workspace variable.

        ws.ifetch(varName) -> iterator

        Unlike ifind, this method doesn't really provide any extra
        functionality over the fetch method.  It is provided for
        completeness, and for those who just like iterators.

        Note that ifetch can be used on FIFO and SINGLE mode variables,
        but not LIFO and MULTI mode variables.

        Arguments:

        varName -- Name of the workspace variable to iterator over.

        """
        if not self.server._cookieProtocol:
            raise NwsUnsupportedOperationException('NWS server does not support ifetch: ' + self._handshake)

        _sanityStrObj(varName, 'variable name')

        return NwsValueIterator(self, varName, 'ifetch')

    def ifetchTry(self, varName):
        """Return a fetchTry iterator for a workspace variable.

        ws.ifetchTry(varName) -> iterator

        Unlike ifindTry, this method doesn't really provide any extra
        functionality over the fetchTry method.  It is provided for
        completeness, and for those who just like iterators.

        Note that ifetchTry can be used on FIFO and SINGLE mode
        variables, but not LIFO and MULTI mode variables.

        Arguments:

        varName -- Name of the workspace variable to iterator over.

        """
        if not self.server._cookieProtocol:
            raise NwsUnsupportedOperationException('NWS server does not support ifetchTry: ' + self._handshake)

        _sanityStrObj(varName, 'variable name')

        return NwsValueIterator(self, varName, 'ifetchTry')

    def ifind(self, varName):
        """Return a find iterator for a workspace variable.

        ws.ifind(varName) -> iterator

        This is very useful if you want to see every value in a variable
        without destroying them.  Unlike the find method, ifind won't
        return the same value repeatedly.  When there are no more values
        to return, the iterator will block, waiting for a new value to
        arrive.

        Note that ifind can be used on FIFO and SINGLE mode variables,
        but not LIFO and MULTI mode variables.

        Arguments:

        varName -- Name of the workspace variable to iterate over.

        """
        if not self.server._cookieProtocol:
            raise NwsUnsupportedOperationException('NWS server does not support ifind: ' + self._handshake)

        _sanityStrObj(varName, 'variable name')

        return NwsValueIterator(self, varName, 'ifind')

    def ifindTry(self, varName):
        """Return a findTry iterator for a workspace variable.

        ws.ifindTry(varName) -> iterator

        This is very useful if you want to see every value in a variable
        without destroying them.  Unlike the findTry method, ifindTry
        won't return the same value repeatedly.  When there are no more
        values to return, the iterator finishes.

        Note that ifindTry can be used on FIFO and SINGLE mode
        variables, but not LIFO and MULTI mode variables.

        Arguments:

        varName -- Name of the workspace variable to iterate over.

        """
        if not self.server._cookieProtocol:
            raise NwsUnsupportedOperationException('NWS server does not support ifindTry: ' + self._handshake)

        _sanityStrObj(varName, 'variable name')

        return NwsValueIterator(self, varName, 'ifindTry')

    def makefile(self, varName, mode='r'):
        if mode in ['r', 'w', 'a']:
            return NwsTextFile(self, varName, mode)
        elif mode in ['rb', 'wb', 'ab']:
            raise ValueError('mode %r is not supported yet' % mode)
        else:
            raise ValueError('illegal mode specified: %r' % mode)

    def view(self, port=8766):
        if webbrowser:
            vstring = "http://%s:%d/doit?op=listVars&wsName=%s" % \
                    (self.server.serverHost, port, quote_plus(self.curWs))
            webbrowser.open_new(vstring)
        else:
            raise NwsUnsupportedMethodException('cannot import webbrowser module')

class NwsValueIterator(object):

    """Implements the iterated operations against a workspace variable.

    Instances of this class are returned from the NetWorkSpace ifetch,
    ifetchTry, ifind, and ifindTry methods.
    """

    def __init__(self, ws, varName, op):
        """Create an iterator over a workspace varible.

        This constructor is intended for internal use only.

        Arguments:

        ws -- NetWorkSpace object containing the specified variable.

        varName -- Name of the workspace variable to iterate over.

        op -- Operation to perform.

        """
        self._ws = ws
        self._varName = varName
        self._op = op
        self._varId = ''
        self._valIndex = 0
        self._stopped = False

    def restart(self):
        """Allow the iterator to continue where it left off after stopping.

        The Python iterator protocol requires that iterators continue to
        raise StopIteration once they've raised it.  The
        NwsValueIterator will do that unless and until you call this
        method.  That can be useful for the "Try" iterators, where you
        might want to find all of the values in a variable, and at a
        later point see if there are any new values without having to
        see the previous ones again.

        """
        self._stopped = False

    def reset(self):
        """Reset the iterator to the beginning of the workspace variable.

        This conveniently restores the state of the iterator to when it
        was first created.

        """
        self._varId = ''
        self._valIndex = 0
        self._stopped = False

    def __iter__(self):
        return self

    def next(self):
        if self._stopped: raise StopIteration

        try:
            status, val, self._varId, self._valIndex, n = \
                    self._ws._iretrieve(self._varName, self._op,
                            self._varId, self._valIndex)
        except StopIteration, e:
            self._stopped = True
            raise e

        if status != 0: raise NwsOperationException('retrieval failed')
        return val

    def writeTo(self, fobj):
        """Write the next value to the specified file or file-like object.

        it.writeTo(fobj) -> number of bytes written to file

        This is very much like the "next" method, but unlike "next", is
        intended to be called explicitly.  It provides the same kind of
        functionality as the various NetWorkSpace findFile/fetchTryFile
        methods.  It is the easiest and most memory efficient way to
        non-destructively write all of the values of a variable to a
        file.

        Arguments:

        fobj -- File to write data to.

        """
        if self._stopped: return 0

        try:
            status, val, self._varId, self._valIndex, n = \
                    self._ws._iretrieve(self._varName, self._op,
                            self._varId, self._valIndex, fobj)
            return n
        except StopIteration, e:
            self._stopped = True
            return 0

class NwsTextFile(object):
    def __init__(self, ws, varName, mode):
        assert mode in ['r', 'w', 'a']
        self._ws = ws
        self._varName = varName
        self._wbuffer = []
        self._rbuffer = ''
        self.closed = False
        self.mode = mode
        self.name = varName
        self.softspace = False

        if mode == 'w':
            # delete the variable if it exists
            try: ws.deleteVar(varName)
            except NwsOperationException: pass

        if mode == 'r':
            self._it = ws.ifindTry(varName)

        ws.declare(varName, FIFO)

    def __del__(self):
        try: self.close()
        except: pass

    def __iter__(self):
        if self.mode not in ['r', 'rb']:
            raise ValueError('iteration not allowed in mode %r' % self.mode)
        return self

    def __repr__(self):
        state = self.closed and "closed" or "open"
        return "<%s nws file %r, mode %r at %s>" % (state, self.name, self.mode, hex(id(self)))

    def __str__(self):
        return repr(self)

    def next(self):
        x = self.readline()
        if not x:
            raise StopIteration()
        return x

    def read(self, n=-1):
        if self.closed:
            raise ValueError('I/O operation on closed file')

        if self.mode not in ['r', 'rb']:
            raise ValueError('read operation not allowed in mode %r' % self.mode)

        navail = len(self._rbuffer)

        # read one or more values from the nws variable until
        # we have enough data, or we run out of values
        if n < 0 or navail < n:
            buffer = [self._rbuffer]
            try:
                while n < 0 or navail < n:
                    x = self._it.next()
                    if not isinstance(x, str):
                        raise ValueError('read a non-string from %r: %r' % (self, x))
                    buffer.append(x)
                    navail += len(x)
            except StopIteration:
                pass
            self._rbuffer = ''.join(buffer)

        if n < 0:
            x = self._rbuffer
            self._rbuffer = ''
        else:
            x = self._rbuffer[:n]
            self._rbuffer = self._rbuffer[n:]
        return x

    def readline(self):
        if self.closed:
            raise ValueError('I/O operation on closed file')

        if self.mode not in ['r', 'rb']:
            raise ValueError('read operation not allowed in mode %r' % self.mode)

        x = self._rbuffer
        buffer = [x]
        try:
            while not x.endswith('\n'):
                x = self._it.next()
                if not isinstance(x, str):
                    raise ValueError('read a non-string from %r: %r' % (self, x))
                buffer.append(x)
        except StopIteration:
            pass

        x = ''.join(buffer)
        self._rbuffer = ''
        return x

    def readlines(self):
        return [x for x in self]

    def write(self, s):
        if self.closed:
            raise ValueError('I/O operation on closed file')

        if self.mode not in ['w', 'a', 'wb', 'ab']:
            raise ValueError('write operation not allowed in mode %r' % self.mode)

        if s:
            x = s.split('\n')
            if len(x) > 1:
                self._wbuffer.append(x[0])
                y = ''.join(self._wbuffer)
                self._ws.store(self._varName, y + '\n')
                for y in x[1:-1]:
                    self._ws.store(self._varName, y + '\n')
                self._wbuffer = []
                if x[-1]:
                    self._wbuffer.append(x[-1])
            else:
                self._wbuffer.append(s)

    def writelines(self, lst):
        for line in lst:
            self.write(line)

    def close(self):
        if not self.closed:
            try:
                if self.mode in ['w', 'wb', 'a', 'ab']:
                    self.flush()
            finally:
                try: del self._wbuffer
                except AttributeError: pass
                try: del self._rbuffer
                except AttributeError: pass
                self.closed = True

    def flush(self):
        if self.closed:
            raise ValueError('I/O operation on closed file')

        if self.mode not in ['w', 'a', 'wb', 'ab']:
            raise ValueError('flush operation not allowed in mode %r' % self.mode)

        # flush any buffered data without a newline
        if self._wbuffer:
            y = ''.join(self._wbuffer)
            if y:
                self._ws.store(self._varName, y)
            self._wbuffer = []

    def isatty(self):
        if self.closed:
            raise ValueError('I/O operation on closed file')
        return False
