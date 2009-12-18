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

import sys, getopt, traceback
from nws.client import NetWorkSpace
from nws.client import NwsServerException, NwsConnectException
from nws.client import NwsUnpicklingException

MAXLEN = 1000

def babelfish(host, port):
    ws = NetWorkSpace('Python babelfish', host, port)

    while 1:
        try:
            v = ws.fetch('food')
            t = str(v)
            if len(t) > MAXLEN:
                t = t[:MAXLEN] + '[WARNING: output truncated]'
            elif not t:
                t = repr(v)
        except NwsUnpicklingException, e:
            t = 'Unpickling error: ' + str(e.exception)
        ws.store('doof', t)

def main(argv):
    host = 'localhost'
    port = 8765

    try:
        opts, args = getopt.getopt(argv, 'h:p:')

        for opt, arg in opts:
            if opt == '-h':
                host = arg
            elif opt == '-p':
                port = int(arg)
            else:
                raise 'internal error: out-of-sync with getopt'

        if len(args) > 0:
            print >> sys.stderr, 'ignoring unused arguments:', args

        babelfish(host, port)
    except getopt.GetoptError, e:
        print >> sys.stderr, e.msg
        sys.exit(1)
    except ValueError, e:
        print >> sys.stderr, "option %s requires an integer argument" % opt
        sys.exit(1)
    except NwsConnectException, e:
        # the server isn't running
        print >> sys.stderr, e.args[0]
    except (KeyboardInterrupt, NwsServerException, SystemExit):
        # either the user or the server is telling us to shutdown
        pass
    except:
        traceback.print_exc()

if __name__ == '__main__':
    main(sys.argv[1:])
