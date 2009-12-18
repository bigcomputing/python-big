#!/usr/bin/env python
import os, sys, threading, Queue, getopt
from Tkinter import *
from ScrolledText import ScrolledText
from nws.client import NetWorkSpace, FIFO

class Chat(threading.Thread):
    def __init__(self, user, varName, wsName, **opt):
        threading.Thread.__init__(self, name='Chat')
        self._user, self._varName, self._wsName = user, varName, wsName
        self._ws = NetWorkSpace(wsName, persistent=True, **opt)
        self._ws.declare(varName, FIFO)
        self._queue = Queue.Queue()
        recvWs = NetWorkSpace(wsName, **opt)
        self._ifind = recvWs.ifind(varName)
        self.setDaemon(True)
        self.start()

    def sendMessage(self, ent):
        msg = ent.get().strip()
        if len(msg) > 0:
            self._ws.store(self._varName, "%s: %s" % (self._user, msg))
        ent.delete(0, END)

    def checkForNewMessages(self, txt):
        txt.config(state=NORMAL)
        while not self._queue.empty():
            txt.insert(END, self._queue.get() + '\n')
            txt.see(END)
        txt.config(state=DISABLED)
        txt.after(1000, self.checkForNewMessages, txt)

    def run(self):
        for val in self._ifind:
            self._queue.put(val)

def usage():
    print >> sys.stderr, "usage: %s [option ...]" % os.path.basename(sys.argv[0])
    print >> sys.stderr, "where option can be:"
    print >> sys.stderr, "  -h <host>"
    print >> sys.stderr, "  -p <port>"
    print >> sys.stderr, "  -n <name of chat room>"
    print >> sys.stderr, "  -u <username>"
    sys.exit(1)

if __name__ == '__main__':
    wsName, varName = 'chatroom', 'chat'
    user = os.environ.get('USER') or os.environ.get('USERNAME' , 'anonymous')
    nwsargs = {}
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'h:p:n:u:')
        for opt, arg in opts:
            if opt == '-h':
                nwsargs['serverHost'] = arg
            elif opt == '-p':
                nwsargs['serverPort'] = int(arg)
            elif opt == '-n':
                wsName = arg
            elif opt == '-u':
                user = arg
            else:
                raise 'internal error: out-of-sync with getopt'

        if args:
            print >> sys.stderr, "illegal argument(s) specified:", " ".join(args)
            usage()

        root = Tk()
        root.title('NWS Chat Client')
        ent = Entry(root)
        ent.pack(side=BOTTOM, fill=X, padx=4, pady=4)
        ent.focus()
        txt = ScrolledText(root)
        txt.pack(expand=YES, fill=BOTH, padx=4, pady=4)

        chat = Chat(user, varName, wsName, **nwsargs)
        ent.bind('<Return>', lambda event: chat.sendMessage(ent))
        chat.checkForNewMessages(txt)

        root.mainloop()
    except getopt.GetoptError, e:
        print >> sys.stderr, e.msg
        usage()
    except ValueError, e:
        print >> sys.stderr, "option %s requires an integer argument" % opt
        usage()
    except SystemExit, e:
        sys.exit(e)
    except KeyboardInterrupt:
        pass
    except Exception, e:
        print >> sys.stderr, str(e)
