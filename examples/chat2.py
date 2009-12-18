#!/usr/bin/env python
import os, sys, threading, Queue
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

if __name__ == '__main__':
    wsName, varName = 'chatroom', 'chat'
    user = os.environ.get('USER') or os.environ.get('USERNAME', 'anonymous')

    root = Tk()
    root.title('NWS Chat Client')
    ent = Entry(root)
    ent.pack(side=BOTTOM, fill=X, padx=4, pady=4)
    ent.focus()
    txt = ScrolledText(root)
    txt.pack(expand=YES, fill=BOTH, padx=4, pady=4)

    chat = Chat(user, varName, wsName)
    ent.bind('<Return>', lambda event: chat.sendMessage(ent))
    chat.checkForNewMessages(txt)

    root.mainloop()
