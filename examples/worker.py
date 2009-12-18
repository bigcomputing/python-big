import socket, nws.client, time

def worker(*s, **kw):
    ws = nws.client.NetWorkSpace(*s, useUse=True)
    id = SleighRank
    ws.store("foo_%d" % id, socket.getfqdn())
    time.sleep(1)
    return (id, socket.getfqdn()) + s

def answer():
    time.sleep(1)
    return 42

if __name__ == '__main__':
    global SleighRank
    SleighRank = 72
    print worker('foo', 'localhost', 8765)
