from __future__ import print_function

import sys
import os
import errno
import socket
import select
import argparse
import threading
import urlparse
import cStringIO
from time import strftime, gmtime

PROXY_VERSION = "0.1"
PROXY_NAME = "Pyroxene"
VIA = PROXY_VERSION + " " + PROXY_NAME
LISTEN_ADDRESS = ''
BUFSIZE = 4096
SUPPORTED_METHODS = ['GET', 'POST', 'HEAD', 'CONNECT']
SUPPORTED_PROTOCOLS = ['HTTP/1.1']
CRLF = '\r\n'

CONNECTION_ESTABLISHED = CRLF.join([
            'HTTP/1.1 200 Connection established',
            'Proxy-agent: {}'.format(VIA),
            CRLF
        ])

class LoggerException(Exception):
    pass

class Logger(object):
    """
    Our own thread-safe logging implementation.
    Rather than working arount the standard library logger implementation
    we implement our own logger object that's (in our oppinion) easier to
    work with.

    This class is a singleton implementation and calling the instance class
    method returns a shared instance.

    set_logfile must be called before attempting to use the logger.
    """
    _instance_lock = threading.Lock()
    _instance = None

    @classmethod
    def instance(klass):
        """
        Returns a shared instance of the class.
        Uses locks to ensure no contest between multiple
        threads trying to access it concurrently.
        """
        if not klass._instance:
            with klass._instance_lock:
                if not klass._instance:
                    klass._instance = klass()
        return klass._instance

    def __init__(self):
        self.logfmt = " : %s:%d %s %s : %s\n"
        self.timefmt = "%Y-%m-%dT%H:%M:%S+0000"
        self.logfile = None
        self._file_lock = threading.Lock()

    def set_logfile(self, filename):
        self.logfile = filename

    def log(self, *args):
        """
        Logs to the file.
        Raises an exception if set_logfile has not been called.
        """
        if not self.logfile:
            raise LoggerException
        with self._file_lock:
            with open(self.logfile, 'a') as f:
                timestamp = strftime(self.timefmt, gmtime())
                f.write(timestamp + self.logfmt % args)


class HTTP_Message(object):
    def __init__(self, headers, data):
        pass

    def get_header(self, key):
        return self.headers.get(key)

    def toRaw(self):
        raw = ''
        for (key,value) in self.headers.iteritems():
            raw += key + ": " + value + CRLF
        raw += CRLF
        if self.data:
            raw += self.data
        return raw


class Request(HTTP_Message):
    def __init__(self, req_line, headers, data):
        self.method, self.resource, self.protocol_version = req_line
        url = urlparse.urlsplit(self.resource)
        if self.method == "CONNECT":
            print("SECURE")
            headers["Host"], self.port = url.path.split(":")
        elif url:
            headers["Host"], self.port = url.hostname, url.port if url.port else 80
        self.headers = headers
        self.data = data

    def toRaw(self):
        raw = ' '.join((self.method, self.resource, self.protocol_version))
        raw += super(Request, self).toRaw()
        return raw


class Response(HTTP_Message):
    def __init__(self, resp_line, headers, data):
        self.protocol_version, self.status, self.reason = resp_line
        self.headers = headers
        self.data = data

    def toRaw(self):
        raw = ' '.join((self.protocol_version, self.status, self.reason))
        raw += super(Response, self).toRaw()
        return raw


class HTTPMessageFactory(object):

    @classmethod
    def packetFromBuffer(self, buffer_):
        f = cStringIO.StringIO(buffer_)

        headers = {}
        message_line = f.readline()
        message_line = message_line.split(" ", 2)
        if message_line == ['']:
            raise Exception

        if message_line[0] in SUPPORTED_PROTOCOLS:
            type_ = "response"
        elif message_line[0] in SUPPORTED_METHODS:
            type_ = "request"
        else:
            print(message_line[0])
            pass
            # TODO: malformed request 400 error
        header = f.readline()
        while header != CRLF:
            parts = header.split(':', 1)
            key = parts[0]
            value = parts[1].strip()
            headers[key] = value
            header = f.readline()
        via = headers.get("Via")
        if via:
            via += ", " + VIA
            headers["Via"] = via
        else:
            headers["Via"] = VIA
        length = headers.get("Content-Length")
        encoding = headers.get("Transfer-Encoding")
        payload = CRLF * 2

        if length:
            amount = int(length)
            s = []
            while amount > 0:
                chunk = f.read(amount)
                if not chunk:
                    break
                s.append(chunk)
                amount -= len(chunk)
            payload = ''.join(s)

        elif encoding and encoding == "chunked":
            # Read first line of chunked response
            l = f.readline()
            s = [l]
            try:
                content_len = int(l, 16)
                while(content_len > 0):
                    chunk = f.read(content_len)
                    s.append(chunk)
                    if not chunk:
                        break
                    l = f.readline()
                    l = f.readline()
                    s.append(l)
                    content_len = int(l, 16)
            except Exception, e:
                print(e)
            s.append(CRLF)
            payload = ''.join(s)

        args = (message_line, headers, payload)
        return Request(*args) if type_ == "request" else Response(*args)


class ConnectionContext(object):

    def __init__(self, client_sock, client_addr):
        self.client_sock = client_sock
        self.client_addr = client_addr
        self.server_sock = None
        self.server_addr = None
        self.expecting_reply = False
        self.packet = None
        self.message_buffer = b''

    def connect_to_server(self):
        host = self.packet.get_header("Host")
        port = self.packet.port
        addr = socket.gethostbyname(host)
        if addr != self.server_addr:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((addr, port))
            sock.setblocking(0)
            self.server_addr = addr
            self.server_sock = sock

    def send_unsupported_method_error(self):
        print("Unsupported Method!")

    def send_unsupported_version_error(self):
        print("Unsupported Protocol Version!")

    def close_sockets(self):
        self.client_sock.close()
        if self.server_sock:
            self.server_sock.close()
        print("Closing connection.")

    def recv(self, on_done):
        in_sock = self.server_sock if self.expecting_reply else self.client_sock
        try:
            data = in_sock.recv(BUFSIZE)
            if not data:
                print("No data")
                return
            self.message_buffer += data
            print(len(self.message_buffer))
            if self.message_buffer.endswith(CRLF*2):
                print("Done reading")
                self.packet = HTTPMessageFactory.packetFromBuffer(
                        self.message_buffer)
                self.connect_to_server()
                on_done(self)
        except socket.error as e:
            if e.args[0] != errno.EAGAIN:
                raise e
            print(e)


    def send(self, on_done):
        out_sock = self.server_sock if not self.expecting_reply else self.client_sock
        if not self.message_buffer:
            return
        try:
            sent = out_sock.send(self.message_buffer)
            if sent == len(self.message_buffer):
                print("Done sending")
                self.expecting_reply = not self.expecting_reply
                self.message_buffer = b''
                self.packet = None
                on_done(self)
            else:
                self.message_buffer = self.message_buffer[sent:]
        except socket.error as e:
            if e.args[0] != errno.EAGAIN:
                raise e
            print(e)

    def tunnel(self, req):
        ## TODO after implementing polling
        self.client_sock.send(CONNECTION_ESTABLISHED)
        print(req.get_header("Host"))
        self.connect_to_server(req.get_header("Host"), int(req.port))
        while True:
            chunk = self.client_sock.recv(BUFSIZE)
            if not chunk:
                print("done")
                break
            self.client_sock.send(chunk)


class ProxyServer(object):

    def __init__(self, port, ipv6=False):
        self.port = port
        self.connections = {}
        self.sock = self.initialize_connection(port, ipv6)

    def initialize_connection(self, port, ipv6):
        sock_type = socket.AF_INET6 if ipv6 else socket.AF_INET
        sock = socket.socket(sock_type, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((LISTEN_ADDRESS, port))
        sock.listen(5)
        sock.setblocking(0)

        self.epoll = select.epoll()
        self.epoll.register(sock.fileno(), select.EPOLLIN)
        return sock

    def register(self, conn):
        self.epoll.register(conn.client_sock.fileno(), select.EPOLLIN)
        #self.epoll.register(conn.server_sock.fileno(), select.EPOLLOUT)
        self.connections[conn.client_sock.fileno()] = conn
        #self.connections[conn.server_sock.fileno()] = conn

    def unregister(self, fileno):
        conn = self.connections[fileno]
        del self.connections[conn.client_sock.fileno()]
        del self.connections[conn.server_sock.fileno()]
        self.epoll.unregister(conn.client_sock.fileno())
        self.epoll.unregister(conn.server_sock.fileno())
        conn.close_sockets()

    def accept_connection(self):
        client_sock, client_addr = self.sock.accept()
        client_sock.setblocking(0)
        conn = ConnectionContext(client_sock, client_addr)
        self.register(conn)

    def done_reading(self, conn):
        self.epoll.register(conn.server_sock.fileno(), select.EPOLLOUT)
        self.connections[conn.server_sock.fileno()] = conn

    def done_sending(self, conn):
        if conn.expecting_reply:
            self.epoll.modify(conn.client_sock.fileno(), select.EPOLLOUT)
            self.epoll.modify(conn.server_sock.fileno(), select.EPOLLIN)
        else:
            self.epoll.modify(conn.server_sock.fileno(), select.EPOLLOUT)
            self.epoll.modify(conn.client_sock.fileno(), select.EPOLLIN)

    def start(self):
        try:
            while True:
                events = self.epoll.poll(1)
                for fileno, event in events:
                    if fileno == self.sock.fileno():
                        print("new connection.")
                        self.accept_connection()
                    elif event & select.EPOLLIN:
                        self.connections[fileno].recv(self.done_reading)
                    elif event & select.EPOLLOUT:
                        self.connections[fileno].send(self.done_sending)
                    elif event & select.EPOLLHUP:
                        print("aaaaand HUP!")
                        self.unregister(fileno)
        finally:
            self.epoll.unregister(self.sock.fileno())
            self.epoll.close()
            self.sock.close()


def main():
    parser = argparse.ArgumentParser(
            description="A command-line http proxy server.")
    parser.add_argument('port', type=int, help="The port to start the server on")
    parser.add_argument('logfile', help='The logfile to log connections to')
    parser.add_argument('--ipv6', action="store_true", help="Use IPv6")
    args = parser.parse_args()
    Logger.instance().set_logfile(args.logfile)

    print("Starting server on port %d." % args.port)

    ProxyServer(args.port, args.ipv6).start()

    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("Goodbye!")
