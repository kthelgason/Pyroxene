from __future__ import print_function

import sys
import os
import socket
import argparse
import threading
from time import strftime, gmtime


LISTEN_ADDRESS = ''
BUFSIZE = 8192
SUPPORTED_METHODS = ['GET', 'POST', 'HEAD']
SUPPORTED_PROTOCOLS = ['HTTP/1.1']
CRLF = '\r\n'

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
        self.logfmt = " : %s:%d %s %s : %d\n"
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
    def create_message(self, from_socket):
        f = from_socket.makefile('rb')
        headers = {}
        message_line = f.readline().split(" ", 2)

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
        f.close()
        return Request(*args) if type_ == "request" else Response(*args)



class ConnectionContext(object):

    def __init__(self, client_sock, client_addr):
        self.client_sock = client_sock
        self.client_addr = client_addr
        self.server_sock = None
        print("New connection from %s" % client_addr[0])
        self.proxy_request()

    def connect_to_server(self, host):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_addr = socket.gethostbyname(host)
        sock.connect((self.server_addr, 80))
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

    def proxy_request(self):
        while True:
            try:
                req = HTTPMessageFactory.create_message(self.client_sock)
                self.connect_to_server(req.get_header("Host"))
                self.server_sock.send(req.toRaw())
                resp = HTTPMessageFactory.create_message(self.server_sock)
                connection = resp.get_header("Connection")
                self.client_sock.send(resp.toRaw())
                if connection and connection == "close":
                    break
            except Exception, e:
                print("aw =( ")
                break

        self.close_sockets()


class ProxyServer(object):

    def __init__(self, port, ipv6=False):
        self.port = port
        self.sock = self.initialize_connection(port, ipv6)

    def initialize_connection(self, port, ipv6):
        sock_type = socket.AF_INET6 if ipv6 else socket.AF_INET
        sock = socket.socket(sock_type, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((LISTEN_ADDRESS, port))
        sock.listen(1)
        return sock

    def start(self):
        while True:
            client_sock, client_addr = self.sock.accept()
            ConnectionContext(client_sock, client_addr)
            client_sock.close()

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
