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
SUPPORTED_VERSIONS = ['HTTP/1.1']

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
    def __init__(self, start_line, headers, data):
        # Class is abstract, should not be instantiated directly
        pass

    def parse_headers(self, header_lines):
        headers = {}
        for header in header_lines.split('\r\n'):
            parts = header.split(':', 1)
            key = parts[0]
            value = parts[1].strip()
            headers[key] = value
        return headers

    def get_header(self, key):
        return self.headers.get(key)

class Request(HTTP_Message):
    def __init__(self, req_line, headers, data):
        self.method, self.resource, self.protocol_version = req_line.split()
        self.headers = self.parse_headers(headers)
        self.data = data

    def toRaw(self):
        raw = ' '.join((self.method, self.resource, self.protocol_version))
        raw += '\r\n'
        for (key,value) in self.headers.iteritems():
            raw += key + ": " + value + '\r\n'
        raw += '\r\n'
        raw += self.data
        return raw


class Response(HTTP_Message):
    def __init__(self, resp_line, headers, data):
        self.protocol_version, self.status, self.reason = resp_line.split(' ', 2)
        self.headers = self.parse_headers(headers)
        self.data = data

    def toRaw(self):
        raw = ' '.join((self.protocol_version, self.status, self.reason))
        raw += '\r\n'
        for (key,value) in self.headers.iteritems():
            raw += key + ": " + value + '\r\n'
        raw += '\r\n'
        raw += self.data
        return raw


class ConnectionContext(object):

    def __init__(self, client_sock, client_addr):
        self.client_sock = client_sock
        self.client_addr = client_addr
        self.request_headers = {}
        self.response_headers = {}
        self.proxy_request()

    def connect_to_server(self, host):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        host_addr = socket.gethostbyname(host)
        sock.connect((host_addr, 80))
        return sock

    def split_message(self, message):
        """
         HTTP-message = start-line CRLF
                        *( header-field CRLF )
                        CRLF
                        [ message-body ]CRLF
        """
        line, rest = message.split('\r\n', 1)
        headers, data = rest.split('\r\n\r\n', 1)
        return line, headers, data

    def parse_request(self, message):
        line, headers, data = self.split_message(message)
        return Request(line, headers, data)

    def parse_response(self, message):
        line, headers, data = self.split_message(message)
        return Response(line, headers, data)


    def send_unsupported_method_error(self):
        print("Unsupported Method!")

    def send_unsupported_version_error(self):
        print("Unsupported Protocol Version!")

    def proxy_request(self):
        message = self.client_sock.recv(BUFSIZE)
        request = self.parse_request(message)
        self.server_sock = self.connect_to_server(request.get_header("Host"))
        self.server_sock.send(request.toRaw())
        self.handle_response()

    def handle_response(self):
        msg = self.server_sock.recv(BUFSIZE)
        response = self.parse_response(msg)
        content_length = response.get_header("Content-length")

        #if not content_length:
            #content_length = sys.maxint

        #while len(data) != content_length and not data.endswith('\r\n\r\n'):
            #msg = self.server_sock.recv(BUFSIZE)
            #if not msg:
                #break
            #data += msg

        self.client_sock.send(response.toRaw())

    def recv_all(self, s):
        total_data=[]
        while True:
            data = s.recv(BUFSIZE)
            if not data: break
            total_data.append(data)
        return ''.join(total_data)

class ProxyServer(object):

    def __init__(self, port, ipv6=False):
        self.port = port
        self.sock = self.initialize_connection(port, ipv6)

    def initialize_connection(self, port, ipv6):
        sock_type = socket.AF_INET6 if ipv6 else socket.AF_INET
        sock = socket.socket(sock_type, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((LISTEN_ADDRESS, port))
        sock.listen(5)
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
