from __future__ import print_function

import sys
import socket
import argparse
import threading
from time import strftime, gmtime


LISTEN_ADDRESS = ''
BUFSIZE = 4096
SUPPORTED_METHODS = ['GET', 'POST', 'HEAD']

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


class ConnectionContext(object):

    def __init__(self, client_sock, client_addr):
        self.client_sock = client_sock
        self.client = client_addr
        self.parse_packet()

    def parse_packet(self):
        """
        request-line = method SP request-target SP HTTP-version CRLF
        *( header-field CRLF)
        CRLF
        [ message-body ]
        """
        message = self.client_sock.recv(BUFSIZE)
        lines = message.split('\r\n')
        method, path, protocol_version = lines.pop(0).split()
        if not method in SUPPORTED_METHODS:
            self.send_unsupported_method_error()
        headers, host = self.parse_headers(lines)

        #TODO: response code
        try:
            Logger.instance().log(self.client[0], self.client[1], method, path, 200)
        except LoggerException:
            print("No logfile")

        self.server_sock = self.connect_to_server(host)
        self.server_sock.send(message)
        self.proxy_loop()


    def connect_to_server(self, host):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        host_addr = socket.gethostbyname(host)
        sock.connect((host_addr, 80))
        return sock


    def parse_headers(self, data):
        host = ""
        headers = []
        while data[0] != '':
            header_line = data.pop(0)
            if header_line.startswith("Host:"):
                host = header_line.split()[1]
            headers.append(header_line)

        return headers, host

    def send_unsupported_method_error(self):
        print("Unsupported Method!")



    def proxy_loop(self):
        while True:
            data = self.server_sock.recv(BUFSIZE)
            if not data:
                break
            self.client_sock.send(data)


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
