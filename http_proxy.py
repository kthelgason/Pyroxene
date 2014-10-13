from __future__ import print_function

import sys
import socket
import argparse


LISTEN_ADDRESS = ''
BUFSIZE = 4096
SUPPORTED_METHODS = ['GET', 'POST', 'HEAD']

class ConnectionContext(object):

    def __init__(self, client):
        self.client_sock = client
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
        print(headers)
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
            print(data)
            if not data:
                break
            self.client_sock.send(data)


class ProxyServer(object):

    def __init__(self, port, logfile, ipv6=False):
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
            ConnectionContext(client_sock)
            client_sock.close()


def main():
    parser = argparse.ArgumentParser(
            description="A command-line http proxy server.")
    parser.add_argument('port', type=int, help="The port to start the server on")
    parser.add_argument('logfile', help='The logfile to log connections to')
    parser.add_argument('--ipv6', action="store_true", help="Use IPv6")
    args = parser.parse_args()
    ProxyServer(args.port, args.logfile, args.ipv6).start()

    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("Goodbye!")
