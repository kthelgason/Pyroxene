from __future__ import print_function

import sys
import socket
import argparse
import os


LISTEN_ADDRESS = ''
BUFSIZE = 8192
SUPPORTED_METHODS = ['GET', 'POST', 'HEAD']
SUPPORTED_VERSIONS = {'HTTP/1.1'}

class ConnectionContext(object):

    def __init__(self, client):
        self.client_sock = client
        self.request_headers = {}
        self.response_headers = {}
        message = self.client_sock.recv(BUFSIZE)
        if message:
            self.parse_request(message)
            self.server_sock = self.connect_to_server(self.request_headers.get('host'))
            self.server_sock.send(message)
            self.proxy_loop()

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
        method, path, protocol_version = line.split()
        if not method in SUPPORTED_METHODS:
            self.send_unsupported_method_error()
        self.request_headers = self.parse_headers(headers)

    def parse_response(self, message):
        if not message:
            return
        line, headers, data = self.split_message(message)
        protocol_version, code, reason = line.split(' ', 2)
        if not protocol_version in SUPPORTED_VERSIONS:
            self.send_unsupported_version_error()
        self.response_headers = self.parse_headers(headers)
        #return len(line) + len(headers) + 6
        return line, headers, data

    def parse_headers(self, header_lines):
        headers = {}
        header_lines = header_lines.split('\r\n')
        for header in header_lines:
            parts = header.split(':')
            key = parts[0].strip()
            value = ':'.join(parts[1:]).strip()
            headers[key.lower()] = value.lower()
        return headers

    def send_unsupported_method_error(self):
        print("Unsupported Method!")

    def send_unsupported_version_error(self):
        print("Unsupported Protocol Version!")

    def proxy_loop(self):
        msg = self.server_sock.recv(BUFSIZE)
        line, headers, data = self.parse_response(msg)
        content_length = sys.maxint
        if "content-length" in self.response_headers:
            content_length = int(self.response_headers.get("content-length"))
        while len(data) != content_length and not data.endswith('\r\n\r\n'):
            msg = self.server_sock.recv(BUFSIZE)
            if not msg:
                break
            data += msg
        packet = line + '\r\n' + headers + '\r\n\r\n' + data
        self.client_sock.send(packet)

    def recv_all(self, s):
        total_data=[]
        while True:
            data = s.recv(BUFSIZE)
            if not data: break
            total_data.append(data)
        return ''.join(total_data)

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
