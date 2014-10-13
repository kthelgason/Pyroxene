import sys
import socket
import argparse

LISTEN_ADDRESS = ''

class ConnectionContext(object):

    def __init__(self, client):
        pass

class ProxyServer(object):

    def __init__(self, port, logfile, ipv6=False):
        self.port = port
        self.sock = self.initialize_connection(port, ipv6)

    def initialize_connection(self, port, ipv6):
        sock_type = socket.AF_INET6 if ipv6 else socket.AF_INET
        sock = socket.socket(sock_type, socket.SOCK_STREAM)
        sock.bind((LISTEN_ADDRESS, port))
        sock.listen(5)
        return sock

    def start(self):
        while True:
            clientSock, clientAddr = self.sock.accept()
            connectionContext(clientSock)
            clientSock.close()


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
        print "Goodbye!"
