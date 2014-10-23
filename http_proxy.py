from __future__ import print_function, with_statement

import sys
import os
import errno
import binascii
import socket
import select
import argparse
import threading
import urlparse
import cStringIO
from time import strftime, gmtime
from Queue import Queue, Empty

PROXY_VERSION = "0.1"
PROXY_NAME = "Pyroxene"
VIA = PROXY_VERSION + " " + PROXY_NAME
LISTEN_ADDRESS = ''
BUFSIZE = 65536
SUPPORTED_METHODS = ['GET', 'POST', 'HEAD', 'CONNECT']
SUPPORTED_PROTOCOLS = ['HTTP/1.1']
CRLF = '\r\n'

CONNECTION_ESTABLISHED = CRLF.join([
            'HTTP/1.1 200 Connection established',
            'Proxy-agent: {}'.format(VIA),
            CRLF
        ])

PARSER_STATE_NONE, PARSER_STATE_LINE, PARSER_STATE_HEADERS, PARSER_STATE_DATA = range(4)
READ_ONLY = select.POLLIN | select.POLLPRI | select.POLLHUP | select.POLLERR
READ_WRITE = READ_ONLY | select.POLLOUT

class LoggerException(Exception):
    pass

class parseException(Exception):
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

    def is_request(self):
        return self.__class__ == Request


class Request(HTTP_Message):
    def __init__(self, req_line, headers, data):
        super(Request, self).__init__(headers, data)
        self.method, self.resource, self.protocol_version = req_line
        url = urlparse.urlsplit(self.resource)
        path = url.path
        if path == '':
            path = '/'
        if not url.query == '':
            path += '?' + url.query
        if not url.fragment == '':
            path += '#' + url.fragment
        self.resource = path
        if self.method == "CONNECT":
            headers["Host"], self.port = url.path.split(":")
            self.port = int(self.port)
        elif url:
            headers["Host"], self.port = url.hostname, url.port if url.port else 80
        if self.method == "HTTP/1.0" and headers["Connection"]:
            headers["Connection"] = "close"
        self.headers = headers
        self.data = data

    def toRaw(self):
        raw = ' '.join((self.method, self.resource, self.protocol_version))
        raw += super(Request, self).toRaw()
        return raw


class Response(HTTP_Message):
    def __init__(self, resp_line, headers, data):
        super(Response, self).__init__(headers, data)
        self.protocol_version, self.status, self.reason = resp_line
        self.headers = headers
        self.data = data

    def toRaw(self):
        raw = ' '.join((self.protocol_version, self.status, self.reason))
        raw += super(Response, self).toRaw()
        return raw


class HTTPMessageParser(object):

    def __init__(self):
        self._message_line = ['']
        self._headers = {}
        self._payload = b''
        self._type = None
        self._data_remaining = None
        self._state = PARSER_STATE_NONE

    def try_parse(self, buffer_):
        f = cStringIO.StringIO(buffer_)
        try:
            if self._state == PARSER_STATE_NONE:
                f = self.parse_message_line(f)
            if self._state == PARSER_STATE_LINE:
                f = self.parse_message_headers(f)
            if self._state == PARSER_STATE_HEADERS:
                f = self.parse_message_data(f)
            if self._state == PARSER_STATE_DATA:
                return self.create_packet()
            return None
        except Exception as e:
            print(e)

    def create_packet(self):
        print("creating packet")
        args = (self._message_line, self._headers, self._payload)
        return Request(*args) if self._type == "request" else Response(*args)

    def parse_message_line(self, f):
        ml = f.readline()
        message_line = ml.split(" ", 2)
        if message_line == ['']:
            raise parseException("No message line")
        if message_line[0] in SUPPORTED_PROTOCOLS:
            self._type = "response"
        elif message_line[0] in SUPPORTED_METHODS:
            self._type = "request"
            print(message_line[0])
        else:
            print("AAAAAAA", message_line[0])
            pass
        self._message_line = message_line
        self._state = PARSER_STATE_LINE
        return f

    def parse_message_headers(self, f):
        header = f.readline()
        if header == CRLF:
            header = f.readline()
        while header and header != CRLF:
            parts = header.split(':', 1)
            key = parts[0]
            value = parts[1].strip() if len(parts) == 2 else ''
            self._headers[key] = value
            header = f.readline()
        via = self._headers.get("Via")
        if via:
            via += ", " + VIA
            self._headers["Via"] = via
        else:
            self._headers["Via"] = VIA
        self._state = PARSER_STATE_HEADERS
        return f

    def parse_message_data(self, f):
        length = self._headers.get("Content-Length")
        if not length:
            length = self._headers.get("content-length")
        encoding = self._headers.get("Transfer-Encoding")
        if not encoding:
            encoding = self._headers.get("transfer-encoding")
        payload = b''
        if length:
            if not self._data_remaining:
                self._data_remaining = int(length)
                print(self._data_remaining)
            f, self._data_remaining = self.read_data_length(f, self._data_remaining)
            print(self._data_remaining)
            if self._data_remaining == 0:
                self._state = PARSER_STATE_DATA
        elif encoding and encoding == "chunked":
            print("chunked")
            f = self.parse_chunked_data(f)
        else:
            if self._type == "response":
                print(self._message_line)
                print(self._headers)
                #TODO: send malformed response
                print("aw =(")
                if self._message_line[1] != 200:
                    self._state = PARSER_STATE_DATA
            else:
                print("parsing request")
                self._state = PARSER_STATE_DATA
        return f

    def parse_chunked_data(self, f):
        if self._data_remaining:
            f, self._data_remaining = self.read_data_length(f, self._data_remaining)
            if self._data_remaining == 0:
                self._payload += f.readline()
                self._data_remaining = None
        while not self._data_remaining:
            l = f.readline()
            if not l:
                return f
            self._payload += l
            while l == CRLF or l == '':
                l = f.readline()
                if not l:
                    return f
            try:
                chunk_length = int(l, 16)
            except Exception as e:
                print(e)
            if chunk_length == 0:
                print("All chunks done")
                self._state = PARSER_STATE_DATA
                l = f.readline()
                while l:
                    self._payload += l
                    l = f.readline()
                return f
            f, self._data_remaining = self.read_data_length(f, chunk_length)
            if self._data_remaining == 0:
                l =  f.readline()
                self._payload += l
                self._data_remaining = None

    def read_data_length(self, f, amount):
        chunk = f.read(amount)
        self._payload += chunk
        return f, amount - len(chunk)


class AbstractConnection(object):
    def __init__(self, conn_type):
        self.conn_type = conn_type
        pass

    def recv(self, on_done, on_disconnect):
        try:
            data = self.sock.recv(BUFSIZE)
            print("Receiving from ", self.conn_type)
            if not data:
                # EOF
                print("Connection closed while reading.")
                on_disconnect(self)
                return
            self.message_buffer += data
            print(len(data))
            packet = self.parser.try_parse(data)
            if packet:
                on_done(self, packet)
                self.message_buffer = b''
                self.parser = HTTPMessageParser()
        except socket.error as e:
            if e.args[0] != errno.EAGAIN:
                raise e
            print("Socket error! ", e)

    def send(self, fd, on_done):
        close = False
        try:
            print("Sending to ", self.conn_type)
            packet = self.packet_queue.get(False)
            if self.conn_type == 'server':
                print("server")
                connection = packet.get_header('Connection')
                if connection and connection == 'close':
                    close = True
        except Empty as e:
            print("Empty queue!")
            if not self.message_buffer:
                on_done(self, True, close)
                return
        if not self.message_buffer:
            self.message_buffer = packet.toRaw()
        try:
            sent = self.sock.send(self.message_buffer)
            if sent == len(self.message_buffer):
                self.message_buffer = b''
                on_done(self, False, close)
            else:
                print("send more")
                self.message_buffer = self.message_buffer[sent:]
        except socket.error as e:
            if e.args[0] != errno.EAGAIN:
                raise e
            print("Socket error! ", e)


class ClientConnection(AbstractConnection):
    def __init__(self, sock, addr):
        super(ClientConnection, self).__init__('client')
        self.sock = sock
        self.server_connections = {}
        self.addr = addr
        self.packet_state = 0
        self.message_buffer = b''
        self.packet_queue = Queue()
        self.parser = HTTPMessageParser()


class ServerConnection(AbstractConnection):
    def __init__(self, sock, addr, clientfd):
        super(ServerConnection, self).__init__('server')
        self.sock = sock
        self.client_connection = clientfd
        self.addr = addr
        self.packet_state = 0
        self.message_buffer = b''
        self.packet_queue = Queue()
        self.parser = HTTPMessageParser()


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

    def register(self, conn, type_):
        self.epoll.register(conn.sock.fileno(), type_)
        self.connections[conn.sock.fileno()] = conn

    def unregister(self, conn):
        print("Unregistering ", conn.sock.fileno())
        if conn.packet_queue.qsize() > 0:
            print("queue length is: ", conn.packet_queue.qsize())
            exit(0)
        self.epoll.unregister(conn.sock.fileno())
        conn.sock.close()
        conn.sock = None
        # TODO: old servers?

    def connect_to_server(self, addr, port, conn):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print(addr)
        sock.connect((addr, port))
        sock.setblocking(0)
        server = ServerConnection(sock, addr, conn.sock.fileno())
        self.connections[sock.fileno()] = server
        self.register(server, select.EPOLLOUT)
        conn.server_connections[addr] = sock.fileno()
        return server

    def get_server_connection(self, conn, packet):
        print("Getting peer for ", conn.sock.fileno())
        if conn.conn_type == 'client':
            host = packet.get_header("Host")
            addr = socket.gethostbyname(host)
            sockfd = conn.server_connections.get(addr)
            print(sockfd)
            if not sockfd:
                server = self.connect_to_server(addr, packet.port, conn)
                return server
            return self.connections[sockfd]
        else:
            print(conn.client_connection)
            return self.connections[conn.client_connection]

    def disconnect_from_server(self, conn):
        self.epoll.modify(conn.sock.fileno(), 0)
        conn.sock.shutdown(socket.SHUT_RDWR)

    def accept_connection(self, fd):
        client_sock, client_addr = self.sock.accept()
        client_sock.setblocking(0)
        conn = ClientConnection(client_sock, client_addr)
        self.register(conn, READ_ONLY)

    def on_read_callback(self, conn, packet):
        print("Done reading from conn ", conn.sock.fileno())
        server = self.get_server_connection(conn, packet)
        if server.sock:
            self.epoll.modify(server.sock.fileno(), READ_WRITE)
        server.packet_queue.put(packet, False)

    def on_send_callback(self, conn, empty, close):
        print("Done sending to ", conn.sock.fileno())
        if empty:
            self.epoll.modify(conn.sock.fileno(), READ_ONLY)
        else:
            self.epoll.modify(conn.sock.fileno(), READ_WRITE)
        if close:
            self.disconnect_from_server(conn)
            self.disconnect_from_server(conn.client_connection)

    def start(self):
        try:
            while True:
                events = self.epoll.poll(2)
                for fileno, event in events:
                    print(events)
                    print(event, " from: ", fileno)
                    if event & (select.POLLIN | select.POLLPRI):
                        print("Read event on ", fileno)
                        if fileno == self.sock.fileno():
                            print("new connection")
                            self.accept_connection(fileno)
                        else:
                            self.connections[fileno].recv(self.on_read_callback,
                                                          self.disconnect_from_server)
                    elif event & select.EPOLLOUT:
                        print("Write event on ", fileno)
                        self.connections[fileno].send(fileno,
                                                      self.on_send_callback)
                    elif event & (select.EPOLLHUP
                                | select.EPOLLERR):
                        print("aaaaand HUP!")
                        self.unregister(self.connections[fileno])
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
