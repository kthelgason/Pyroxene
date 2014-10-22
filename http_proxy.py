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
        else:
            print(message_line[0])
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
        print(self._headers)
        return f

    def parse_message_data(self, f):
        length = self._headers.get("Content-Length")
        encoding = self._headers.get("Transfer-Encoding")
        payload = b''

        if length:
            if not self._data_remaining:
                self._data_remaining = int(length)
            f, self._data_remaining = self.read_data_length(f, self._data_remaining)
            if self._data_remaining == 0:
                self._state = PARSER_STATE_DATA
        elif encoding and encoding == "chunked":
            f = self.parse_chunked_data(f)
        else:
            if self._type == "response":
                #TODO: send malformed response
                print("aw =(")
            else:
                print("parsing request")
                self._state = PARSER_STATE_DATA
        return f

    def parse_chunked_data(self, f):
        if self._data_remaining:
            print(self._data_remaining)
            f, self._data_remaining = self.read_data_length(f, self._data_remaining)
            if self._data_remaining == 0:
                self._payload += f.readline()
                self._data_remaining = None
        while not self._data_remaining:
            l = f.readline()
            if not l:
                return f
            self._payload += l + f.readline()
            try:
                chunk_length = int(l, 16)
                print("chunk length: ", chunk_length)
            except Exception as e:
                print(e)
            if chunk_length == 0:
                self._state = PARSER_STATE_DATA
                return f
            f, self._data_remaining = self.read_data_length(f, chunk_length)
            if self._data_remaining == 0:
                self._payload += f.readline()
                self._data_remaining = None

    def read_data_length(self, f, amount):
        chunk = f.read(amount)
        self._payload += chunk
        return f, amount - len(chunk)


class AbstractConnection(object):
    def __init__(self):
        pass

    def recv(self, on_done, on_disconnect):
        try:
            data = self.sock.recv(BUFSIZE)
            if not data:
                # EOF
                print("Connection closed while reading.")
                on_disconnect(self)

            self.message_buffer += data
            packet = self.parser.try_parse(data)
            if packet:
                on_done(self, packet)
                self.parser = HTTPMessageParser()
        except socket.error as e:
            if e.args[0] != errno.EAGAIN:
                raise e
            print("Socket error! ", e)

    def send(self, fd, on_done):
        try:
            packet = self.packet_queue.get(False)
        except Empty as e:
            return
        if not self.message_buffer:
            self.message_buffer = packet.toRaw()
        try:
            sent = self.sock.send(self.message_buffer)
            if sent == len(self.message_buffer):
                self.message_buffer = b''
                on_done(self)
            else:
                self.message_buffer = self.message_buffer[sent:]
        except socket.error as e:
            if e.args[0] != errno.EAGAIN:
                raise e
            print("Socket error! ", e)


class ClientConnection(AbstractConnection):
    def __init__(self, sock, addr):
        self.sock = sock
        self.server_connections = {}
        self.addr = addr
        self.packet_state = 0
        self.message_buffer = b''
        self.response_queue = Queue()
        self.parser = HTTPMessageParser()


class ServerConnection(AbstractConnection):
    def __init__(self, sock, addr, clientfd):
        self.sock = sock
        self.client_connection = clientfd
        self.addr = addr
        self.packet_state = 0
        self.message_buffer = b''
        self.packet_queue = Queue()
        self.parser = HTTPMessageParser()


class ConnectionContext(object):

    def __init__(self, client_sock, client_addr):
        self.client_sock     = client_sock
        self.client_addr     = client_addr
        self.server_sock     = None
        self.server_addr     = None
        self.expecting_reply = False
        self.packet          = None
        self.close           = False
        self.message_buffer  = b''
        self.factory         = HTTPMessageFactory()

    def get_socket_for_fileno(self, fileno):
        if self.client_sock and self.client_sock.fileno() == fileno:
            return self.client_sock
        elif self.server_sock and self.server_sock.fileno() == fileno:
            return self.server_sock
        else:
            return None

    def get_opposing_socket_for_fileno(self, fileno):
        if self.client_sock and self.client_sock.fileno() == fileno:
            return self.server_sock
        elif self.server_sock and self.server_sock.fileno() == fileno:
            return self.client_sock
        else:
            return None

    def close_socket_for_fileno(self, fileno):
        if self.client_sock and self.client_sock.fileno() == fileno:
            self.client_sock.close()
            self.clinet_sock = None
        elif self.server_sock and self.server_sock.fileno() == fileno:
            self.server_sock.close()
            self.server_sock = None

    def connect_to_server(self, on_disconnect, on_connect):
        host = self.packet.get_header("Host")
        addr = socket.gethostbyname(host)
        if addr != self.server_addr:
            if self.server_sock:
                # An existing connection to another server must be terminated.
                on_disconnect(self, self.server_sock.fileno())
            port = self.packet.port
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            sock.connect((addr, port))
            sock.setblocking(0)
            self.server_addr = addr
            self.server_sock = sock
            on_connect(self, sock.fileno(), select.EPOLLOUT)

    def send_unsupported_method_error(self):
        print("Unsupported Method!")

    def send_unsupported_version_error(self):
        print("Unsupported Protocol Version!")

    def close_sockets(self):
        self.client_sock.close()
        if self.server_sock:
            self.server_sock.close()
        print("Closing connection.")

    def recv(self, fd, on_done, on_disconnect, on_connect):
        in_sock = self.get_socket_for_fileno(fd)
        try:
            data = in_sock.recv(BUFSIZE)
            if not data:
                # EOF
                if not self.message_buffer:
                    on_disconnect(self, fd)
                    return
                print("Connection closed while reading.")

            if CRLF*2 in self.message_buffer:
                self.packet = HTTPMessageFactory.packetFromBuffer(
                        self.message_buffer)
                if self.packet.__class__ == Request:
                    self.connect_to_server(on_disconnect, on_connect)
                out_sock = self.get_opposing_socket_for_fileno(fd)
                on_done(self, out_sock.fileno())
        except socket.error as e:
            if e.args[0] != errno.EAGAIN:
                raise e
            print("Socket error! ", e)


    def send(self, fd, on_done):
        out_sock = self.get_socket_for_fileno(fd)
        if not self.message_buffer:
            return
        try:
            sent = out_sock.send(self.message_buffer)
            if sent == len(self.message_buffer):
                close = self.packet.get_header("Connection")
                if close and close == 'close':
                    self.close = True
                self.expecting_reply = not self.expecting_reply
                self.message_buffer = b''
                self.packet = None
                on_done(self, out_sock.fileno())
            else:
                self.message_buffer = self.message_buffer[sent:]
        except socket.error as e:
            if e.args[0] != errno.EAGAIN:
                raise e
            print("Socket error! ", e)

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

    def register(self, conn, type_):
        self.epoll.register(conn.sock.fileno(), type_)
        self.connections[conn.sock.fileno()] = conn

    def unregister(self, conn, fd):
        print("Unregistering ", fd)
        conn.close_socket_for_fileno(fd)
        self.epoll.unregister(fd)
        # TODO: old servers?

    def connect_to_server(self, addr, port, conn):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        sock.connect((addr, port))
        sock.setblocking(0)
        server = ServerConnection(sock, addr, conn.sock.fileno())
        self.connections[sock.fileno()] = server
        self.register(server, select.EPOLLOUT)
        conn.server_connections[addr] = sock.fileno()
        return server

    def get_server_connection(self, conn, packet):
        if packet.is_request():
            host = packet.get_header("Host")
            addr = socket.gethostbyname(host)
            sockfd = conn.server_connections.get(addr)
            if not sockfd:
                return self.connect_to_server(addr, packet.port, conn)

        return self.connections[conn.client_connection]

    def disconnect_from_server(self, conn):
        if conn.sock:
            conn.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_CORK, 0)
            self.epoll.modify(conn.sock.fileno(), 0)
            conn.sock.shutdown(socket.SHUT_RDWR)

    def accept_connection(self, fd):
        client_sock, client_addr = self.sock.accept()
        client_sock.setblocking(0)
        conn = ClientConnection(client_sock, client_addr)
        self.register(conn, select.EPOLLIN)

    def on_read_callback(self, conn, packet):
        print("Done reading")
        server = self.get_server_connection(conn, packet)
        server.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_CORK, 1)
        self.epoll.modify(server.sock.fileno(), select.EPOLLOUT)
        server.packet_queue.put(packet, False)

    def on_send_callback(self, conn):
        print("Done sending")
        self.epoll.modify(conn.sock.fileno(), select.EPOLLIN)

    def start(self):
        try:
            while True:
                events = self.epoll.poll(1)
                for fileno, event in events:
                    if fileno == self.sock.fileno():
                        print("new connection")
                        self.accept_connection(fileno)
                    elif event & select.EPOLLIN:
                        print("Read event on ", fileno)
                        self.connections[fileno].recv(self.on_read_callback,
                                                      self.unregister)
                    elif event & select.EPOLLOUT:
                        print("Write event on ", fileno)
                        self.connections[fileno].send(fileno,
                                                      self.on_send_callback)
                    elif event & (select.EPOLLHUP
                                | select.EPOLLERR):
                        print("aaaaand HUP!")
                        self.unregister(self.connections[fileno], fileno)
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
