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
        print(self._headers)
        return f

    def parse_message_data(self, f):
        length = self._headers.get("Content-Length")
        encoding = self._headers.get("Transfer-Encoding")
        payload = b''
        if length:
            if not self._data_remaining:
                self._data_remaining = int(length)
                print("reading data, length:", self._data_remaining)
            f, self._data_remaining = self.read_data_length(f, self._data_remaining)
            if self._data_remaining == 0:
                print("done, payload length:", len(self._payload))
                self._state = PARSER_STATE_DATA
        elif encoding and encoding == "chunked":
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
            print("Finish prev chunk with: ", self._data_remaining, "data remaining")
            print("Payload size:", len(self._payload), "ends with: ", self._payload[-20:])
            f, self._data_remaining = self.read_data_length(f, self._data_remaining)
            if self._data_remaining == 0:
                self._payload += f.readline()
                self._data_remaining = None
        while not self._data_remaining:
            l = f.readline()
            if not l:
                return f
            self._payload += l
            try:
                chunk_length = int(l, 16)
                print("chunk length: ", chunk_length)
            except Exception as e:
                print(e)
            if chunk_length == 0:
                print("All chunks done")
                self._payload += CRLF
                self._state = PARSER_STATE_DATA
                return f
            f, self._data_remaining = self.read_data_length(f, chunk_length)
            if self._data_remaining == 0:
                self._payload += f.readline()
                self._data_remaining = None

    def read_data_length(self, f, amount):
        chunk = f.read(amount)
        self._payload += chunk
        print("read chunk, length:",len(chunk))
        return f, amount - len(chunk)


class HTTPConnection(object):
    """
    This class represents a connection object.
    It keeps track of the state for a connection.
    The packet queue keeps track of packets destined
    to this client, which makes sending really simple.
    """
    class ConnState:
        """ Dummy class to represent a state enum """
        ready, read, sent, close = range(4)

    def __init__(self):
        self.state = ConnState.ready
        self.sock = sock
        self.addr = addr
        self.message_buffer = b''
        self.packet_queue = Queue()
        self.parser = HTTPMessageParser()

    def recv(self, on_done, on_disconnect):
        """
        Takes two callbacks as parameters.
        `on_done` is called when the read event is finished.
        `on_disconnect` is called if the remote end hangs up,
        and is responsible for shutting down the connection cleanly.
        """
        if self.state != ConnState.ready:
            raise Exception("Reading from socket not in ready state")
        try:
            data = self.sock.recv(BUFSIZE)
            if not data:
                # If data is empty, that indicates that the client will
                # send no more data on this connection, and that it should
                # be closed when all data in flight has reached it's destination.
                print("Connection closed while reading.")
                on_disconnect(self)

            # Each connection has a message buffer that stores the entire
            # message recieved.
            self.message_buffer += data
            # Check to see if we have a complete packet.
            packet = self.parser.try_parse(data)

            if packet:
                on_done(self, packet)
                # Reset connection state to get ready for next read.
                self.message_buffer = b''
                self.parser = HTTPMessageParser()

        # Non-blocking sockets throw exceptions if they would block
        # we ignore those errors and try again.
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
            print("No buffer!")
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


class Forwarder(object):
    """
    This class implements the main proxy state-machine.
    """
    def __init__(self, client_sock, client_addr):
        self.client = ClientConnection(client_sock, client_addr)
        self.servers = {}

    def get_connection_for_fileno(fd):
        if self.client.sock.fileno() == fd:
            return self.client
        else:
            return self.servers[fd]


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
        print("Getting peer for ", conn.sock.fileno())
        if packet.is_request():
            host = packet.get_header("Host")
            addr = socket.gethostbyname(host)
            sockfd = conn.server_connections.get(addr)
            print(sockfd)
            if not sockfd:
                return self.connect_to_server(addr, packet.port, conn)
            return self.connections[sockfd]

        print(conn.client_connection)
        return self.connections[conn.client_connection]

    def disconnect_from_server(self, conn):
        self.epoll.modify(conn.sock.fileno(), 0)
        conn.sock.shutdown(socket.SHUT_RDWR)

    def accept_connection(self):
        client_sock, client_addr = self.sock.accept()
        client_sock.setblocking(0)
        self.epoll.register(client_sock.fileno(), select.EPOLLIN)
        proxy = Forwarder(client_sock, client_addr)
        self.connections[client_sock.fileno()] = proxy

    def on_read_callback(self, conn, packet):
        print("Done reading from conn ", conn.sock.fileno())
        server = self.get_server_connection(conn, packet)
        self.epoll.modify(server.sock.fileno(), select.EPOLLOUT)
        server.packet_queue.put(packet, False)
        print("packet added to server queue, length:", len(packet.toRaw()))

    def on_send_callback(self, conn):
        print("Done sending to ", conn.sock.fileno())
        self.epoll.modify(conn.sock.fileno(), select.EPOLLIN)

    def start(self):
        try:
            while True:
                events = self.epoll.poll(1)
                for fileno, event in events:
                    if fileno == self.sock.fileno():
                        print("new connection")
                        self.accept_connection()
                    elif event & select.EPOLLIN:
                        print("Read event on ", fileno)
                        self.connections[fileno].recv(self.on_read_callback,
                                                      self.disconnect_from_server)
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
