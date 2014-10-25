from __future__ import print_function, with_statement

import sys
import os
import errno
import binascii
import socket
import select
import signal
import argparse
import threading
import urlparse
import cStringIO
import subprocess
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

# TODO: change to the same sort of enum as below
PARSER_STATE_NONE, PARSER_STATE_LINE, PARSER_STATE_HEADERS, PARSER_STATE_DATA = range(4)


class LoggerException(Exception):
    pass

class parseException(Exception):
    pass

class EmptySocketException(Exception):
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
        print("creating packet with content size ", len(self._payload))
        print("content-length was ", self._headers.get("Content-Length"))
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
            return f
        self._message_line = message_line
        self._state = PARSER_STATE_LINE
        return f

    def parse_message_headers(self, f):
        header = f.readline()
        if header == CRLF:
            header = f.readline()
        while header != CRLF:
            parts = header.split(':', 1)
            key = parts[0]
            value = parts[1].strip() if len(parts) == 2 else ''
            self._headers[key] = value
            header = f.readline()
            if not header:
                return
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
            length = self._headers.get("content-cength")
        encoding = self._headers.get("Transfer-Encoding")
        if not encoding:
            encoding = self._headers.get("transfer-encoding")
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
                self._payload += l
            try:
                chunk_length = int(l, 16)
            except Exception as e:
                print(e)
            if chunk_length == 0:
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
        return f, amount - len(chunk)


class HTTPConnection(object):
    """
    This class represents a connection object.
    It keeps track of the state for a connection.
    The packet queue keeps track of packets destined
    to this client, which makes sending really simple.
    """

    def __init__(self, sock, addr):
        self.sock = sock
        self.addr = addr
        self.message_buffer = b''
        self.packet_queue = Queue()
        self.parser = HTTPMessageParser()

    def disconnect(self):
        self.sock.shutdown(socket.SHUT_RDWR)

    def enqueue(self, packet):
        """
        Add a given packet to this connections packet queue.
        """
        self.packet_queue.put(packet)

    def recv(self):
        """
        and is responsible for shutting down the connection cleanly.
        The return type signals the proxyContext wether we recieved a
        whole packet or whether this socket should be read again.
        """
        try:
            data = self.sock.recv(BUFSIZE)
            if not data:
                # If data is empty, that indicates that the client will
                # send no more data on this connection, and that it should
                # be closed when all data in flight has reached it's destination.
                print("Connection closed while reading.")
                raise EmptySocketException("Socket empty")

            # Each connection has a message buffer that stores the entire
            # message recieved.
            self.message_buffer += data
            # Check to see if we have a complete packet.
            packet = self.parser.try_parse(data)

            if packet:
                #on_done(self, packet)
                # Reset connection state to get ready for next read.
                self.message_buffer = b''
                self.parser = HTTPMessageParser()

            return packet
        # Non-blocking sockets throw exceptions if they would block
        # we ignore those errors and try again.
        except socket.error as e:
            if e.args[0] != errno.EAGAIN:
                raise e
            print("Socket error! ", e)

    def send(self):
        """
        Pops a packet from this connections packet queue and sends it.
        Uses the messagebuffer to store the amout sent when the buffer
        is too big to send all at once. Returns true or false depending
        on wether send was successful.
        """
        # If there is not a buffer waiting to be sent,
        # fill it with the next packet in the queue.
        if not self.message_buffer:
            try:
                packet = self.packet_queue.get(False)
            except Empty as e:
                return True
            self.message_buffer = packet.toRaw()
        try:
            sent = self.sock.send(self.message_buffer)
            if sent == len(self.message_buffer):
                print(len(self.message_buffer))
                self.message_buffer = b''
                return True
            else:
                self.message_buffer = self.message_buffer[sent:]
                print(len(self.message_buffer))
                return False
        except socket.error as e:
            if e.args[0] != errno.EAGAIN:
                raise e
            print("Socket error! ", e)


class ConnState:
    """ Dummy class to represent a state enum """
    ready, req_recv, req_sent, resp_recv, resp_sent, close  = range(6)

class ProxyContext(object):
    """
    Keeps track of the relationship between a client and one or more servers.
    This class runs the client and server state machines and makes sure
    things happen in lock-step.
    """

    def __init__(self, sock, addr,
                 register_callback,
                 recv_callback,
                 send_callback,
                 disconnect_callback
                ):
        self.client = HTTPConnection(sock, addr)
        self.servers = {}
        self.state = ConnState.ready
        self.register = register_callback
        self.on_recv = recv_callback
        self.on_send = send_callback
        self.on_disc = disconnect_callback

    def get_host_by_name(self, host):
        """
        A function that wraps gethostbyname with a timeout
        as the function is provided by the kernel and by
        default blocks everything for 30 secs if the host
        is invalid.
        """
        name = ""
        # Signal handler raises error
        def handler(signum, frame):
            raise Exception("timed out!")
        signal.signal(signal.SIGALRM, handler)
        # send SIGALRM to ourselves in 1 sec.
        signal.alarm(1)
        try:
            name = socket.gethostbyname(host)
            # If gethostbyname succeeds we unregister the signal handler.
            signal.signal(signal.SIGALRM, signal.SIG_IGN)
        # Catch exception thrown by signal handler if timout has expired.
        except Exception as e:
            print("Gethost timed out!")
            #TODO: handle
        return name

    def connect_to_server(self, packet):
        host = packet.get_header("Host")
        print("Connecting to %s for %d" % (host, self.client.sock.fileno()))
        addr = self.get_host_by_name(host)
        if not addr:
            #TODO: send 404
            return
        port = packet.port
        # If we do not already have an open connection to this server
        # we go ahead and create it.
        if host not in self.servers.keys():
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((addr, port))
            sock.setblocking(0)
            server = HTTPConnection(sock, addr)
            self.servers[sock.fileno()] = server
            self.servers[host] = server
            self.register(self, sock.fileno(), select.EPOLLOUT)

    def handle_packet(self, packet):
        print("Handling packet for: ", packet.get_header("Host"))
        if packet.__class__ == Request:
            server = self.servers.get(packet.get_header("Host"))
            if not server:
                self.connect_to_server(packet)
                server = self.servers[packet.get_header("Host")]
            server.enqueue(packet)
            self.on_recv(server.sock.fileno())
        else:
            self.client.enqueue(packet)
            print(packet.headers)
            self.on_recv(self.client.sock.fileno())

    def close(self, conn):
        print("Closing ", conn.sock.fileno())
        self.on_disc(conn.sock.fileno())
        conn.disconnect()
        self.servers = {key: value for key, value in self.servers.items()
                if value != conn}

    def close_all(self):
        self.close(self.client)
        # Iterate over servers and disconnect them as well
        for (key, server) in self.servers.items():
            # this check is nessecary as each server appears twice in the
            # dictionary, once keyed on the FD and once on the hostname.
            if type(key) == int:
                self.close(server)

    def recv(self, fd):
        if fd == self.client.sock.fileno():
            host = self.client
        else:
            host = self.servers[fd]
        try:
            packet = host.recv()
            if packet:
                self.handle_packet(packet)
        except EmptySocketException as e:
            # Recieved EOF from socket
            if host == self.client:
                self.close_all()
            else:
                self.close(host)


    def send(self, fd):
        if fd == self.client.sock.fileno():
            host = self.client
        else:
            host = self.servers[fd]
        if host.send():
            self.on_send(fd)

    def cleanup(self, fd):
        print("Closing connection ", fd)
        server = self.servers.get(fd)
        if server:
            server.sock.close()
        else:
            self.client.sock.close()


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

    def register(self, conn, fd, type_):
        print("Registering ", fd)
        self.epoll.register(fd, type_)
        self.connections[fd] = conn

    def unregister(self, conn):
        print("Unregistering ", fd)
        fd = conn.sock.fileno()
        conn.sock.close()
        conn.sock = None
        del self.connections[fd]
        self.epoll.unregister(fd)

    def on_disconnect_callback(self, fd):
        print("Unregistering ", fd)
        del self.connections[fd]
        self.epoll.unregister(fd)

    def accept_connection(self, fd):
        client_sock, client_addr = self.sock.accept()
        client_sock.setblocking(0)
        conn = ProxyContext(client_sock, client_addr, self.register,
                            self.on_read_callback, self.on_send_callback,
                            self.on_disconnect_callback)
        self.register(conn, client_sock.fileno(), select.EPOLLIN)

    def on_read_callback(self, fd):
        print("Done reading from ", fd)
        self.epoll.modify(fd, select.EPOLLOUT)

    def on_send_callback(self, fd):
        print("Done sending to ", fd)
        self.epoll.modify(fd, select.EPOLLIN)

    def start(self):
        try:
            while True:
                events = self.epoll.poll(1)
                for fileno, event in events:
                    try:
                        if fileno == self.sock.fileno():
                            print("new connection")
                            self.accept_connection(fileno)
                        elif event & select.EPOLLIN:
                            print("Read event on ", fileno)
                            self.connections[fileno].recv(fileno)
                        elif event & select.EPOLLOUT:
                            print("Write event on ", fileno)
                            self.connections[fileno].send(fileno)
                        elif event & (select.EPOLLHUP
                                    | select.EPOLLERR):
                            print("aaaaand HUP!")
                            self.connections[fileno].cleanup(fileno)
                    except KeyError:
                        continue
        finally:
            self.epoll.unregister(self.sock.fileno())
            self.epoll.close()
            self.sock.close()


def main():
    parser = argparse.ArgumentParser(
            description="A full-featured http proxy server.")
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
