# -*- coding: utf-8 -*-
"""
Pyroxene v.0.1

An event-driven proxy server.

license: 2-Clause BSD License.

The server listens for incoming connections on it's port, given in
a command-line argument. The server uses epoll to poll file descriptors
to determine if they are ready to send/recieve data. Read events on the
server socket are new connections being established. We create a ProxyContext
instance to handle the connection between that client and the servers it
talks to.

"""
from __future__ import print_function, with_statement

import sys
import os
import shutil
import errno
import socket
import select
import signal
import argparse
import threading
import urlparse
import time
import calendar
import cPickle as pickle
import cStringIO
from time import strftime, gmtime, sleep
from Queue import Queue, Empty
from proxy_cache import Cache
from hashlib import md5
from fnmatch import fnmatch

PROXY_VERSION = "0.1"
PROXY_NAME = "Pyroxene"
VIA = PROXY_VERSION + " " + PROXY_NAME
LISTEN_ADDRESS = ''
BUFSIZE = 65536
SUPPORTED_METHODS = ['GET', 'POST', 'HEAD', 'CONNECT']
SUPPORTED_PROTOCOLS = ['HTTP/1.1', 'HTTP/1.0']
CRLF = '\r\n'
CACHE_DIR = os.path.abspath("cache")

CONNECTION_ESTABLISHED = CRLF.join([
    'HTTP/1.1 200 Connection established',
    'Proxy-agent: {}'.format(VIA), CRLF
])

PARSER_STATE_NONE, PARSER_STATE_LINE, PARSER_STATE_HEADERS, PARSER_STATE_DATA, PARSER_STATE_ERROR = range(5)

STATUS_CODES = {
    "400": "Bad Request\r\n",
    "404": "Not Found\r\n",
    "501": "Not Implemented\r\n",
    "505": "HTTP Version Not Supported\r\n"
}

"""
We define a few custom exceptions to better indicate
what happened.
"""
class LoggerException(Exception):
    pass


class parseException(Exception):
    pass


class EmptySocketException(Exception):
    pass


class RequestError(Exception):
    """
    Indicates a problem with the request
    and that a response with statuscode `code`
    should be sent to the client
    """

    def __init__(self, code):
        self.code = code


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
    def instance(cls):
        """
        Returns a shared instance of the class.
        Uses locks to ensure no contest between multiple
        threads trying to access it concurrently.
        """
        if not cls._instance:
            with cls._instance_lock:
                if not cls._instance:
                    cls._instance = cls()
        return cls._instance

    def __init__(self):
        self.logfmt = " : %s:%d %s %s : %s %s\n"
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


class DateTimeParser():
    """
    Contains methods to parse date strings to unix time
    """
    def __init__(self):
        pass

    weekdayname = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    monthname = [None,
                 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    weekdayname_lower = [name.lower() for name in weekdayname]
    monthname_lower = [name and name.lower() for name in monthname]

    @classmethod
    def timegm(cls, year, month, day, hour, minute, second):
        """
        Convert time tuple in GMT to seconds since epoch, GMT
        """
        EPOCH = 1970
        if year < EPOCH:
            raise ValueError("Years prior to %d not supported" % (EPOCH,))
        assert 1 <= month <= 12
        days = 365*(year-EPOCH) + calendar.leapdays(EPOCH, year)
        for i in range(1, month):
            days = days + calendar.mdays[i]
        if month > 2 and calendar.isleap(year):
            days += 1
        days = days + day - 1
        hours = days*24 + hour
        minutes = hours*60 + minute
        seconds = minutes*60 + second
        return seconds

    @classmethod
    def stringToDatetime(cls, dateString):
        """
        Convert an HTTP date string (one of three formats)
        to seconds since epoch.
        """
        parts = dateString.split()

        if not parts[0][0:3].lower() in cls.weekdayname_lower:
            # Weekday might be omitted.
            try:
                return cls.stringToDatetime(b"Sun, " + dateString)
            except ValueError:
                # Guess not.
                pass

        partlen = len(parts)
        if (partlen == 5 or partlen == 6) and parts[1].isdigit():
            # 1st date format: Sun, 06 Nov 1994 08:49:37 GMT
            day = parts[1]
            month = parts[2]
            year = parts[3]
            time = parts[4]
        elif (partlen == 3 or partlen == 4) and parts[1].find('-') != -1:
            # 2nd date format: Sunday, 06-Nov-94 08:49:37 GMT
            day, month, year = parts[1].split('-')
            time = parts[2]
            year = int(year)
            if year < 69:
                year += 2000
            elif year < 100:
                year += 1900
        elif len(parts) == 5:
            # 3rd date format: Sun Nov  6 08:49:37 1994
            # ANSI C asctime() format.
            day = parts[2]
            month = parts[1]
            year = parts[4]
            time = parts[3]
        else:
            raise ValueError("Unknown datetime format %r" % dateString)

        day = int(day)
        month = int(cls.monthname_lower.index(month.lower()))
        year = int(year)
        hour, min_, sec = map(int, time.split(':'))
        return int(cls.timegm(year, month, day, hour, min_, sec))


class CacheEntry():
    """
    Class associated with an entry in the cache.
    Keeps track of the ttl of entries
    """
    def __init__(self, key, response, ttl):
        self.ttl = ttl
        self.key = key
        with open(os.path.join(CACHE_DIR, key), 'w+') as f:
            pickle.dump(response, f)

    def data(self):
        """
        Read the stored response from the cache
        and return a Response object
        """
        with open(os.path.join(CACHE_DIR, self.key), 'r') as f:
            data = pickle.load(f)
        if not self.is_fresh():
            data.needs_validation = True
        return data

    def is_fresh(self):
        return time.time() < self.ttl


class Cache():
    def __init__(self):
        self.map = {}

    def key_from_message(self, response):
        """
        Construct a cache-key from the response object
        by calculating the md5 hash of the URI and any
        `vary` header values that may be present.
        """
        # TODO: handle Vary: *
        key = response.abs_resource
        vary = response.get_header("Vary")
        if vary:
            for hf in vary.split(","):
                # if this header is present in the response,
                # add it to the digest
                key += response.get_header(hf) or ''
        key = md5(key).hexdigest()
        return key

    def should_cache(self, request, response):
        """
        Determines whether the response should be cached
        according to the RFC. If the response should be cached,
        this method returns it's expiration time.
        """
        if request.method != "GET" or response.status != "200":
            return None
        resp_directives = response.get_header("Cache-Control")
        req_directives = request.get_header("Cache-Control")
        if req_directives:
            req_directives = map(lambda x: x.strip().lower(),
                    req_directives.split(","))
            for directive in req_directives:
                if fnmatch("no-store", directive):
                    return None
                if fnmatch("no-cache", directive):
                    return None
        if request.get_header("Authorization"):
            return None
        if request.get_header("Cookie"):
            return None
        if resp_directives:
            resp_directives = map(lambda x: x.strip().lower(),
                    resp_directives.split(","))
            for directive in resp_directives:
                if fnmatch("no-store", directive):
                    return None
                if fnmatch("private", directive):
                    return None
                if fnmatch("s-maxage*", directive):
                    return time.time() + float(directive.split("=")[1])
                if fnmatch(directive, "max-age*"):
                    return time.time() + float(directive.split("=")[1])
        expires = response.get_header("Expires")
        date = response.get_header("Date")
        if expires and date:
            try:
                return DateTimeParser.stringToDatetime(expires)
            except ValueError as e:
                pass
        return None

    def store(self, request, response):
        ttl = self.should_cache(request, response)
        if ttl:
            key = self.key_from_message(request)
            self.map[key] = CacheEntry(key, response, ttl)

    def retrieve(self, request):
        key = self.key_from_message(request)
        cached_response = self.map.get(key)
        return cached_response.data() if cached_response else None


class HTTP_Message(object):
    """
    A parent class for HTTP response and requests messages.
    This class is never instatiated directly.
    """

    def __init__(self, headers, data):
        self.headers = headers
        self.data = data

    def get_header(self, key):
        return self.headers.get(key)

    def toRaw(self):
        """
        return a raw string representation of the packet.
        """
        raw = ''
        for (key, value) in self.headers.iteritems():
            raw += key + ": " + value + CRLF
        raw += CRLF
        if self.data:
            raw += self.data
        return raw


class Request(HTTP_Message):
    """
    In-memory representation of a HTTP request.
    The initializer deals with rewriting headers and other
    duties a proxy should perform.
    """

    def __init__(self, req_line, headers, data):
        super(Request, self).__init__(headers, data)
        self.method, self.resource, self.protocol_version = req_line
        # keep track of the aboslute path, before rewriting.
        self.abs_resource = self.resource
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
            # replace the Host header field with the value from the resource
            # string, as per RFC 7230.
            headers["Host"], self.port = url.hostname, url.port if url.port else 80
        #if self.method == "POST":
        #    print(len(data))
        self.url = headers.get("Host")
        self.headers = headers
        self.data = data

    def toRaw(self):
        raw = ' '.join((self.method, self.resource, self.protocol_version))
        raw += super(Request, self).toRaw()
        return raw


class Response(HTTP_Message):
    def __init__(self, resp_line, headers, data):
        super(Response, self).__init__(headers, data)
        # This fixes responses that do not include a reason
        if len(resp_line) < 3: resp_line.append('')
        self.protocol_version, self.status, self.reason = resp_line
        if self.protocol_version == "HTTP/1.0" and headers["Connection"]:
            headers["Connection"] = "close"

        if self.status != "200":
            headers["Connection"] = "close"
        self.headers = headers
        self.data = data
        # Used by the cache to indicate that a validation request
        # should be sent.
        self.needs_validation = False

    def toRaw(self):
        raw = ' '.join((self.protocol_version, self.status, self.reason))
        raw += super(Response, self).toRaw()
        return raw


class HTTPMessageParser(object):
    """
    This class encapsulates the message-parsing functionality.
    It is insantiated for each connection object and keeps track of
    the state of the packet recieved so far.

    As we cannot be sure that we recieve the whole packet at once,
    we must keep all intermediate state in the message parser.
    """

    def __init__(self):
        self._message_line = ''
        self._headers = {}
        self._payload = []
        self._type = None
        self._key = None
        self._value = None
        self._data_remaining = None
        self._state = PARSER_STATE_NONE

    @classmethod
    def validation_packet_for_request(cls, request, last_modified):
        headers = request.headers
        headers["If-Modified-Since"] = last_modified
        host = headers["Host"]
        message_line = ["GET", "http://" + host + request.resource,
                        "HTTP/1.1\r\n"]
        return Request(message_line, headers, "")

    @classmethod
    def error_packet_for_code(cls, code):
        return Response(["HTTP/1.1", code, STATUS_CODES[code]],
                        {"Content-Length": "0", "Connection": "close", "Server": VIA},
                        "")

    def try_parse(self, buffer_):
        """
        Try to generate a packet from the current string.
        The packet is implemented as a state-machine keeping track
        of how much of the message we have recieved thusfar.
        """
        # Create a file object from the buffer
        f = cStringIO.StringIO(buffer_)
        if self._state == PARSER_STATE_NONE:
            f = self.parse_message_line(f)
        if self._state == PARSER_STATE_LINE:
            f = self.parse_message_headers(f)
        if self._state == PARSER_STATE_HEADERS:
            self.parse_message_data(f)
        if self._state == PARSER_STATE_DATA:
            return self.create_packet()
        return None

    def create_packet(self):
        args = (self._message_line, self._headers, ''.join(self._payload))
        return Request(*args) if self._type == "request" else Response(*args)

    def parse_message_line(self, f):
        self._message_line += f.readline()
        # Handle empty line at start of packet
        if self._message_line == CRLF:
            self._message_line += f.readline()
        # Some message lines are so long that we don't get the whole thing
        # in one recv call
        if not self._message_line or not self._message_line.endswith(CRLF):
            return f
        message_line = self._message_line.split(" ", 2)
        if message_line == ['']:
            raise parseException("No message line")
        if message_line[0] in SUPPORTED_PROTOCOLS:
            self._type = "response"
        elif message_line[0] in SUPPORTED_METHODS:
            self._type = "request"
        # The response is not malformed, but we don't support HTTP/1.0
        elif "HTTP" in message_line[0]:
            raise RequestError("505")
        # Malformed packet.
        else:
            raise RequestError("501")
        self._message_line = message_line
        # Transition state once the message-line is read and approved
        self._state = PARSER_STATE_LINE
        return f

    def parse_message_headers(self, f):
        header = f.readline()
        # Possible empty line at start of headers
        if header == CRLF:
            header = f.readline()
        # Finish entering the long value from before
        if self._key and self._value:
            self._headers[self._key] = self._value + header
            self._value = None
            self._key = None
            header = f.readline()
        while header and header != CRLF:
            parts = header.split(':', 1)
            key = parts[0]
            value = parts[1].strip() if len(parts) == 2 else ''
            self._headers[key] = value
            header = f.readline()
            # Empty line in middle of headers indicates we have yet
            # to recieve more.
            if not header:
                self._key = key
                self._value = value
                return f
        # Add via header
        via = self._headers.get("Via")
        if via:
            via += ", " + VIA
            self._headers["Via"] = via
        else:
            self._headers["Via"] = VIA
        # Transition state
        self._state = PARSER_STATE_HEADERS
        return f

    def parse_message_data(self, f):
        # check for content-length or chunked data.
        # we need to check for both upper and lower case strings
        # as some servers for some reason send headers in uncapitalized.
        length = self._headers.get("Content-Length")
        if not length:
            length = self._headers.get("content-length")
        encoding = self._headers.get("Transfer-Encoding")
        if not encoding:
            encoding = self._headers.get("transfer-encoding")
        if length:
            if not self._data_remaining:
                self._data_remaining = int(length)
            f, self._data_remaining = self.read_data_length(f, self._data_remaining)
            if self._data_remaining == 0:
                self._state = PARSER_STATE_DATA
        elif encoding and encoding == "chunked":
            f = self.parse_chunked_data(f)
        else:
            # assume no data if neither content-length or encoding is present.
            self._state = PARSER_STATE_DATA
        return f

    def parse_chunked_data(self, f):
        """ reads data from a chunked packet """
        if self._data_remaining:
            f, self._data_remaining = self.read_data_length(f, self._data_remaining)
            if self._data_remaining == 0:
                self._payload.append(f.readline())
                self._data_remaining = None
        while not self._data_remaining:
            l = f.readline()
            if not l:
                return f
            self._payload.append(l)
            while l == CRLF or l == '':
                l = f.readline()
                if not l:
                    return f
                self._payload.append(l)
            try:
                chunk_length = int(l, 16)
            except Exception as e:
                raise e
            if chunk_length == 0:
                self._payload.append(CRLF)
                if len(self._payload) < 8:
                    # First chunk has length 0
                    self._headers["Connection"] = "close"
                self._state = PARSER_STATE_DATA
                return f
            f, self._data_remaining = self.read_data_length(f, chunk_length)
            if self._data_remaining == 0:
                self._payload.append(f.readline())
                self._data_remaining = None

    def read_data_length(self, f, amount):
        """ Reads a chunk of data, and returns the amount read """
        chunk = f.read(amount)
        self._payload.append(chunk)
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
        self.request = None
        self.message_buffer = b''
        self.recv_buffer = b''
        self.packet_queue = Queue()
        self.parser = HTTPMessageParser()
        self.close = False
        self.https = False

    def shutdown(self):
        """
        Attempt to gracefully shutdown this connection.
        Returns True if it has disconnected previously.
        This can occur if the remote terminates the connection.
        """
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
            return False
        except Exception:
            return True

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
                if self.message_buffer:
                    raise RequestError("400")
                # If data is empty, that indicates that the client will
                # send no more data on this connection, and that it should
                # be closed when all data in flight has reached it's destination.
                raise EmptySocketException("Socket empty")

            # Each connection has a message buffer that stores the entire
            # message recieved.
            self.recv_buffer += data
            if self.https:
                packet = self.recv_buffer
            else:
                packet = self.parser.try_parse(data)

            # Check to see if we have a complete packet.
            if packet:
                # Reset connection state to get ready for next read.
                self.recv_buffer = b''
                self.parser = HTTPMessageParser()

            return packet
        # Non-blocking sockets throw exceptions if they would block
        # we ignore those errors and try again.
        except socket.error as e:
            # TODO might me redundant now, needs testing
            if e.args[0] == errno.ECONNRESET:
                raise EmptySocketException("reset")
            elif e.args[0] != (errno.EAGAIN | errno.EBADF):
                raise e

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
            except Empty:
                return True
            if not self.https:
                self.message_buffer = packet.toRaw()
            else:
                self.message_buffer = packet
        try:
            sent = self.sock.send(self.message_buffer)
            if sent == len(self.message_buffer):
                self.message_buffer = b''
                return False
            else:
                self.message_buffer = self.message_buffer[sent:]
                return False
        # Non-blocking sockets throw exceptions if they would block
        # we ignore those errors and try again.
        except socket.error as e:
            if e.args[0] != errno.EAGAIN:
                raise e


class ProxyContext(object):
    """
    Keeps track of the relationship between a client and one or more servers.
    It has several callbacks to the main server to modify the epoll state.
    Server connections are tracked in a dict indexed by file descriptor and by
    hostname. Each server appears twice in the dict for this reason.
    """

    def __init__(self, sock, addr,
                 cache,
                 register_callback,
                 recv_callback,
                 send_callback,
                 disconnect_callback
    ):
        self.client = HTTPConnection(sock, addr)
        self.cache = cache
        self.servers = {}
        self.register = register_callback
        self.on_recv = recv_callback
        self.on_send = send_callback
        self.on_disc = disconnect_callback
        self.close_connection = False

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
        signal.alarm(2)
        try:
            name = socket.gethostbyname(host)
        # Catch exception thrown by signal handler if timout has expired.
        except Exception:
            pass
        # If gethostbyname succeeds we unregister the signal handler.
        signal.signal(signal.SIGALRM, signal.SIG_IGN)
        return name

    def connect_to_server(self, packet):
        host = packet.get_header("Host")
        addr = self.get_host_by_name(host)
        if not addr:
            return False
        port = packet.port
        # If we do not already have an open connection to this server
        # we go ahead and create it.
        if host not in self.servers.keys():
            # Set a 5 sec timeout on connect
            try:
                sock = socket.create_connection((addr, port), 5)
                sock.setblocking(0)
                server = HTTPConnection(sock, addr)
                self.servers[sock.fileno()] = server
                self.servers[host] = server
                self.register(self, sock.fileno(), select.EPOLLIN | select.EPOLLOUT)
            except Exception, socket.timeout:
                return False
        return True

    def validate_response(self, request, response):
        """
        Validates a stored cache response
        for a given request
        """
        last_modified = response.get_header("Last-Modified")
        if last_modified:
            packet = HTTPMessageParser.validation_packet_for_request(
                request, last_modified)
            self.handle_packet(None, packet, None, True)

    def handle_secure_packet(self, packet, host):
        if host == self.client:
            server = self.servers.get(host.request.url)
            if server:
                server.enqueue(packet)
                self.on_recv(server.sock.fileno())
        else:
            self.client.enqueue(packet)
            self.on_recv(self.client.sock.fileno())

    def handle_packet(self, host, packet, request=None, nocache=False):
        """
        This method does all the packet handling magic.
        """
        if packet.__class__ == Request:
            cached_response = self.cache.retrieve(packet)
            # A cached response was available
            if cached_response and not nocache:
                # Validate it if it's not fresh
                if cached_response.needs_validation:
                    self.validate_response(packet, cached_response)
                else:
                    self.handle_packet(None, cached_response, packet, nocache=True)
                return
            server = self.servers.get(packet.get_header("Host"))
            if not server:
                success = self.connect_to_server(packet)
                if not success:
                    response = HTTPMessageParser.error_packet_for_code("404")
                    self.handle_packet(None, response, packet)
                    return
                server = self.servers[packet.get_header("Host")]
            if packet.method == "CONNECT":
                host.https = True
                host.request = packet
                server.https = True
                server.request = packet
                self.client.sock.send(CONNECTION_ESTABLISHED)
                self.on_send(self.client.sock.fileno())
                return
            server.enqueue(packet)
            server.request = packet
            self.on_recv(server.sock.fileno())
        else:
            Logger.instance().log(self.client.addr[0], self.client.addr[1],
                                  request.method,
                                  request.abs_resource,
                                  packet.status,
                                  packet.reason
            )
            if packet.status == "304":
                # Response successfully validated.
                # Retrieve it from cache and send to the client.
                cached_response = self.cache.retrieve(request)
                if cached_response:
                    self.handle_packet(None, cached_response, request, nocache=True)
                    return
            if not nocache:
                self.cache.store(request, packet)
            self.client.enqueue(packet)
            self.on_recv(self.client.sock.fileno())

    def close(self, conn):
        conn.shutdown()
        self.on_disc(conn.sock.fileno())
        self.servers = {key: value for key, value in self.servers.items()
                        if value != conn}

    def close_all(self):
        # Iterate over servers and disconnect them as well
        for (key, server) in self.servers.items():
            # this check is nessecary as each server appears twice in the
            # dictionary, once keyed on the FD and once on the hostname.
            if type(key) == int:
                self.close(server)

        self.close(self.client)

    def get_host(self, fd):
        host = self.servers.get(fd)
        if not host:
            host = self.client
        return host

    def recv(self, fd):
        host = self.get_host(fd)
        if not host:
            return
        try:
            packet = host.recv()
            if packet:
                if host.https:
                    self.handle_secure_packet(packet, host)
                    return
                self.handle_packet(host, packet, host.request)
                if packet.get_header("Connection") == "close":
                    self.close_connection = True
                if self.close_connection and host != self.client:
                    # Close the server that sent Connection: close
                    self.close(host)
        except EmptySocketException:
            # Recieved EOF from socket
            if host == self.client:
                self.close_all()
            else:
                self.close(host)
        except RequestError as e:
            self.client.message_buffer = b''
            packet = HTTPMessageParser.error_packet_for_code(e.code)
            self.client.packet_queue.put(packet)
            self.on_recv(self.client.sock.fileno())

    def send(self, fd):
        host = self.get_host(fd)
        if host.send():
            self.on_send(fd)
        if host == self.client and self.close_connection:
            # If the connection should be closed and we have no pending
            # server connections, close the client
            if len(self.servers) == 0:
                self.close(host)


class ProxyServer(object):
    def __init__(self, port, ipv6=False):
        self.port = port
        self.connections = {}
        self.cache = Cache()
        self.epoll = select.epoll()
        self.sock = self.initialize_connection(port, ipv6)

    def initialize_connection(self, port, ipv6):
        sock_type = socket.AF_INET6 if ipv6 else socket.AF_INET
        sock = socket.socket(sock_type, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((LISTEN_ADDRESS, port))
        sock.listen(5)
        sock.setblocking(0)

        self.epoll.register(sock.fileno(), select.EPOLLIN)
        return sock

    def register(self, conn, fd, type_):
        self.epoll.register(fd, type_)
        self.connections[fd] = conn

    def unregister(self, conn, fd):
        host = conn.get_host(fd)
        host.sock.close()
        del self.connections[fd]
        self.epoll.unregister(fd)

    def on_disconnect_callback(self, fd):
        self.unregister(self.connections[fd], fd)

    def accept_connection(self):
        client_sock, client_addr = self.sock.accept()
        client_sock.setblocking(0)
        conn = ProxyContext(client_sock, client_addr, self.cache, self.register,
                            self.on_read_callback, self.on_send_callback,
                            self.on_disconnect_callback)
        self.register(conn, client_sock.fileno(), select.EPOLLIN | select.EPOLLOUT)

    def on_read_callback(self, fd):
        self.epoll.modify(fd, select.EPOLLOUT | select.EPOLLIN)

    def on_send_callback(self, fd):
        self.epoll.modify(fd, select.EPOLLIN)

    def start(self):
        try:
            while True:
                events = self.epoll.poll(1)
                for fileno, event in events:
                    try:
                        if fileno == self.sock.fileno():
                            self.accept_connection()
                        elif event & select.EPOLLIN:
                            self.connections[fileno].recv(fileno)
                        elif event & select.EPOLLOUT:
                            self.connections[fileno].send(fileno)
                        elif event & (select.EPOLLHUP | select.EPOLLERR):
                            self.unregister(self.connections[fileno], fileno)
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

    if os.path.exists(CACHE_DIR):
        shutil.rmtree(CACHE_DIR)
    os.mkdir(CACHE_DIR)

    print("Starting server on port %d." % args.port)

    ProxyServer(args.port, args.ipv6).start()

    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\nGoodbye!")
