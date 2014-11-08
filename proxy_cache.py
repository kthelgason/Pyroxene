from __future__ import print_function, with_statement

import os
import time
import calendar
import cPickle as pickle
from hashlib import md5
from fnmatch import fnmatch

CACHE_DIR = os.path.abspath("cache")

class DateTimeParser():

    weekdayname = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    monthname = [None,
                 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    weekdayname_lower = [name.lower() for name in weekdayname]
    monthname_lower = [name and name.lower() for name in monthname]

    @classmethod
    def timegm(klass, year, month, day, hour, minute, second):
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
            days = days + 1
        days = days + day - 1
        hours = days*24 + hour
        minutes = hours*60 + minute
        seconds = minutes*60 + second
        return seconds

    @classmethod
    def stringToDatetime(klass, dateString):
        """
        Convert an HTTP date string (one of three formats)
        to seconds since epoch.
        """
        parts = dateString.split()

        if not parts[0][0:3].lower() in klass.weekdayname_lower:
            # Weekday might be omitted.
            try:
                return klass.stringToDatetime(b"Sun, " + dateString)
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
            year=int(year)
            if year < 69:
                year = year + 2000
            elif year < 100:
                year = year + 1900
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
        month = int(klass.monthname_lower.index(month.lower()))
        year = int(year)
        hour, min_, sec = map(int, time.split(':'))
        return int(klass.timegm(year, month, day, hour, min_, sec))


class CacheEntry():
    def __init__(self, key, response, ttl):
        self.timestamp = time.ctime()
        self.ttl = ttl
        self.key = key
        with open(os.path.join(CACHE_DIR, key), 'w+') as f:
            pickle.dump(response, f)

    def data(self):
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
                return (DateTimeParser.stringToDatetime(expires))
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


