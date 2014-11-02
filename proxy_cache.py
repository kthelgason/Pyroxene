from __future__ import print_function, with_statement

import os
import time
import pickle
from hashlib import md5

from http_proxy import Response

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
        Convert an HTTP date string (one of three formats) to seconds since epoch.
        @type dateString: C{bytes}
        """
        parts = dateString.split()

        if not parts[0][0:3].lower() in klass.weekdayname_lower:
            # Weekday is stupid. Might have been omitted.
            try:
                return stringToDatetime(b"Sun, " + dateString)
            except ValueError:
                # Guess not.
                pass

        partlen = len(parts)
        if (partlen == 5 or partlen == 6) and parts[1].isdigit():
            # 1st date format: Sun, 06 Nov 1994 08:49:37 GMT
            # (Note: "GMT" is literal, not a variable timezone)
            # (also handles without "GMT")
            # This is the normal format
            day = parts[1]
            month = parts[2]
            year = parts[3]
            time = parts[4]
        elif (partlen == 3 or partlen == 4) and parts[1].find('-') != -1:
            # 2nd date format: Sunday, 06-Nov-94 08:49:37 GMT
            # (Note: "GMT" is literal, not a variable timezone)
            # (also handles without without "GMT")
            # Two digit year, yucko.
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
        hour, min, sec = map(int, time.split(':'))
        return int(timegm(year, month, day, hour, min, sec))


class CacheEntry():
    def __init__(self, key, data, directives):
        self.timestamp = time.ctime()
        self.directives = map(lambda x: x.strip(), directives.split(","))
        self.key = key
        with open(key, 'w+') as f:
            f.write(response.data)

    def data(self):
        with open(self.key, 'r') as f:
            data = f.read()
        return data

class Cache():
    def __init__(self):
        self.map = {}


    def key_from_response(self, response):
        """
        Construct a cache-key from the response object
        by calculating the md5 hash of the URI and any
        `vary` header values that may be present.
        """
        # TODO: handle Vary: *
        key = response.uri
        vary = response.get_header("Vary")
        if vary:
            for hf in vary.split(","):
                # if this header is present in the response,
                # add it to the digest
                key += response.get_header(hf) or ''
        key = md5(key).digest()
        return key

    def should_cache(self, response):
        """
        Determines whether the response should be cached
        according to the RFC.
        """
        directives = response.get_header("Cache-Control")
        directives = map(lambda x: x.strip().tolower(), directives.split(","))
        if "max-age" in directives or "s-maxage" in directives:
            return True
        elif response.get_header("Expires"):
            return True
        return False

    def store(self, response):
        key = self.key_from_response(response)
        if self.should_cache(response):
            self.map[key] = CacheEntry(key, response.data, directives)

    def retrieve(self, response):
        key = self.key_from_response(response)
        return self.map[key].data()


