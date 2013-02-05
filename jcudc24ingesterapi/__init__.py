import pkg_resources
pkg_resources.declare_namespace(__name__)

__author__ = 'Casey Bajema'
from decimal import Decimal

import time
import datetime
import email.utils as eut
import inspect

# Registered type convertes. (from, to) = func
converters = { (Decimal, float): float,
              (unicode, str): str,
              (int, float): float }

def deleter(attr):
    """Deleter closure, used to remove the inner variable"""
    def deleter_real(self):
        return delattr(self, attr)
    return deleter_real

def getter(attr):
    """Getter closure, used to simply return the inner variable"""
    def getter_real(self):
        if not hasattr(self, attr): return None
        return getattr(self, attr)
    return getter_real

def setter(attr, valid_types):
    """Setter closure, used to do type checking before storing var"""
    if type(valid_types) not in (list, tuple):
        valid_types = (valid_types,)
    def setter_real(self, var):
        if var != None and \
                not isinstance(var, valid_types): 
            # Try to convert
            converted = False
            for t in valid_types:
                if (type(var), t) in converters:
                    var = converters[(type(var), t)](var)
                    converted = True
                    break
            if not converted:
                raise TypeError("%s Not of required type %s for %s"%(str(type(var)), str(valid_types), attr))
        setattr(self,attr,var)
        if hasattr(self, "_listener") and inspect.isfunction(getattr(self, "_listener")):
            func = getattr(self, "_listener")
            func(self, attr, var)
            
    setter_real.valid_types = valid_types
    return setter_real

def typed(attr, valid_types, docs=""):
    """Wrapper around property() so that we can easily apply type checking
    to properties"""
    prop = property(getter(attr), setter(attr, valid_types), deleter(attr), docs)
    return prop

class FixedOffset(datetime.tzinfo):
    """Fixed offset in minutes east from UTC."""

    def __init__(self, offset, name):
        self.__offset = datetime.timedelta(minutes = offset)
        self.__name = name

    def utcoffset(self, dt):
        return self.__offset

    def tzname(self, dt):
        return self.__name

    def dst(self, dt):
        return datetime.timedelta(0)

UTC = FixedOffset(0, "UTC")

def format_timestamp(in_date):
    if type(in_date) == str:
        in_date = datetime.datetime(*eut.parsedate(in_date)[:6])
    r = in_date.strftime("%Y-%m-%dT%H:%M:%S.%f")
    r = r[0:r.find(".")+4] + 'Z'
    return r

def parse_timestamp(date_str):
    """Parse the date time returned by the DAM"""
    if date_str == None: return None
    (dt, mSecs) = date_str.strip().split(".") 
    if mSecs.endswith('Z'): mSecs = mSecs[:-1]
    mSecs = mSecs+'0'*(6-len(mSecs))
    dt = datetime.datetime(*time.strptime(dt, "%Y-%m-%dT%H:%M:%S")[0:6], tzinfo=UTC)
    mSeconds = datetime.timedelta(microseconds = int(mSecs))

    return dt+mSeconds

class APIDomainObject(object):
    """This is the base class of all API domain objects, and provides a listener method
    for indicating when data on the object is updated."""
    def set_listener(self, func):
        self._listener = func
        
