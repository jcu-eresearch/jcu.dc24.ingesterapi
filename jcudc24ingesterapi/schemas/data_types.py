__author__ = 'Casey Bajema'

import re
from jcudc24ingesterapi import typed

RE_ATTR_NAME = re.compile("^[A-Za-z][A-Za-z0-9]*$")

class DataType(object):
    """
    Base data type schema defines an empty dictionary that can have fields added to it dynamically,
    these fields will then be used by the ingester platform to setup the required table.

    Field names map to table column names

    Note: ForeignKey or other table links are not supported, only single, flat tables are supported.
    """
    description = typed("_description", str, "Description of the field")
    name = typed("_name", str, "Name of the field")
    units = typed("_units", str, "Units of the field")
    def __init__(self, name, description=None, units=None):
        if RE_ATTR_NAME.match(name) == None:
            raise ValueError("Name is not valid")
        self.name = name
        self.description = description
        self.units = units


class FileDataType(DataType):
    """
    This schema extends the base _DataType schema and additionally defines that data will be stored as a
    flat file and each data_entry will provide the file mime_type and the file handle.

    Ingesters that want to index additional data should add fields to this schema and provide
    a custom processing script to extract that data.
    """
    __xmlrpc_class__ = "file"

class String(DataType):
    __xmlrpc_class__ = "string"

class Integer(DataType):
    __xmlrpc_class__ = "integer"

class Double(DataType):
    __xmlrpc_class__ = "double"

class DateTime(DataType):
    __xmlrpc_class__ = "datetime"

class Boolean(DataType):
    __xmlrpc_class__ = "boolean"
    
    
