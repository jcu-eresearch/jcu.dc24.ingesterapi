__author__ = 'Casey Bajema'

# TODO: Add common data types (eg. temp, video, humidity, rain fall etc.)

class DataType(object):
    """
    Base data type schema defines an empty dictionary that can have fields added to it dynamically,
    these fields will then be used by the ingester platform to setup the required table.

    Field names map to table column names
    Field values should be sqlalchemy data types defining the database column data type

    Note: ForeignKey or other table links are not supported, only single, flat tables are supported.
    """
    pass


class FileDataType(DataType):
    """
    This schema extends the base _DataType schema and additionally defines that data will be stored as a
    flat file and each data_entry will provide the file mime_type and the file handle.

    Ingesters that want to index additional data should add fields to this schema and provide
    a custom processing script to extract that data.
    """
    __xmlrpc_class__ = "file"
    mime_type = None  # eg. text/xml
    file_handle = None # URL (eg. file://c:/test_file.txt)

class String(DataType):
    __xmlrpc_class__ = "string"

class Integer(DataType):
    __xmlrpc_class__ = "integer"

class Double(DataType):
    __xmlrpc_class__ = "double"

class DateTime(DataType):
    __xmlrpc_class__ = "datetime"
