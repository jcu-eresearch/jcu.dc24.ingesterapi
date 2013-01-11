import datetime
from jcudc24ingesterapi.ingester_exceptions import UnknownParameterError
from jcudc24ingesterapi import typed, APIDomainObject
from jcudc24ingesterapi.models.locations import LocationOffset

__author__ = 'Casey Bajema'

class DataEntry(APIDomainObject):
    """
    Base class for individual data points of a dataset.

    DataEntry objects will be used for each data point where the actual data is passed in through the
    kwargs argument.

    The kwargs parameters must conform to the data_type schema in the dataset or an exception will be thrown on initialisation.
    
    The (dataset, id) tuple uniquely identifies the data entry.
    """
    __xmlrpc_class__ = "data_entry"
    
    id = typed("_id", int, "An identifier for the data entry")
    dataset = typed("_dataset", int, "The dataset ID")
    timestamp = typed("_timestamp", datetime.datetime, "The timestamp for the entry") 
    location_offset = typed("_location_offset", LocationOffset, "Offset from the locations frame of reference")
    data = typed("_data", dict, "Data storage")

    def __init__(self, dataset=None, timestamp=None, data_entry_id = None, **kwargs):
        self.data_entry_id = data_entry_id
        self.dataset = dataset
        self.timestamp = timestamp
        self.data = {}

        # Push the kwargs to fields
#        for key in data_type_schema.keys():
#            self[key] =  kwargs.pop(key, None)

        for key, value in kwargs:
            raise UnknownParameterError(key, value)

    def __getitem__(self, item):
        return self.data[item]
    def __setitem__(self, item, value):
        self.data[item] = value
    def __delitem__(self, item):
        del self.data[item]


class FileObject(object):
    """This object references a file on disk that is to be downloaded
    """
    __xmlrpc_class__ = "file"
    
    mime_type = typed("_mime_type", str, "The mime type of the file")
    
    def __init__(self, f_handle, mime_type=None):
        self.f_handle = f_handle
        self.mime_type = mime_type
        